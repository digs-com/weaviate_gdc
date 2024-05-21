import FastifyCors from "@fastify/cors";
import {
  CapabilitiesResponse,
  MutationRequest,
  MutationResponse,
  QueryRequest,
  QueryResponse,
  SchemaResponse,
} from "@hasura/dc-api-types";
import Fastify from "fastify";
import Configuration from "./config";
import { getCapabilities } from "./handlers/capabilities";
import { executeMutation } from "./handlers/mutation";
import { executeQuery } from "./handlers/query";
import { getSchema } from "./handlers/schema";
import { log } from "./logger";

const port = Number(process.env.PORT) || 8100;
const server = Fastify({ logger: false });
const config = Configuration.getInstance();

server.register(FastifyCors, {
  // Accept all origins of requests. This must be modified in
  // a production setting to be specific allowable list
  // See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
  origin: true,
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: [
    "X-Hasura-DataConnector-Config",
    "X-Hasura-DataConnector-SourceName",
  ],
});

server.addHook("preHandler", async (request, _reply) => {
  config.setConfig(request);
});

server.get<{ Reply: CapabilitiesResponse }>(
  "/capabilities",
  async (request, _response) => {
    return getCapabilities();
  }
);

server.get<{ Reply: SchemaResponse }>("/schema", async (request, _response) => {
  const schema = await getSchema(config.getConfig());
  return schema;
});

server.post<{ Body: QueryRequest; Reply: QueryResponse }>(
  "/query",
  async (request, _response) => {
    log.info("query initiated");
    const query = request.body;
    const response = await executeQuery(query, config.getConfig());
    return response;
  }
);

server.post<{ Body: MutationRequest; Reply: MutationResponse }>(
  "/mutation",
  async (request, _response) => {
    const mutation = request.body;
    const response = await executeMutation(mutation, config.getConfig());
    return response;
  }
);

server.get("/health", async (request, response) => {
  log.debug("health check", { headers: request.headers, query: request.body });
  response.statusCode = 204;
});

process.on("SIGINT", () => {
  log.error("server interrupted");
  process.exit(0);
});

const start = async () => {
  try {
    await server.listen({ port: port, host: "0.0.0.0" });
  } catch (err) {
    log.error("server failed to start", err);
    process.exit(1);
  }
};
start();
