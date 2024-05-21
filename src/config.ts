import { FastifyRequest } from "fastify";
import { ConfigSchemaResponse } from "@hasura/dc-api-types";

export type Config = {
  scheme?: string;
  host?: string;
  apiKey?: string;
  openApiKey?: string;
  digsEnv?: string;
};

class Configuration {
  private static instance: Configuration;
  private config: Config;

  constructor() {
    this.config = {};
  }

  public static getInstance(): Configuration {
    if (!Configuration.instance) {
      Configuration.instance = new Configuration();
    }
    return Configuration.instance;
  }

  public setConfig(request: FastifyRequest): void {
    const configHeader = request.headers["x-hasura-dataconnector-config"];
    const rawConfigJson = Array.isArray(configHeader)
      ? configHeader[0]
      : configHeader ?? "{}";
    const config = JSON.parse(rawConfigJson);
    this.config = {
      host: config.host,
      scheme: config.scheme ?? "http",
      apiKey: config.apiKey,
      openApiKey: config.openApiKey,
      digsEnv: config.digsEnv,
    };
  }

  public getConfig(): Config {
    return this.config;
  }
}

export default Configuration;

export const configSchema: ConfigSchemaResponse = {
  config_schema: {
    type: "object",
    nullable: false,
    properties: {
      scheme: {
        description:
          "Weaviate connection scheme or corresponding env var, defaults to http",
        type: "string",
        nullable: true,
      },
      host: {
        description: "Weaviate host or corresponding env var, including port",
        type: "string",
        nullable: false,
      },
      apiKey: {
        description: "Weaviate api key or corresponding env var",
        type: "string",
        nullable: false,
      },
      openApiKey: {
        description: "OpenAI api key or corresponding env var",
        type: "string",
        nullable: false,
      },
      digsEnv: {
        description: "Environment name",
        type: "string",
        nullable: false,
      },
    },
  },
  other_schemas: {},
};
