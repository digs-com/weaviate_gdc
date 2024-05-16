import {
  MutationRequest,
  MutationResponse,
} from "@hasura/dc-api-types";
import { Config } from "../config";
import { log } from "../logger";
import { getWeaviateClient } from "../weaviate";
import { queryWhereOperator } from "./query";
import { builtInPropertiesKeys } from "./schema";

export async function executeMutation(
  mutation: MutationRequest,
  config: Config
): Promise<MutationResponse> {
  const response: MutationResponse = {
    operation_results: [],
  };

  for (const operation of mutation.operations) {
    switch (operation.type) {
      case "insert":
        const creator = getWeaviateClient(config).batch.objectsBatcher();

        for (const row of operation.rows) {
          const baseProperties: Record<string, any> = {
            class: operation.table[0],
          };
          const additionalProperties: Record<string, any> = {};

          for (const prop in row) {
            if (builtInPropertiesKeys.includes(prop)) {
              baseProperties[prop] = row[prop];
            } else {
              additionalProperties[prop] = row[prop];
            }
          }

          creator.withObject({
            ...baseProperties,
            properties: additionalProperties,
          });
        }

        const insertResponse = await creator.do();
        
        const successfulInserts = insertResponse.filter((r) => {
          if (!r.result || r.result.status !== "SUCCESS") {
            log.error("Insert failed", r);
            return false;
          }
          return true;
        });

        response.operation_results.push({
          affected_rows: successfulInserts.length,
        });

        break;
      case "update":
        log.error("update not implemented");
        throw new Error("update not implemented");
      case "delete":
        const deleter = getWeaviateClient(config)
          .batch.objectsBatchDeleter()
          .withClassName(operation.table[0])
          .withOutput("verbose");
        if (operation.where) {
          const where = queryWhereOperator(operation.where);
          if (where) {
            deleter.withWhere(where);
          }
        }

        const deleteResponse = await deleter.do();

        // console.log("delete response", deleteResponse);
        log.debug("delete response", deleteResponse);

        response.operation_results.push({
          affected_rows: deleteResponse.results?.matches!,
        });

        break;
    }
  }

  return response;
}
