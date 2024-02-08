import {
  ComparisonColumn,
  ComparisonValue,
  Expression,
  Field,
  Query,
  QueryRequest,
  QueryResponse,
  ScalarType,
  ScalarValue,
} from "@hasura/dc-api-types";
import { Config } from "../config";
import { WhereFilter } from "weaviate-ts-client";
import { getWeaviateClient } from "../weaviate";
import { builtInPropertiesKeys } from "./schema";

function column_name(column: ComparisonColumn): string {
  if (Array.isArray(column.name)) {
    return column.name.join(".");
  } else {
    return column.name;
  }
}

export async function executeQuery(
  query: QueryRequest,
  config: Config
): Promise<QueryResponse> {
  if (query.target.type !== "table") {
    throw new Error("Only table requests are supported");
  }

  const queryTableName = query.target.name[0];

  // handle requests by primary key
  if (
    query.query.where?.type === "binary_op" &&
    query.query.where.operator === "equal" &&
    query.query.where.column.name === "id" &&
    query.query.where.value.type === "scalar"
  ) {
    return executeQueryById(
      query.query.where.value.value,
      queryTableName,
      query.query,
      config
    );
  }

  if (query.foreach) {
    const queries = query.foreach.map((foreach) => {
      // todo: build where filter as a map of columns and values
      const where: WhereFilter = {
        operator: "And",
        operands: Object.entries(foreach).map(([column, value]) => ({
          operator: "Equal",
          path: [column],
          ...expressionScalarValue(value),
        })),
      };
      return executeSingleQuery(where, queryTableName, query.query, config);
    });

    return Promise.all(queries).then((results) => ({
      rows: results.map((query) => ({ query })),
    }));
  } else {
    return executeSingleQuery(null, queryTableName, query.query, config);
  }
}

async function executeQueryById(
  id: string,
  table: string,
  query: Query,
  config: Config
) {
  const getter = await getWeaviateClient(config)
    .data.getterById()
    .withClassName(table)
    .withId(id);

  if (query.fields && "vector" in query.fields) {
    getter.withVector();
  }

  const response = await getter.do();

  return {
    rows: [
      Object.fromEntries(
        Object.entries(query.fields!).map(([alias, field]) => {
          if (field.type === "column") {
            if (builtInPropertiesKeys.includes(field.column)) {
              return [alias, response[field.column as keyof typeof response]];
            } else {
              return [
                alias,
                response.properties![
                  field.column as keyof typeof response.properties
                ],
              ];
            }
          } else if (field.type === "array" && field.field.type === "column") {
            if (builtInPropertiesKeys.includes(field.field.column)) {
              return [
                alias,
                response[field.field.column as keyof typeof response],
              ];
            } else {
              return [
                alias,
                response.properties![
                  field.field.column as keyof typeof response.properties
                ],
              ];
            }
          }
          throw new Error(`field of type ${field.type} not supported`);
        })
      ) as Record<string, QueryResponse>, // assertion not safe, but necessary. I hate typescript
    ],
  };
}

async function executeSingleQuery(
  forEachWhere: WhereFilter | null,
  table: string,
  query: Query,
  config: Config
) {
  const getter = getWeaviateClient(config).graphql.get();
  let fieldsString = "";
  getter.withClassName(table);

  if (query.fields) {
    // const additionalFields = query.query.fields.filter()
    // todo: filter out additional properties into the _additional field.
    fieldsString = queryFieldsAsString(query.fields);
    getter.withFields(fieldsString);
  }

  if (query.limit) {
    getter.withLimit(query.limit);
  }

  if (query.offset) {
    getter.withOffset(query.offset);
  }

  if (query.where) {
    const searchTextFilter = getSearchTextFilter(query.where);
    const searchProps = queryProperties(query.where, "with_properties");
    const autocut = queryProperties(query.where, "autocut");

    if (searchTextFilter.length > 0) {
      if (autocut) {
        getter.withAutocut(autocut[0] as unknown as number);
      }
      if (isTextFilter(query.where, "near_text")) {
        getter.withNearText({
          concepts: searchTextFilter,
        });
      } else if (isTextFilter(query.where, "match_text")) {
        getter.withBm25({
          query: searchTextFilter.toString(),
          properties: searchProps,
        });
      } else if (isTextFilter(query.where, "hybrid_match_text")) {
        getter.withHybrid({
          query: searchTextFilter.toString(),
          properties: searchProps,
        });
      } else if (isTextFilter(query.where, "generative_search")) {
        getter.withHybrid({
          query: searchTextFilter.toString(),
          properties: searchProps,
        });
        getter.withGenerate({
          groupedTask: searchTextFilter.toString(),
        });
      } else if (isTextFilter(query.where, "ask_question")) {
        getter.withAsk({
          question: searchTextFilter.toString(),
          properties: searchProps,
        });
        // we have to add this additional string to the fields to get the answer
        fieldsString = fieldsString.concat(
          " _additional { answer { hasAnswer property result startPosition endPosition } }"
        );
        getter.withFields(fieldsString);
      }
    }
  }

  if (forEachWhere) {
    if (query.where) {
      const where = queryWhereOperator(query.where);

      if (where !== null) {
        getter.withWhere({
          operator: "And",
          operands: [where, forEachWhere],
        });
      } else {
        getter.withWhere(forEachWhere);
      }
    } else {
      getter.withWhere(forEachWhere);
    }
  } else if (query.where) {
    const where = queryWhereOperator(query.where);
    if (where !== null) {
      if (where.operands && where.operands.length > 0) {
        getter.withWhere(where);
      }
    }
  }
  const response = await getter.do();

  const rows = response.data.Get[table].map((row: any) =>
    Object.fromEntries(
      Object.entries(query.fields!).map(([alias, field]) => {
        if (
          field.type === "column" &&
          builtInPropertiesKeys.includes(field.column)
        ) {
          let value = null;
          if (alias === "generate" || alias === "answer") {
            if (!row["_additional"]) { // triggered when using plain get queries
              value = null 
            } else { 
              value = row["_additional"][alias] 
            }
          } else {
            value =
              row[alias as keyof typeof row][field.column as keyof typeof row];
          }
          return [alias, value];
        }
        if (
          field.type === "array" &&
          field.field.type === "column" &&
          builtInPropertiesKeys.includes(field.field.column)
        ) {
          const value =
            row[alias as keyof typeof row][
              field.field.column as keyof typeof row
            ];
          return [alias, value];
        }
        if (
          field.type === "relationship" &&
          builtInPropertiesKeys.includes(field.relationship)
        ) {
          const value =
            row[alias as keyof typeof row][
              field.relationship as keyof typeof row
            ];
          return [alias, value];
        }
        const value = row[alias as keyof typeof row];
        return [alias, value];
      })
    )
  );
  if (query.aggregates) {
    const tableAggregates = await executeAggregateQuery(
      query,
      config,
      table
    );
    if (query.aggregates.aggregate_count) {
      const aggregates = {
        aggregate_count: {
          [table]: tableAggregates[0].meta.count,
        }
      }
      return { rows, aggregates };
    }
    if (query.aggregates.aggregate_group_by_vector) {
      const aggregates = {
        aggregate_group_by_vector: {
          [table]: tableAggregates,
        },
      };
      return { rows, aggregates };
    }
  }
  return { rows };
}

async function executeAggregateQuery(
  query: Query,
  config: Config,
  table: string
) {
  const getter = getWeaviateClient(config).graphql.aggregate();
  getter.withClassName(table);
  getter.withFields("meta { count }");
  //@ts-ignore
  const groupByProps = queryProperties(query.where.expressions[0], "with_groupedby");
  if (groupByProps && query.aggregates && query.aggregates.aggregate_group_by_vector) {
    getter.withGroupBy(groupByProps);
    getter.withFields("groupedBy { path value } meta { count }");
  }
  if (query.where) {
    const where = queryWhereOperator(query.where);
    if (where !== null) {
      if (where.operands && where.operands.length > 0) {
        getter.withWhere(where);
      }
    }
  }
  const {
    data: { Aggregate },
  } = await getter.do();
  return Aggregate[table];
}

function isTextFilter(expression: Expression, operator: string): boolean {
  switch (expression.type) {
    case "not":
      return isTextFilter(expression.expression, operator);
    case "and":
    case "or":
      return expression.expressions.some((expr) =>
        isTextFilter(expr, operator)
      );
    case "binary_op":
      return expression.operator === operator;
    default:
      return false;
  }
}

function getSearchTextFilter(
  expression: Expression,
  negated = false,
  ored = false
): string[] {
  switch (expression.type) {
    case "not":
      return getSearchTextFilter(expression.expression, !negated, ored);
    case "and":
      return expression.expressions
        .map((expression) => getSearchTextFilter(expression, negated, ored))
        .flat()
        .filter((filter) => filter !== null);
    case "or":
      return expression.expressions
        .map((expression) => getSearchTextFilter(expression, negated, true))
        .flat()
        .filter((filter) => filter !== null);
    case "binary_op":
      switch (expression.operator) {
        case "near_text":
        case "match_text":
        case "hybrid_match_text":
        case "ask_question":
        case "generative_search":
        case "with_properties":
        case "with_groupedby":
        case "autocut":
          if (negated) {
            throw new Error(
              "Negated near_text or match_text or hybrid_match_text or ask_question or generative search or autocut not supported"
            );
          }
          if (ored) {
            throw new Error(
              "Ored near_text or match_text or hybrid_match_text or ask_question or generative search or autocut not supported"
            );
          }
          switch (expression.value.type) {
            case "scalar":
              return [expression.value.value];
            case "column":
              throw new Error("Column comparison not implemented");
          }
        default:
          return [];
      }
    default:
      return [];
  }
}

export function queryProperties(
  expression: Expression,
  from_props: string
): string[] | undefined {
  if (expression.type === "and") {
    for (let x of expression.expressions) {
      if (
        x.type === "binary_op" &&
        x.operator === from_props &&
        x.value.type === "scalar"
      ) {
        return x.value.value?.split(",");
      }
    }
  }
  return undefined;
}

export function queryWhereOperator(
  expression: Expression,
  path: string[] = []
): WhereFilter | null {
  switch (expression.type) {
    case "not":
      const expr = queryWhereOperator(expression.expression, path);
      if (expr === null) {
        return null;
      }
      return {
        operator: "NotEqual",
        operands: [expr],
      };
    case "and":
      if (expression.expressions.length < 1) return null;
      const operands = expression.expressions.reduce<WhereFilter[] | null>(
        (
          exprs: WhereFilter[] | null,
          expression: Expression
        ): WhereFilter[] | null => {
          const expr = queryWhereOperator(expression, path);
          if (expr !== null) {
            if (exprs === null) {
              exprs = [];
            }
            exprs.push(expr);
          }
          return exprs;
        },
        null
      );
      return operands && operands.length > 0
        ? {
            operator: "And",
            operands: operands,
          }
        : null;
    case "or":
      if (expression.expressions.length < 1) return null;
      return {
        operator: "Or",
        operands: expression.expressions.reduce<WhereFilter[]>(
          (exprs: WhereFilter[], expression: Expression): WhereFilter[] => {
            const expr = queryWhereOperator(expression, path);
            if (expr !== null) {
              exprs.push(expr);
            }
            return exprs;
          },
          []
        ),
      };
    case "binary_op":
      switch (expression.operator) {
        case "equal":
          return {
            operator: "Equal",
            path: [...path, column_name(expression.column)],
            ...expressionValue(expression.value),
          };
        case "less_than":
          return {
            operator: "LessThan",
            path: [...path, column_name(expression.column)],
            ...expressionValue(expression.value),
          };
        case "less_than_or_equal":
          return {
            operator: "LessThanEqual",
            path: [...path, column_name(expression.column)],
            ...expressionValue(expression.value),
          };
        case "greater_than":
          return {
            operator: "GreaterThan",
            path: [...path, column_name(expression.column)],
            ...expressionValue(expression.value),
          };
        case "greater_than_or_equal":
          return {
            operator: "GreaterThanEqual",
            path: [...path, column_name(expression.column)],
            ...expressionValue(expression.value),
          };
        case "near_text":
        case "match_text":
        case "hybrid_match_text":
        case "ask_question":
        case "generative_search":
        case "with_properties":
        case "with_groupedby":
        case "autocut":
          // silently ignore near_text, match_text, hybrid_match_text or ask_question or generative search or autocut operator
          return null;
        default:
          throw new Error(
            `Unsupported binary comparison operator: ${expression.operator}`
          );
      }
    case "unary_op":
      switch (expression.operator) {
        case "is_null":
          return {
            operator: "IsNull",
            path: [...path, column_name(expression.column)],
          };
        default:
          throw new Error(
            `Unsupported unary comparison operator: ${expression.operator}`
          );
      }
    case "binary_arr_op":
      switch (expression.operator) {
        case "in":
          if (expression.values.length < 1) return null;
          return {
            operator: "Or",
            operands: expression.values.map((value) => ({
              operator: "Equal",
              path: [...path, column_name(expression.column)],
              [expressionValueType(expression.value_type)]: value,
            })),
          };
        default:
          throw new Error(
            `Unsupported binary array comparison operator: ${expression.operator}`
          );
      }
    default:
      throw new Error(`Unsupported expression type: ${expression.type}`);
  }
}

function expressionValueType(value_type: ScalarType): string {
  switch (value_type) {
    case "text":
      return "valueText";
    case "int":
      return "valueInt";
    case "boolean":
      return "valueBoolean";
    case "number":
      return "valueNumber";
    case "date":
      return "valueDate";
    case "uuid":
      return "valueText";
    case "geoCoordinates":
      return "valueText";
    case "phoneNumber":
      return "valueText";
    case "blob":
      return "valueText";
    default:
      throw new Error(`Unknown scalar type: ${value_type}`);
  }
}

function expressionValue(value: ComparisonValue) {
  switch (value.type) {
    case "scalar":
      switch (value.value_type) {
        case "text":
          return { valueText: value.value };
        case "int":
          return { valueInt: value.value };
        case "boolean":
          return { valueBoolean: value.value };
        case "number":
          return { valueNumber: value.value };
        case "date":
          return { valueDate: value.value };
        case "uuid":
          return { valueText: value.value };
        case "geoCoordinates":
          return { valueText: value.value };
        case "phoneNumber":
          return { valueText: value.value };
        case "blob":
          return { valueText: value.value };
        default:
          throw new Error(`Unknown scalar type: ${value.value_type}`);
      }
    case "column":
      throw new Error("Column comparison not implemented");
  }
}

function expressionScalarValue(value: ScalarValue) {
  switch (value.value_type) {
    case "text":
      return { valueText: value.value };
    case "int":
      return { valueInt: value.value };
    case "boolean":
      return { valueBoolean: value.value };
    case "number":
      return { valueNumber: value.value };
    case "date":
      return { valueDate: value.value };
    case "uuid":
      return { valueText: value.value };
    case "geoCoordinates":
      return { valueText: value.value };
    case "phoneNumber":
      return { valueText: value.value };
    case "blob":
      return { valueText: value.value };
    default:
      throw new Error(`Unknown scalar type: ${value.value_type}`);
  }
}

function queryFieldsAsString(fields: Record<string, Field>): string {
  return Object.entries(fields)
    .map(([alias, field]) => {
      if (alias === "generate" || alias.includes("groupedBy") || alias === "answer") return "";
      return `${alias}: ${fieldString(field)}`;
    })
    .join(" ");
}

// given a field, returns the graphql string for that field
// does not return the alias
// note we currently don't handle built-in objects or relationships.
function fieldString(field: Field): string {
  switch (field.type) {
    case "object":
      return `${field.column} { ${queryFieldsAsString(field.query.fields!)} }`;
    case "relationship":
      return field.relationship;
    case "column":
      if (builtInPropertiesKeys.includes(field.column)) {
        return `_additional { ${field.column} }`;
      }
      return field.column;
    case "array":
      return fieldString(field.field);
  }
}
