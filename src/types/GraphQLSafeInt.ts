// Based on https://github.com/Urigo/graphql-scalars/blob/master/src/scalars/SafeInt.ts
import {
  GraphQLError,
  GraphQLScalarType,
  GraphQLScalarTypeConfig,
  Kind,
  print,
} from "https://deno.land/x/graphql_deno@v15.0.0/mod.ts";

function isObjectLike(value: unknown): value is { [key: string]: unknown } {
  return typeof value === "object" && value !== null;
}

// Taken from https://github.com/graphql/graphql-js/blob/30b446938a9b5afeb25c642d8af1ea33f6c849f3/src/type/scalars.ts#L267

// Support serializing objects with custom valueOf() or toJSON() functions -
// a common way to represent a complex value which can be represented as
// a string (ex: MongoDB id objects).
function serializeObject(outputValue: unknown): unknown {
  if (isObjectLike(outputValue)) {
    if (typeof outputValue.valueOf === "function") {
      const valueOfResult = outputValue.valueOf();
      if (!isObjectLike(valueOfResult)) {
        return valueOfResult;
      }
    }
    if (typeof outputValue.toJSON === "function") {
      return outputValue.toJSON();
    }
  }
  return outputValue;
}

const specifiedByURL =
  "https://www.ecma-international.org/ecma-262/#sec-number.issafeinteger";

export const GraphQLSafeIntConfig = {
  name: "Int",
  description:
    "The `SafeInt` scalar type represents non-fractional signed whole numeric values that are " +
    "considered safe as defined by the ECMAScript specification.",
  specifiedByURL,
  specifiedByUrl: specifiedByURL,
  serialize(outputValue) {
    const coercedValue = serializeObject(outputValue);

    if (typeof coercedValue === "boolean") {
      return coercedValue ? 1 : 0;
    }

    let num = coercedValue;
    if (typeof coercedValue === "string" && coercedValue !== "") {
      num = Number(coercedValue);
    }

    if (typeof num !== "number" || !Number.isInteger(num)) {
      throw new GraphQLError(
        `SafeInt cannot represent non-integer value: ${coercedValue}`
      );
    }
    if (!Number.isSafeInteger(num)) {
      throw new GraphQLError(
        "SafeInt cannot represent unsafe integer value: " + coercedValue
      );
    }
    return num;
  },

  parseValue(inputValue) {
    if (typeof inputValue !== "number" || !Number.isInteger(inputValue)) {
      throw new GraphQLError(
        `SafeInt cannot represent non-integer value: ${inputValue}`
      );
    }
    if (!Number.isSafeInteger(inputValue)) {
      throw new GraphQLError(
        `SafeInt cannot represent unsafe integer value: ${inputValue}`
      );
    }
    return inputValue;
  },

  parseLiteral(valueNode) {
    if (valueNode.kind !== Kind.INT) {
      throw new GraphQLError(
        `SafeInt cannot represent non-integer value: ${print(valueNode)}`,
        {
          nodes: valueNode,
        }
      );
    }
    const num = parseInt(valueNode.value, 10);
    if (!Number.isSafeInteger(num)) {
      throw new GraphQLError(
        `SafeInt cannot represent unsafe integer value: ${valueNode.value}`,
        {
          nodes: valueNode,
        }
      );
    }
    return num;
  },
  extensions: {
    codegenScalarType: "number",
    jsonSchema: {
      title: "Int",
      type: "integer",
      minimum: Number.MIN_SAFE_INTEGER,
      maximum: Number.MAX_SAFE_INTEGER,
    },
  },
} as GraphQLScalarTypeConfig<number | string, number>;

export const GraphQLSafeInt = /*#__PURE__*/ new GraphQLScalarType(
  GraphQLSafeIntConfig
);
