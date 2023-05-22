import "https://deno.land/x/corejs@v3.26.1/index.js";

import {
  GraphQLBoolean,
  GraphQLEnumType,
  GraphQLFloat,
  GraphQLInputObjectType,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLString,
  printSchema,
} from "https://deno.land/x/graphql_deno@v15.0.0/mod.ts";
import { search as jmespath } from "https://deno.land/x/jmespath@v0.2.2/index.ts";
import { DB } from "https://deno.land/x/sqlite@v3.7.2/mod.ts";
import { Parser } from "https://esm.sh/graphql-js-tree@0.3.5";
import { TreeToTS } from "https://esm.sh/graphql-zeus-core@5.3.0";
import sql from "https://esm.sh/noop-tag@2.0.0";
import DataLoader from "https://esm.sh/v116/dataloader@2.2.2/es2022/dataloader.mjs";
import { GraphQLHTTP } from "https://gist.githubusercontent.com/nestarz/7c0275b94b4e18ed1a108237732fd57f/raw/c6b2d8e9f0191536e09997c1383478f37400cac5/gql.http.ts";

import { GraphQLSafeInt as GraphQLInt } from "./GraphQLSafeInt.js";

export { DB } from "https://deno.land/x/sqlite@v3.7.2/mod.ts";
import type {
  QueryParameterSet,
  RowObject,
} from "https://deno.land/x/sqlite@v3.7.2/mod.ts";
export interface DBWithHash {
  _db: DB | null;
  hash: string | null | undefined;
  query: (
    query: string,
    values?: QueryParameterSet
  ) => Promise<RowObject[] | undefined>;
}

const tableLoaderMap = new Map();
const tableLoader = (db) => {
  return new DataLoader(async (keys) => {
    const results = db
      .query(d`SELECT * FROM ${table} WHERE id = ${id}`)
      .then((result) => result[0]);
    const values = await Promise.all(
      keys.map((key) => {
        const [table, id] = key.split(":");
        return db
          .query(d`SELECT * FROM ${table} WHERE id = ${id}`)
          .then((result) => result[0]);
      })
    );
    return values;
  });
};

const memoize = <T extends (...args: any[]) => any>(
  fn: T,
  propKey?: string
) => {
  const cache: Record<string, ReturnType<T>> = {};
  return (...args: Parameters<T>): ReturnType<T> => {
    const key = propKey ?? JSON.stringify(args);
    if (!cache[key]) {
      cache[key] = fn(...args);
    }
    return cache[key];
  };
};

const GraphQLJSON = new GraphQLScalarType({ name: "JSON" });
const sqlGqlTypes = {
  REAL: GraphQLFloat,
  INTEGER: GraphQLInt,
  TEXT: GraphQLString,
  JSON: GraphQLJSON,
};
const mapSqliteGraphql = (type, columnName: string, notnull?: boolean) => {
  const rgx = /\b(?:((?:\w*_)*)(json)((?:_\w*)*))\b/;
  const newType = type === "TEXT" && rgx.test(columnName ?? "") ? "JSON" : type;
  return notnull
    ? new GraphQLNonNull(sqlGqlTypes[newType])
    : sqlGqlTypes[newType];
};

const createColumnFields = (columns) => {
  return columns.reduce(
    (acc, { name, type, notnull }) => ({
      ...acc,
      [name]: { type: mapSqliteGraphql(type, name, notnull) },
    }),
    {}
  );
};

const createForeignKeyFields = (columns, tables) => {
  const foreignKeys = columns.filter((column) => column.references);
  return foreignKeys.reduce((acc, { name, references }) => {
    const relatedTable = tables.find((table) => table.name === references);
    if (relatedTable) {
      acc[`${name}_by_fk`] = {
        type: createTableType(relatedTable.name, relatedTable.columns, tables),
        resolve: async (parent, _, { db }) => {
          const result = await db.query(
            sql`SELECT * FROM ${references} WHERE id = ${parent[name]}`
          );
          return result[0];
        },
      };
    }
    return acc;
  }, {});
};

const createReverseForeignKeyFields = (name, tables) => {
  return tables.reduce((acc, table) => {
    const backRef = table.columns.find((col) => col.references === name);
    if (backRef) {
      acc[`${table.name}_by_${backRef.name}`] = {
        type: new GraphQLList(
          createTableType(table.name, table.columns, tables)
        ),
        args: {
          order_by: { type: createOrderByInputType(name, table.columns) },
          where: { type: createBoolExpInputType(name, table.columns) },
        },
        resolve: async (parent, args, { db }) => {
          const where = {
            ...(args.where ?? {}),
            [backRef.name]: { _eq: parent.id },
          };
          const result = await fetchRecords(db, table.name, { ...args, where });
          return result;
        },
      };
    }
    return acc;
  }, {});
};

const addJmesPathArgument = (field, fieldName, isJSONType) => {
  field.args = { path: { type: GraphQLString } };
  const originalResolve = field.resolve;
  field.resolve = (parent, args, context, info) => {
    const fieldValue = parent[fieldName];
    if (isJSONType || args.path) {
      try {
        const value = JSON.parse(fieldValue);
        if (args.path) {
          const jmesPathResult = jmespath(value, args.path);
          return isJSONType ? jmesPathResult : JSON.stringify(jmesPathResult);
        }
        return originalResolve
          ? originalResolve(parent, args, context, info)
          : value;
      } catch (error) {
        console.error("invalid JSON for", fieldName, error);
      }
    }
    return originalResolve
      ? originalResolve(parent, args, context, info)
      : fieldValue;
  };
};

const processJsonFields = (fields) =>
  Object.fromEntries(
    Object.entries(fields).map(([fieldName, field]) => {
      const oTypes = [field.type, field.type.ofType];
      const isStringType = oTypes.some((v) => v === GraphQLString);
      const isJSONType = oTypes.some((v) => v === GraphQLJSON);
      if (isJSONType || isStringType) {
        addJmesPathArgument(field, fieldName, isJSONType);
      }
      return [fieldName, field];
    })
  );

const createAggregateType = memoize(() => {
  return new GraphQLObjectType({
    name: "aggregate",
    fields: {
      count: {
        type: GraphQLInt,
        resolve: async (parent, _) => {
          const { name, constraint, db } = parent;
          const result = await db.query(
            sql`SELECT COUNT(*) as count FROM ${name} WHERE ${constraint}`
          );
          return result[0].count;
        },
      },
    },
  });
});

const createTableAggregateType = memoize((name) => {
  return new GraphQLObjectType({
    name: `${name}_aggregate`,
    fields: {
      aggregate: {
        type: createAggregateType(),
        resolve: (parent) => parent,
      },
    },
  });
});

const createTableType = memoize((name, columns, tables) => {
  const fields = () => {
    const columnFields = createColumnFields(columns);
    const foreignKeyFields = createForeignKeyFields(columns, tables);
    const reverseForeignKeyFields = createReverseForeignKeyFields(name, tables);

    const relatedAggregates = tables.reduce((acc, table) => {
      const backRef = table.columns.find((col) => col.references === name);
      if (backRef) {
        acc[`${table.name}_aggregate`] = {
          type: createTableAggregateType(table.name),
          resolve: (parent, _, { db }) => ({
            name: table.name,
            constraint: `${backRef.name} = ${parent.id}`,
            db,
          }),
        };
      }
      return acc;
    }, {});

    return processJsonFields({
      ...columnFields,
      ...foreignKeyFields,
      ...reverseForeignKeyFields,
      ...relatedAggregates,
    });
  };

  return new GraphQLObjectType({
    name,
    fields,
  });
});

const findTableByName = (tables, name) => {
  const table = tables.find(({ name: v }) => v === name);
  return table;
};

const fetchRecords = async (db, tableName, { where, limit, order_by }) => {
  const { clause: whereClause, values } = buildWhereClause(where);
  const limitClause = limit ? `LIMIT ${limit}` : "";
  const orderClause = order_by
    ? `ORDER BY ${Object.entries(order_by).map(
        ([key, value]) => `"${key}" ${value}`
      )}`
    : "";
  const query = `SELECT * FROM ${tableName} ${whereClause} ${orderClause} ${limitClause}`;
  const result = await db.query(query, values);
  return result;
};

const createQueryType = (tables) => {
  const fields = () => {
    const tableQueries = tables.reduce(
      (acc, { name, columns }) => {
        //
        const primaryKeyColumn = columns.find((col) => col.pk);
        if (primaryKeyColumn) {
          acc[`${name}_by_pk`] = {
            type: createTableType(name, columns, tables),
            args: {
              [primaryKeyColumn.name]: {
                type: new GraphQLNonNull(
                  mapSqliteGraphql(primaryKeyColumn.type, primaryKeyColumn.name)
                ),
              },
            },
            resolve: async (_, args, { db }) => {
              const results = await fetchRecords(db, name, {
                ...args,
                limit: 1,
                where: {
                  ...(args.where ?? {}),
                  [primaryKeyColumn.name]: { _eq: args[primaryKeyColumn.name] },
                },
              });
              return results?.[0];
            },
          };
        }

        acc[name] = {
          type: new GraphQLList(createTableType(name, columns, tables)),
          args: {
            where: { type: createBoolExpInputType(name, columns) },
            order_by: { type: createOrderByInputType(name, columns) },
          },
          resolve: async (_, args, { db }) =>
            await fetchRecords(db, name, args),
        };

        acc[`${name}_aggregate`] = {
          type: createTableAggregateType(name),
          resolve: () => ({}),
        };

        return acc;
      },
      {
        _sql: {
          type: GraphQLJSON,
          args: { query: { type: new GraphQLNonNull(GraphQLString) } },
          resolve: (_, { query }, { db }) => db.query(query),
        },
        _tables: { type: GraphQLJSON, resolve: () => tables },
        _table: {
          type: GraphQLJSON,
          args: { name: { type: new GraphQLNonNull(GraphQLString) } },
          resolve: (_, { name }) => findTableByName(tables, name),
        },
      }
    );

    return tableQueries;
  };

  return new GraphQLObjectType({
    name: "Query",
    fields,
  });
};

const createInputType = memoize((name, columns, set?: string) => {
  return new GraphQLInputObjectType({
    name: `${name}_${set ? "set" : "insert"}_input`,
    fields: columns.reduce(
      (acc, { name, type, notnull }) => ({
        ...acc,
        [name]: { type: mapSqliteGraphql(type, name, set ? false : notnull) },
      }),
      {}
    ),
  });
});

const insertRecords = async (db, tableName, records, onConflict) => {
  const columns = Object.keys(records[0]);
  const conflictAction = onConflict
    ? `ON CONFLICT(${
        onConflict.constraint
      }) DO ${onConflict.action.toUpperCase()}`
    : "";
  const columnNames = columns.join(", ");
  const valuePlaceholders = columns.map(() => "?").join(", ");
  const query = sql`INSERT INTO ${tableName} (${columnNames}) VALUES `;
  const rows = records.map(() => `(${valuePlaceholders})`).join(", ");
  const params = records.flatMap((row) => columns.map((col) => row[col]));
  const queryStr = sql`${query} ${rows} ${conflictAction} RETURNING *`;
  const result = await db.query(queryStr, params);
  return result;
};

const updateRecords = async (db, tableName, updates) => {
  try {
    await db.query("BEGIN");
    const results = await Promise.all(
      updates.map(async ({ where, _set, _inc }) => {
        const { clause: whereClause, values } = buildWhereClause(where);
        const setClauses = [];
        const setValues = [];
        if (_set) {
          Object.entries(_set).forEach(([key, value]) => {
            setClauses.push(`"${key}" = ?`);
            setValues.push(value);
          });
        }
        if (_inc) {
          Object.entries(_inc).forEach(([key, value]) => {
            setClauses.push(`"${key}" = "${key}" + ?`);
            setValues.push(value);
          });
        }
        const setClause = setClauses.join(", ");
        const query = `UPDATE ${tableName} SET ${setClause} ${whereClause} RETURNING *`;
        const updated = await db.query(query, [...setValues, ...values]);
        return updated;
      })
    );

    await db.query("COMMIT");
    return results.map((updated) => ({
      affected_rows: updated.length ?? 0,
      returning: updated,
    }));
  } catch (error) {
    await db.query("ROLLBACK");
    console.error(error);
    throw error;
  }
};

const createConflictType = memoize(() => {
  return new GraphQLEnumType({
    name: "conflict_action",
    values: {
      nothing: { value: "NOTHING" },
      update: { value: "UPDATE" },
    },
  });
});

const createOnConflictInputType = memoize((name) => {
  const onConflictInputType = new GraphQLInputObjectType({
    name: `${name}_on_conflict`,
    fields: {
      constraint: { type: GraphQLString },
      action: {
        type: new GraphQLNonNull(createConflictType()),
      },
    },
  });
  return onConflictInputType;
});

const buildWhereClause = (where) => {
  if (!where) return { clause: "", values: [] };

  const conditions = [];
  const values = [];
  Object.entries(where).forEach(([key, value]) => {
    if (typeof value === "object") {
      if (value._in) {
        conditions.push(`"${key}" IN (${value._in.map(() => "?").join(",")})`);
        values.push(...value._in);
      }
      if (value._eq) {
        conditions.push(`"${key}" = ?`);
        values.push(value._eq);
      }
      if (value._ilike) {
        conditions.push(`"${key}" LIKE ?`); // sqlite default insensitive
        values.push(value._ilike);
      }
      if (value._neq) {
        conditions.push(`"${key}" != ?`);
        values.push(value._neq);
      }
    }
  });

  return {
    clause: conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "",
    values,
  };
};

const createPrimaryKeyInputType = memoize((name, primaryKeyColumn) => {
  return new GraphQLInputObjectType({
    name: `${name}_pk_columns_input`,
    fields: {
      [primaryKeyColumn.name]: { type: new GraphQLNonNull(GraphQLInt) },
    },
  });
});

const createUpdateMutation = (name, columns, tables) => {
  const primaryKeyColumn = columns.find((col) => col.pk);
  if (!primaryKeyColumn) return null;

  return {
    type: createTableType(name, columns, tables),
    args: {
      _set: { type: new GraphQLNonNull(createInputType(name, columns, "set")) },
      pk_columns: { type: createPrimaryKeyInputType(name, primaryKeyColumn) },
    },
    resolve: async (_, { _set, pk_columns }, { db }) => {
      const updates = Object.keys(_set)
        .map((key) => `"${key}" = ?`)
        .join(", ");
      const result = await db.query(
        sql`UPDATE ${name} SET ${updates} WHERE ${primaryKeyColumn.name} = ? RETURNING *`,
        [...Object.values(_set), pk_columns[primaryKeyColumn.name]]
      );
      return result[0];
    },
  };
};

const createComparisonExpType = memoize((type) => {
  const gqlType = mapSqliteGraphql(type, "");

  return new GraphQLInputObjectType({
    name: `${gqlType}_comparison_exp`,
    fields: {
      _ilike: { type: GraphQLString },
      _is_null: { type: GraphQLBoolean },
      _neq: { type: gqlType },
      _eq: { type: gqlType },
      _nin: { type: new GraphQLList(new GraphQLNonNull(gqlType)) },
      _in: { type: new GraphQLList(new GraphQLNonNull(gqlType)) },
    },
  });
});

const boolExpCache = {};
const createBoolExpInputType = (name, columns) => {
  if (boolExpCache[name]) return boolExpCache[name];
  const boolExpInputType: GraphQLInputObjectType = new GraphQLInputObjectType({
    name: `${name}_bool_exp`,
    fields: () => ({
      ...columns.reduce((acc, col) => {
        acc[col.name] = {
          type: createComparisonExpType(col.type),
        };
        return acc;
      }, {}),
      _or: { type: new GraphQLList(new GraphQLNonNull(boolExpInputType)) },
      _and: { type: new GraphQLList(new GraphQLNonNull(boolExpInputType)) },
    }),
  });

  boolExpCache[name] = boolExpInputType;
  return boolExpInputType;
};

const createOrderByType = memoize(() => {
  return new GraphQLEnumType({
    name: "order_by",
    values: {
      asc: { value: "ASC" },
      asc_nulls_first: { value: "ASC" },
      asc_nulls_last: { value: "ASC" },
      desc: { value: "DESC" },
      desc_nulls_first: { value: "DESC" },
      desc_nulls_last: { value: "DESC" },
    },
  });
});

const orderByCache = {};
const createOrderByInputType = (name, columns) => {
  if (orderByCache[name]) return orderByCache[name];
  const orderByInputType: GraphQLInputObjectType = new GraphQLInputObjectType({
    name: `${name}_order_by`,
    fields: () => ({
      ...columns.reduce((acc, col) => {
        acc[col.name] = {
          type: createOrderByType(),
        };
        return acc;
      }, {}),
    }),
  });

  orderByCache[name] = orderByInputType;
  return orderByInputType;
};

const createAffectedRows = memoize((name, columns, tables) => {
  return new GraphQLObjectType({
    name: `${name}_mutation_response`,
    fields: {
      affected_rows: { type: GraphQLInt },
      returning: {
        type: new GraphQLNonNull(
          new GraphQLList(
            new GraphQLNonNull(createTableType(name, columns, tables))
          )
        ),
      },
    },
  });
});

const createIncType = memoize((name, columns) => {
  const fields = columns.reduce((acc, { name, type }) => {
    const gqlType = mapSqliteGraphql(type, name);
    if (gqlType?.name === "Int" || gqlType.ofType?.name === "Int") {
      acc[name] = { type: gqlType };
    }
    return acc;
  }, {});
  return Object.entries(fields).length
    ? new GraphQLInputObjectType({ name: `${name}_inc`, fields })
    : null;
});

const createDeleteMutation = (name, columns, tables) => {
  //const primaryKeyColumn = columns.find((col) => col.pk);
  //if (!primaryKeyColumn) return null;

  return {
    type: createTableType(name, columns, tables),
    args: { where: { type: createBoolExpInputType(name, columns) } },
    resolve: async (_, { where }, { db }) => {
      const { clause: whereClause, values } = buildWhereClause(where);
      if (!whereClause.trim()) throw Error("Can't delete without where clause");
      const result = await db.query(
        sql`DELETE FROM ${name} ${whereClause} RETURNING *`,
        values
      );
      return result[0];
    },
  };
};

function removeEmpty<T>(data: T): T {
  const checkNotEmpty = (v: unknown) => v !== null && typeof v !== "undefined";
  if (Array.isArray(data)) {
    return (data as unknown[])
      .filter(checkNotEmpty)
      .map((value) => removeEmpty(value)) as unknown as T;
  }
  const entries = Object.entries(data as Record<string, unknown>).filter(
    ([, value]) => checkNotEmpty(value)
  );

  return Object.fromEntries(entries) as T;
}

const createMutationType = (tables) =>
  new GraphQLObjectType({
    name: "Mutation",
    fields: tables.reduce(
      (acc, { name, columns }) =>
        removeEmpty({
          ...acc,
          [`update_${name}_one`]: createUpdateMutation(name, columns, tables),
          [`delete_${name}`]: createDeleteMutation(name, columns, tables),
          [`insert_${name}_one`]: {
            type: createTableType(name, columns, tables),
            args: {
              object: {
                type: new GraphQLNonNull(createInputType(name, columns)),
              },
              on_conflict: { type: createOnConflictInputType(name, columns) },
            },
            resolve: async (_, { object, on_conflict }, { db }) => {
              const result = await insertRecords(
                db,
                name,
                [object],
                on_conflict
              );
              return result[0];
            },
          },
          [`insert_${name}`]: {
            type: new GraphQLList(createTableType(name, columns, tables)),
            args: {
              objects: {
                type: new GraphQLNonNull(
                  new GraphQLList(createInputType(name, columns))
                ),
              },
              on_conflict: { type: createOnConflictInputType(name, columns) },
            },
            resolve: async (_, { objects, on_conflict }, { db }) => {
              const result = await insertRecords(
                db,
                name,
                objects,
                on_conflict
              );
              return result;
            },
          },
          [`update_${name}_many`]: {
            type: new GraphQLList(createAffectedRows(name, columns, tables)),
            args: {
              updates: {
                type: new GraphQLNonNull(
                  new GraphQLList(
                    new GraphQLNonNull(
                      new GraphQLInputObjectType({
                        name: `${name}_updates_input`,
                        fields: {
                          where: {
                            type: createBoolExpInputType(name, columns),
                          },
                          ...(createIncType(name, columns)
                            ? { _inc: { type: createIncType(name, columns) } }
                            : {}),
                          _set: { type: createInputType(name, columns, "set") },
                        },
                      })
                    )
                  )
                ),
              },
            },
            resolve: async (_, { updates }, { db }) => {
              return await updateRecords(db, name, updates);
            },
          },
        }),
      {}
    ),
  });

export const createGetSchema = (db: DBWithHash) => {
  let schema: GraphQLSchema;
  let hash: string;
  return async () => {
    if (db.hash && db.hash === hash && schema) return schema;
    else if (!db.hash) console.warn("DB should have hash get property");
    else hash = db.hash;
    const tables = await db
      .query(
        sql`
      SELECT s.name AS table_name, info.*, fk."table" as "references", fk."to"
      FROM sqlite_schema AS s
      JOIN pragma_table_info(s.name) AS info ON 1=1
      LEFT JOIN pragma_foreign_key_list(s.name) AS fk ON fk."from" = info.name
      WHERE s.type = 'table' AND s.name NOT LIKE 'sqlite_%';
    `
      )
      .then((results) => results.group(({ table_name }) => table_name))
      .then(Object.entries)
      .then((arr) => arr.map(([name, columns]) => ({ name, columns })))
      .then((tables) =>
        tables.map(({ columns, ...obj }) => ({
          ...obj,
          columns: columns.map((obj) => ({
            gqltype: mapSqliteGraphql(obj.type, obj.pk, obj.notnull),
            ...obj,
          })),
        }))
      );

    const mutation = createMutationType(tables);
    schema = new GraphQLSchema({
      query: createQueryType(tables),
      mutation:
        Object.keys(mutation.getFields()).length > 0 ? mutation : undefined,
    });

    return schema;
  };
};

export const createGetTypeScriptDef = (
  getSchema: () => Promise<GraphQLSchema>
) => {
  let typeScriptDefinition: string;
  let schemaCache: string;
  return async () => {
    const schema = printSchema(await getSchema());
    if (schema && schema === schemaCache && typeScriptDefinition)
      return typeScriptDefinition;
    else if (!schema) console.warn("No schema provided");
    else schemaCache = schema;

    console.time("[codegen]");
    typeScriptDefinition = TreeToTS.resolveBasisTypes(Parser.parse(schema));
    console.timeEnd("[codegen]");
    return typeScriptDefinition;
  };
};

const readOnly = !!Deno.env.get("DENO_DEPLOYMENT_ID");

export const createSaveDefToFile = (
  getTypeScriptDef: ReturnType<typeof createGetTypeScriptDef>,
  name = "types.d.ts"
) => {
  let cache: string;
  return async () => {
    const typeScriptDefinition = await getTypeScriptDef();
    if (cache === typeScriptDefinition || readOnly) return;
    const fn = [Deno.cwd(), name].join("/");
    cache = typeScriptDefinition;
    if ((await Deno.readTextFile(fn).catch(() => "")) !== typeScriptDefinition)
      await Deno.writeTextFile(fn, typeScriptDefinition);
  };
};
// const prefixLog = (v) => `[gqlite] ${v}`;

export default (db: DBWithHash) => {
  const getSchema = createGetSchema(db);
  const getTypeScriptDef = createGetTypeScriptDef(getSchema);
  const saveDefToFile = createSaveDefToFile(getTypeScriptDef);
  return {
    getSchema,
    getTypeScriptDef,
    saveDefToFile,
    handler: async (req: Request): Promise<Response> => {
      console.time("[gqlite] get schema");
      const schema = await getSchema();
      console.timeEnd("[gqlite] get schema");
      await saveDefToFile();

      if (req.headers.get("upgrade") === "websocket") {
        // Handle GraphQL subscriptions
        return new Response(null, { status: 500 });
      } else {
        // Handle GraphQL queries and mutations
        console.time("[gqlite] GraphQLHTTP");
        const response = await GraphQLHTTP({
          schema,
          context: () => ({ db }),
          graphiql: true,
        })(req);
        console.timeEnd("[gqlite] GraphQLHTTP");
        return response;
      }
    },
  };
};
