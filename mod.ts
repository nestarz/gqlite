import "https://deno.land/x/corejs@v3.26.1/index.js";
import sql from "https://esm.sh/noop-tag";
import { GraphQLHTTP } from "https://gist.githubusercontent.com/nestarz/7c0275b94b4e18ed1a108237732fd57f/raw/c6b2d8e9f0191536e09997c1383478f37400cac5/gql.http.ts";
import {
  GraphQLObjectType,
  GraphQLNonNull,
  GraphQLInt,
  printSchema,
  GraphQLString,
  GraphQLSchema,
  GraphQLList,
  GraphQLInputObjectType,
  GraphQLEnumType,
  subscribe,
  parse,
} from "https://deno.land/x/graphql_deno@v15.0.0/mod.ts";
import { search as jmespath } from "https://deno.land/x/jmespath@v0.2.2/index.ts";

const noopLog = (v) => (console.log(v), v);

const GraphQLSubscribe = (schema, ws) => async (v) => {
  const { query, mutation, variables, operationName } =
    JSON.parse(v.data)?.payload ?? {};
  const source = query ?? mutation;
  if (source) {
    for await (const data of await subscribe({
      document: parse(source),
      variableValues: variables,
      operationName,
      schema,
    })) {
      if (ws.readyState !== 1) break;
      ws.send(JSON.stringify({ type: "data", id: "1", payload: { data } }));
    }
  }
};

const MAP_SQLITE_GRAPHQL = {
  INT: GraphQLInt,
  INTEGER: GraphQLInt,
  TEXT: GraphQLString,
};

const mapSqliteGraphql = (type, notnull) =>
  notnull
    ? new GraphQLNonNull(MAP_SQLITE_GRAPHQL[type])
    : MAP_SQLITE_GRAPHQL[type];

const byPk = (name) => [name, "by_pk"].join("_");
const insertOne = (name) => ["insert", name, "one"].join("_");
const insertMany = (name) => ["insert", name].join("_");
const deleteOne = (name) => ["delete", name, "one"].join("_");
const updateOne = (name) => ["update", name, "one"].join("_");
const insertInput = (name) => `${name}_insert_input`;
const onConflictInput = (name) => `${name}_on_conflict`;
const constraint = (name) => `${name}_constraint`;
const setInput = (name) => `${name}_set_input`;
const pkInput = (name) => `${name}_pk_columns_input`;
const removeUndefined = (obj) =>
  Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined)
  );
const isJSON = (obj) => { try { return JSON.parse(obj); } catch (error) { return false; } }; // prettier-ignore
const concatObjects = [(prev, v) => ({ ...prev, ...removeUndefined(v) }), {}];
const throwErr = (arr) => { throw Error(JSON.stringify(arr)) }; // prettier-ignore
const checkError = (arr) =>
  Array.isArray(arr) ? arr : throwErr(arr?.error ?? arr);
const extractMany = (res) => checkError(res);
const extractOne = (r) => extractMany(r)[0];
const getFields = (ctx): string[] =>
  ctx.fieldNodes[0].selectionSet.selections.map((d) => d.name.value);

const polling = (db, tableName: string) => ({
  async *[Symbol.asyncIterator]() {
    while (true) {
      const ids = db
        .query(sql`SELECT id FROM ${tableName}`)
        .then(extractMany)
        .then((results) => new Set(results.map((r) => r.id)))
        .catch(console.error);
      if (ids) yield ids;
      await new Promise((res) => setTimeout(res, 1000));
    }
  },
});

const listen = (db, tableName: string, fields: string[]) => ({
  ids: new Set(),
  async *[Symbol.asyncIterator]() {
    for await (const currentIds of polling(db, tableName)) {
      const ids = [...new Set([...currentIds].filter((i) => !this.ids.has(i)))];
      const ph = ids.map(() => "?").join(",");
      const data = await db
        .query(
          sql`SELECT ${fields
            .map((d) => `"${d}"`)
            .join(", ")} FROM ${tableName} WHERE "id" IN (${ph})`,
          ids
        )
        .then(extractMany)
        .catch(console.error);
      if (!data) continue;
      currentIds.forEach((id) => this.ids.add(id));
      if (ids.length > 0) yield { [tableName]: data };
    }
  },
});

export default async (db) => {
  const queries = await db
    .query(
      sql`
        SELECT
          s.name AS table_name,
          info.*
        FROM
          sqlite_schema AS s,
          pragma_table_info (s.name) AS info
        WHERE
          s.type = 'table'
          AND s.name NOT LIKE 'sqlite_%';
        
      `
    )
    .then(extractMany)
    .then((results) => results.group(({ table_name }) => table_name))
    .then(Object.entries)
    .then((arr) => arr.map(([tableName, columns]) => ({ tableName, columns })));

  const tableTypes = queries.map(
    ({ tableName: name, columns }) =>
      new GraphQLObjectType({
        name,
        fields: columns
          .map(({ name, type, notnull }) => ({
            [name]: {
              type: mapSqliteGraphql(type, notnull),
              args:
                mapSqliteGraphql(type, false) === GraphQLString
                  ? { path: { type: GraphQLString } }
                  : {},
              resolve: async (parent, { path }) => {
                const withPath = path && isJSON(parent[name]);
                const result = withPath
                  ? jmespath(JSON.parse(parent[name]), path)
                  : parent[name];
                return !withPath ||
                  typeof result === "string" ||
                  result === null
                  ? result
                  : JSON.stringify(result);
              },
            },
          }))
          .reduce(...concatObjects),
      })
  );

  const queryTypes = ["Query", "Subscription"].map(
    (rootName) =>
      new GraphQLObjectType({
        name: rootName,
        fields: queries
          .flatMap(({ tableName, columns }) => [
            {
              tableName,
              typeName: tableName,
              type: new GraphQLList(
                tableTypes.find((v) => v.name === tableName)
              ),
            },
            {
              byPk: true,
              tableName,
              typeName: byPk(tableName),
              type: tableTypes.find((v) => v.name === tableName),
              args: Object.fromEntries(
                columns
                  .filter(({ pk }) => pk)
                  .map(({ name, type }) => [
                    name,
                    { type: mapSqliteGraphql(type, true) },
                  ])
              ),
            },
          ])
          .reduce(
            (prev, { byPk, typeName, tableName, ...props }) => ({
              ...prev,
              [typeName]: {
                ...props,
                resolve:
                  rootName === "Subscription"
                    ? undefined
                    : async (_, args, __, ctx) => {
                        const fields = getFields(ctx);
                        return await db
                          .query(
                            sql`SELECT ${fields
                              .map((d) => `"${d}"`)
                              .join(", ")} FROM ${tableName} ` +
                              (byPk
                                ? sql`WHERE ${Object.keys(args)
                                    .map((d) => `"${d}"`)
                                    .map((name) => [name, "?"].join("="))
                                    .join(sql` AND `)}`
                                : ""),
                            Object.values(args)
                          )
                          .then(byPk ? extractOne : extractMany);
                      },
                subscribe: (...props) =>
                  listen(db, tableName, getFields(props[3])),
              },
            }),
            []
          ),
      })
  );

  const insertInputTypes = queries
    .map(({ tableName, columns }) => ({
      [tableName]: new GraphQLInputObjectType({
        name: insertInput(tableName),
        fields: columns
          .map(({ name, type, notnull }) => ({
            [name]: { type: mapSqliteGraphql(type, notnull) },
          }))
          .reduce(...concatObjects),
      }),
    }))
    .reduce(...concatObjects);

  const typeConstraints = queries
    .filter(({ columns }) => columns.some(({ pk }) => pk))
    .map(({ tableName, columns }) => ({
      [tableName]: new GraphQLEnumType({
        name: constraint(tableName),
        values: columns
          .filter(({ pk }) => pk)
          .map(({ name: value }) => ({
            [`${tableName}_pkey`]: { value },
          }))
          .reduce(...concatObjects),
      }),
    }))
    .reduce(...concatObjects);

  const onConflictInputTypes = queries
    .filter(({ tableName }) => typeConstraints[tableName])
    .map(({ tableName }) => ({
      [tableName]: new GraphQLInputObjectType({
        name: onConflictInput(tableName),
        fields: {
          constraint: {
            type: new GraphQLNonNull(typeConstraints[tableName]),
          },
          update_columns: {
            type: new GraphQLNonNull(
              new GraphQLList(new GraphQLNonNull(GraphQLString))
            ),
          },
        },
      }),
    }))
    .reduce(...concatObjects);

  const setInputTypes = queries
    .map(({ tableName, columns }) => ({
      [tableName]: new GraphQLInputObjectType({
        name: setInput(tableName),
        fields: columns
          .map(({ name, type, notnull }) => ({
            [name]: { type: mapSqliteGraphql(type, notnull) },
          }))
          .reduce(...concatObjects),
      }),
    }))
    .reduce(...concatObjects);

  const pkInputTypes = queries
    .filter((v) => v.columns.some((v) => v.pk))
    .map(({ tableName, columns }) => ({
      [tableName]: new GraphQLInputObjectType({
        name: pkInput(tableName),
        fields: columns
          .filter(({ pk }) => pk)
          .map(({ name, type, notnull }) => ({
            [name]: { type: mapSqliteGraphql(type, notnull) },
          }))
          .reduce(...concatObjects),
      }),
    }))
    .reduce(...concatObjects);

  const mutationTypes = new GraphQLObjectType({
    name: "Mutation",
    fields: queries
      .map(({ tableName, columns }) => ({
        [insertMany(tableName)]: {
          type: tableTypes.find((v) => v.name === tableName),
          args: {
            objects: {
              type: new GraphQLNonNull(
                new GraphQLList(new GraphQLNonNull(insertInputTypes[tableName]))
              ),
            },
          },
          resolve: async (_, { objects }) => {
            const ph = objects
              .map(
                (object) =>
                  `(${Object.keys(object)
                    .map(() => `?`)
                    .join(",")})`
              )
              .join(", ");
            return await db
              .query(
                sql`INSERT INTO ${tableName} (${Object.keys(objects[0])
                  .map((d) => `"${d}"`)
                  .join(",")}) VALUES ${ph} RETURNING *`,
                objects.flatMap((object) => Object.values(object))
              )
              .then(extractMany);
          },
        },
        [insertOne(tableName)]: {
          type: tableTypes.find((v) => v.name === tableName),
          args: {
            object: { type: new GraphQLNonNull(insertInputTypes[tableName]) },
            ...(onConflictInputTypes[tableName]
              ? {
                  on_conflict: {
                    type: new GraphQLNonNull(onConflictInputTypes[tableName]),
                  },
                }
              : {}),
          },
          resolve: async (_, { object, on_conflict }) => {
            const { update_columns, constraint } = on_conflict ?? {};
            const updateOrIgnoreStatement =
              update_columns?.length > 0
                ? sql`UPDATE SET ${update_columns
                    .map(
                      (field) => sql`"${field}" = ${sql`excluded."${field}"`}`
                    )
                    .join(sql`, `)}`
                : sql`NOTHING`;
            const onConflictStatement = constraint
              ? sql`ON CONFLICT (${constraint}) 
                DO ${updateOrIgnoreStatement}`
              : sql``;
            return await db
              .query(
                sql`INSERT INTO ${tableName} (${Object.keys(object)
                  .map((d) => `"${d}"`)
                  .join(",")}) VALUES (${Object.keys(object)
                  .map(() => `?`)
                  .join(",")}) 
                  ${onConflictStatement}
                  RETURNING *`,
                Object.values(object)
              )
              .then(extractOne);
          },
        },
        [deleteOne(tableName)]: !pkInputTypes[tableName]
          ? undefined
          : {
              type: tableTypes.find((v) => v.name === tableName),
              args: columns
                .filter(({ pk }) => pk)
                .map(({ name, type }) => ({
                  [name]: { type: mapSqliteGraphql(type, true) },
                }))
                .reduce(...concatObjects),
              resolve: async (_, args) => {
                return await db
                  .query(
                    sql`DELETE FROM ${tableName} WHERE ${Object.keys(args)
                      .map((d) => `"${d}"`)
                      .map((k) => [k, "?"].join("="))
                      .join(sql` AND `)} RETURNING *`,
                    Object.values(args)
                  )
                  .then(extractOne);
              },
            },
        [updateOne(tableName)]: !pkInputTypes[tableName]
          ? undefined
          : {
              type: tableTypes.find((v) => v.name === tableName),
              args: {
                _set: { type: new GraphQLNonNull(setInputTypes[tableName]) },
                pk_columns: {
                  type: new GraphQLNonNull(pkInputTypes[tableName]),
                },
              },
              resolve: async (_, { _set, pk_columns, _relationships }) => {
                return await db
                  .query(
                    sql`${
                      _set
                        ? sql`UPDATE ${tableName} SET ${Object.keys(_set)
                            .map((d) => `"${d}"`)
                            .map((k) => [k, "?"].join("="))
                            .join(", ")}`
                        : sql`SELECT * FROM ${tableName}`
                    } WHERE ${Object.keys(pk_columns)
                      .map((d) => `"${d}"`)
                      .map((k) => [k, "?"].join("="))
                      .join(sql` AND `)} ${_set ? sql`RETURNING *` : sql``}`,
                    [...Object.values(_set ?? {}), ...Object.values(pk_columns)]
                  )
                  .then(extractOne);
              },
            },
      }))
      .reduce(...concatObjects),
  });

  return async (req: Request) => {
    const DEBUG = false;
    const schema = new GraphQLSchema(
      Object.fromEntries(
        [
          ...queryTypes,
          mutationTypes,
          ...Object.values(insertInputTypes),
          ...Object.values(onConflictInputTypes),
        ].map((type) => [type.name.toLowerCase(), type])
      )
    );
    if (DEBUG) console.log(printSchema(schema));
    if (req.headers.get("upgrade") === "websocket") {
      const { socket: ws, response } = Deno.upgradeWebSocket(req, {
        protocol: "graphql-ws",
      });
      ws.addEventListener("message", GraphQLSubscribe(schema, ws));
      return response;
    }
    const url = new URL(req.url);
    return await GraphQLHTTP<Request>({
      schema,
      graphiql: true,
      playgroundOptions: {
        subscriptionEndpoint: Object.assign(url, {
          protocol: url.protocol === "http:" ? "ws" : "wss",
        }),
      },
    })(req);
  };
};
