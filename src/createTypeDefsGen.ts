import {
  getIntrospectionQuery,
  buildClientSchema,
  printSchema,
  parse,
  type GraphQLSchema,
} from "https://deno.land/x/graphql_deno@v15.0.0/mod.ts";
import { GraphQLFetchResult, GraphQLOptions } from "../mod.ts";

const readOnly = !!Deno.env.get("DENO_DEPLOYMENT_ID");

const createCache = () => {
  const cacheMap = new Map();
  return {
    setItem: (key: string, value: any) => {
      return globalThis.localStorage
        ? globalThis.localStorage.setItem(String(key), JSON.stringify(value))
        : cacheMap.set(key, value);
    },
    getItem: (key: string) => {
      if (!globalThis.localStorage) return cacheMap.get(key);
      const value = localStorage.getItem(String(key));
      return value ? JSON.parse(value) : value;
    },
    has: (key: string) =>
      globalThis.localStorage
        ? localStorage.getItem(String(key)) !== null
        : cacheMap.has(key),
  };
};

export const createGetSchemaFromUrl = (
  graphqlClient: (arg: { query: string }) => Promise<GraphQLFetchResult>
) => {
  return async () => {
    console.time("[introspection]");
    const { data, errors } = await graphqlClient({
      query: getIntrospectionQuery(),
    });
    if (errors) console.error(errors);
    const graphqlSchemaObj = data ? buildClientSchema(data) : null;
    console.timeEnd("[introspection]");
    return graphqlSchemaObj;
  };
};

export const createGetTypeScriptDef = (
  getSchema: () => Promise<GraphQLSchema>
) => {
  const localCache = createCache();

  return async () => {
    if (readOnly) return;
    const typescriptPlugin = await import(
      "npm:@graphql-codegen/typescript@4.0.0"
    );
    const { codegen } = await import("npm:@graphql-codegen/core@4.0.0");

    const schema = printSchema(await getSchema());
    const cache = await (async () =>
      await localCache.getItem(import.meta.url))().catch(() => null);
    if (schema && schema === cache?.schema && cache?.typeScriptDefinition)
      return cache.typeScriptDefinition;
    else if (!schema) console.warn("No schema provided");
    console.time("[codegen]");
    const config = {
      documents: [],
      config: {},
      schema: parse(schema),
      plugins: [{ typescript: {} }],
      pluginMap: {
        typescript: typescriptPlugin,
      },
    };
    const typeScriptDefinition = await codegen(config);
    console.timeEnd("[codegen]");
    localCache.setItem(import.meta.url, { typeScriptDefinition, schema });
    return typeScriptDefinition;
  };
};

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

export const createTypes =
  (fetcher: (options: { query: string }) => Promise<GraphQLFetchResult>) =>
  () =>
    createSaveDefToFile(
      createGetTypeScriptDef(createGetSchemaFromUrl(fetcher))
    )().catch(console.error);

export const createTypeDefsGen = (
  fetcher: <TData = any, TVariables = any>(
    options: GraphQLOptions<TData, TVariables>
  ) => Promise<GraphQLFetchResult<TData>>
) => ({
  saveTypeDefs: createSaveDefToFile(
    createGetTypeScriptDef(createGetSchemaFromUrl(fetcher))
  ),
  getTypeDefs: createGetTypeScriptDef(createGetSchemaFromUrl(fetcher)),
});
