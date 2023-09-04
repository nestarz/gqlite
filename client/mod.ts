import type { TypedDocumentNode } from "https://esm.sh/@graphql-typed-document-node/core@3.2.0";

type JSON = { [key: string]: unknown };

export type GraphQLFetchResult<TData = any> = {
  data?: TData;
  errors?: Error[];
};

export type ExactStructure<T, U> = T & {
  [K in keyof T]: K extends keyof U
    ? T[K] extends Record<string, unknown> | undefined
      ? ExactStructure<NonNullable<T[K]>, NonNullable<U[K]>>
      : T[K]
    : never;
};

export type GraphQLClientRequestHeaders = Headers | Record<string, string>;

export const gql = (l: any, ...o: any[]): string => {
  let t = l[0];
  for (let e = 1, r = l.length; e < r; e++) (t += o[e - 1]), (t += l[e]);
  return t;
};

export interface GraphQLOptions<TData, TVariables> {
  query: TypedDocumentNode<TData, TVariables> | string;
  variables?: TVariables;
  url?: string;
  handler?: (req: Request) => Promise<Response>;
}

export const createGraphqlFromHandler =
  (
    handler: (req: Request) => Promise<Response>,
    config?: {
      requestHeaders?: GraphQLClientRequestHeaders;
      requestInput?: RequestInfo | URL;
      afterHook?: <T extends JSON>(
        value: GraphQLFetchResult<T>
      ) => GraphQLFetchResult<T>;
    }
  ) =>
  <TData = JSON, TVariables = Record<string, any>>(
    options: GraphQLOptions<TData, TVariables>
  ): Promise<GraphQLFetchResult<TData>> =>
    handler(
      new Request(config?.requestInput ?? "https://localhost", {
        method: "POST",
        body: JSON.stringify(options),
        headers: {
          "content-type": "application/json",
          ...(config?.requestHeaders ?? {}),
        },
      })
    ).then(async (r) => {
      if (r.ok)
        return r.json().then((value) => config?.afterHook?.(value) ?? value);
      throw Error(await r.text());
    });

export const request = <TData = any, TVariables = Record<string, any>>(
  requestInput: RequestInfo | URL,
  query: string,
  variables?: Record<string, unknown>,
  requestHeaders?: GraphQLClientRequestHeaders
) =>
  createGraphqlFromHandler(fetch, { requestHeaders, requestInput })<
    TData,
    TVariables | Record<string, any>
  >({ query, variables });

export const createGraphQLClient = (
  url: string,
  requestHeaders?: GraphQLClientRequestHeaders
) => ({
  request: <T = any, U = Record<string, any>>(
    query: string,
    variables?: Record<string, unknown>
  ) => request<T, U>(url, query, variables, requestHeaders),
});

export type GraphQLClient = ReturnType<typeof createGraphQLClient>;

export const createGraphqlFromHandlerWithJSON: typeof createGraphqlFromHandler =
  (handler, config) =>
    createGraphqlFromHandler(handler, {
      ...config,
      afterHook: <T>(payload): GraphQLFetchResult<T> => {
        const tryParseJSON = (str: string): unknown => {
          try { return JSON.parse(str); } catch { return str; } // prettier-ignore
        };
        const parseJSONKeys = (
          obj: JSON | undefined,
          test: (key: string) => boolean
        ): JSON => {
          const result = Array.isArray(obj) ? [] : {};
          for (const key in obj)
            if (Object.hasOwn(obj, key))
              if (typeof obj[key] === "object" && obj[key] !== null)
                result[key] = parseJSONKeys(obj[key] as JSON, test);
              else if (typeof obj[key] === "string" && test(key))
                result[key] = tryParseJSON(obj[key] as string);
              else result[key] = obj[key];
          return result;
        };
        return {
          ...payload,
          data: parseJSONKeys(payload.data, (key) =>
            /\b(?:((?:\w*_)*)(json)((?:_\w*)*))\b/.test(key)
          ),
        };
      },
    });
