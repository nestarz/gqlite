export const gql = (l, ...o) => {
  let t = l[0];
  for (let e = 1, r = l.length; e < r; e++) (t += o[e - 1]), (t += l[e]);
  return t;
};

export type ExactStructure<T, U> = T & {
  [K in keyof T]: K extends keyof U
    ? T[K] extends Record<string, unknown> | undefined
      ? ExactStructure<NonNullable<T[K]>, NonNullable<U[K]>>
      : T[K]
    : never;
};

export const tryParseJSON = (str: string): unknown => {
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
};

export type JsonObject = { [key: string]: unknown };

export const parseJSONKeys = (
  obj: JsonObject,
  test: (key: string) => boolean
): JsonObject => {
  const result: JsonObject = Array.isArray(obj) ? [] : {};
  for (const key in obj) {
    if (Object.hasOwn(obj, key)) {
      if (typeof obj[key] === "object" && obj[key] !== null) {
        result[key] = parseJSONKeys(obj[key], test);
      } else if (test(key)) {
        result[key] = tryParseJSON(obj[key]);
      } else {
        result[key] = obj[key];
      }
    }
  }
  return result;
};

interface GraphQLOptions<TVariables> {
  query: DocumentNode;
  variables?: TVariables;
  url?: string;
  handler?: (req: Request) => Promise<Response>;
}

export default function graphql<TData = any, TVariables = Record<string, any>>({
  query,
  variables,
  handler,
  url,
}: GraphQLOptions<TVariables>): Promise<TData> {
  const req = new Request(url, {
    method: "POST",
    body: JSON.stringify({ query, variables }),
  });
  return (handler ?? fetch)(req)
    .then((r) => r.json())
    .then((data: { data: TData; errors: Error[] }) => {
      if (data.errors) {
        console.error(query, data.errors);
        throw Error(JSON.stringify(data.errors));
      }
      return data;
    })
    .then(
      ({ data }) =>
        parseJSONKeys(data as any, (key) =>
          /\b(?:((?:\w*_)*)(json)((?:_\w*)*))\b/.test(key)
        ) as TData
    );
  //.then((v) => (console.log(v), v));
}

export const createGraphqlFromHandler =
  (handler: (req: Request) => Promise<Response>) =>
  <TData = any, TVariables = Record<string, any>>(
    options: GraphQLOptions<TVariables>
  ) =>
    graphql<TData, TVariables>({
      handler,
      url: "https://localhost",
      ...options,
    });
