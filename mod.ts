import type { MiddlewareHandlerContext } from "https://deno.land/x/fresh@1.4.2/server.ts";

import gqlite, { type DBWithHash } from "./src/gqlite.ts";
import { createTypeDefsGen } from "./src/createTypeDefsGen.ts";
import { createGraphqlFromHandlerWithJSON } from "./client/mod.ts";

export * from "./client/mod.ts";

export type GQLiteClientContext = {
  graphQLClient: ReturnType<typeof createGraphqlFromHandlerWithJSON>;
};

export const createGQLite = async (
  db: DBWithHash,
  options: { disableTypeScriptDefinitionsGeneration: boolean }
) => {
  const { handler } = await gqlite(db);
  const client = createGraphqlFromHandlerWithJSON(handler);
  const { saveTypeDefs, getTypeDefs } = createTypeDefsGen(client);

  if (!options.disableTypeScriptDefinitionsGeneration)
    await saveTypeDefs().catch(console.error);

  return {
    handler,
    client,
    saveTypeDefs,
    getTypeDefs,
    freshMiddleware: async (
      _req: Request,
      ctx: MiddlewareHandlerContext<{ graphQLClient: typeof client }>
    ) => {
      ctx.state.graphQLClient = client;
      return await ctx.next();
    },
    typeDefsHandler: async () => {
      return new Response(await getTypeDefs(), {
        headers: { "content-type": "application/typescript" },
      });
    },
  };
};

export default createGQLite;
