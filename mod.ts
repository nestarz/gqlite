import gqlite, { type DBWithHash } from "./src/gqlite.ts";

export { default as sql } from "https://esm.sh/noop-tag@2.0.0";

export default async (db: DBWithHash) => {
  const { handler, getTypeScriptDef, saveDefToFile } = await gqlite(db);
  await saveDefToFile();
  return {
    "": handler,
    "GET@/types.d.ts": async () => {
      return new Response(await getTypeScriptDef(), {
        headers: { "content-type": "application/typescript" },
      });
    },
  };
};
