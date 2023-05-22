import gqlite, { type DBWithHash } from "./src/gqlite.js";

export { default as sql } from "https://esm.sh/noop-tag";

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
