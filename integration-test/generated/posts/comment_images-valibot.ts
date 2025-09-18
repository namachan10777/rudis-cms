import * as rudis from "../rudis-valibot.ts";
import * as v from "valibot";
export const imageColumn = rudis.imageReference(rudis.r2StoragePointer);
export const frontmatter = v.object({
  src_id: v.nullable(v.string()),
  image: imageColumn,
});
export const table = v.object({
  src_id: v.nullable(v.string()),
  image: v.pipe(v.string(), v.parseJson(), imageColumn),
});
export const frontmatterWithMarkdownColumns = v.object({
  src_id: v.nullable(v.string()),
  image: imageColumn,
});
