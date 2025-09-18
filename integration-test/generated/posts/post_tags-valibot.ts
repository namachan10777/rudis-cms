import * as rudis from "../rudis-valibot.ts";
import * as v from "valibot";
export const frontmatter = v.object({
  tag: v.nullable(v.string()),
});
export const table = v.object({
  tag: v.nullable(v.string()),
});
export const frontmatterWithMarkdownColumns = v.object({
  tag: v.nullable(v.string()),
});
