import * as rudis from "../rudis-valibot.ts";
import * as v from "valibot";
export const frontmatter = v.object({
  comment_id: v.nullable(v.string()),
});
export const commentKeep = v.union([
  rudis.alertKeep,
  rudis.footnoteReferenceKeep,
  rudis.linkCardKeep,
  rudis.codeblockKeep,
  rudis.headingKeep,
  rudis.imageKeep(rudis.r2StoragePointer),
]);
export const commentRoot = rudis.markdownRoot(commentKeep);
export const commentColumn = rudis.markdownReference(
  rudis.inlineStoragePointer,
);
export const table = v.object({
  comment_id: v.nullable(v.string()),
  comment: v.nullable(v.pipe(v.string(), v.parseJson(), commentColumn)),
});
export const frontmatterWithMarkdownColumns = v.object({
  comment_id: v.nullable(v.string()),
  comment: v.nullable(commentColumn),
});
