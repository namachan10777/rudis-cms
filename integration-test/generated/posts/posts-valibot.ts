import * as rudis from "../rudis-valibot.ts";
import * as v from "valibot";
export const ogImageColumn = rudis.imageReference(rudis.r2StoragePointer);
export const frontmatter = v.object({
  id: v.nullable(v.string()),
  hash: v.string(),
  og_image: v.nullable(ogImageColumn),
  publish: v.nullable(v.boolean()),
  published: v.pipe(
    v.string(),
    v.transform((datetime) => new Date(datetime)),
  ),
  commentable: v.nullable(v.boolean()),
  comments: v.array(comments.frontmatterWithMarkdownColumns),
  tags: v.array(post_tags.frontmatterWithMarkdownColumns),
});
export const bodyKeep = v.union([
  rudis.alertKeep,
  rudis.footnoteReferenceKeep,
  rudis.linkCardKeep,
  rudis.codeblockKeep,
  rudis.headingKeep,
  rudis.imageKeep(rudis.r2StoragePointer),
]);
export const bodyRoot = rudis.markdownRoot(bodyKeep);
export const bodyDocument = rudis.markdownDocument(frontmatter, bodyKeep);
export const bodyColumn = rudis.markdownReference(rudis.kvStoragePointer);
import * as comments from "./comments-valibot.ts";
import * as post_tags from "./post_tags-valibot.ts";
export const table = v.object({
  id: v.nullable(v.string()),
  hash: v.string(),
  body: v.nullable(v.pipe(v.string(), v.parseJson(), bodyColumn)),
  og_image: v.nullable(v.pipe(v.string(), v.parseJson(), ogImageColumn)),
  publish: v.nullable(
    v.pipe(
      v.number(),
      v.integer(),
      v.transform((flag) => flag === 1),
      v.boolean(),
    ),
  ),
  published: v.pipe(
    v.string(),
    v.transform((datetime) => new Date(datetime)),
  ),
  commentable: v.nullable(
    v.pipe(
      v.number(),
      v.integer(),
      v.transform((flag) => flag === 1),
      v.boolean(),
    ),
  ),
});
export const frontmatterWithMarkdownColumns = v.object({
  id: v.nullable(v.string()),
  hash: v.string(),
  body: v.nullable(bodyColumn),
  og_image: v.nullable(ogImageColumn),
  publish: v.nullable(v.boolean()),
  published: v.pipe(
    v.string(),
    v.transform((datetime) => new Date(datetime)),
  ),
  commentable: v.nullable(v.boolean()),
  comments: v.array(comments.frontmatterWithMarkdownColumns),
  tags: v.array(post_tags.frontmatterWithMarkdownColumns),
});
