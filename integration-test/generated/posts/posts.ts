import * as rudis from "../rudis.ts";
import * as comments from "./comments.ts";
import * as post_tags from "./post_tags.ts";
export type BodyKeep =
  | rudis.AlertKeep
  | rudis.FootnoteReferenceKeep
  | rudis.LinkCardKeep
  | rudis.CodeblockKeep
  | rudis.HeadingKeep
  | rudis.ImageKeep<rudis.R2StoragePointer>;
export type BodyRoot = rudis.MarkdownRoot<BodyKeep>;
export type BodyDocument = rudis.MarkdownDocument<Frontmatter, BodyKeep>;
export type BodyColumn = rudis.MarkdownReference<rudis.KvStoragePointer>;
export type OgImageColumn = rudis.ImageReference<rudis.R2StoragePointer>;
export interface Table {
  id: string | null;
  hash: string;
  body: BodyColumn | null;
  og_image: OgImageColumn | null;
  publish: boolean | null;
  published: Date;
  commentable: boolean | null;
}
export interface Frontmatter {
  id: string | null;
  hash: string;
  og_image: OgImageColumn | null;
  publish: boolean | null;
  published: Date;
  commentable: boolean | null;
  comments: comments.FrontmatterWithMarkdownColumns[];
  tags: post_tags.FrontmatterWithMarkdownColumns[];
}
export interface FrontmatterWithMarkdownColumns {
  id: string | null;
  hash: string;
  body: BodyColumn | null;
  og_image: OgImageColumn | null;
  publish: boolean | null;
  published: Date;
  commentable: boolean | null;
  comments: comments.FrontmatterWithMarkdownColumns[];
  tags: post_tags.FrontmatterWithMarkdownColumns[];
}
