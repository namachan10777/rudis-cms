import * as rudis from "../rudis.ts";
export type CommentKeep =
  | rudis.AlertKeep
  | rudis.FootnoteReferenceKeep
  | rudis.LinkCardKeep
  | rudis.CodeblockKeep
  | rudis.HeadingKeep
  | rudis.ImageKeep<rudis.R2StoragePointer>;
export type CommentRoot = rudis.MarkdownRoot<CommentKeep>;
export type CommentColumn = rudis.MarkdownReference<rudis.InlineStoragePointer>;
export interface Table {
  comment_id: string | null;
  comment: CommentColumn | null;
}
export interface Frontmatter {
  comment_id: string | null;
}
export interface FrontmatterWithMarkdownColumns {
  comment_id: string | null;
  comment: CommentColumn | null;
}
