# SQL Schema

rudis-cms automatically generates SQL tables based on your schema definition.

## Generated Tables

For a schema like:

```yaml
table: posts
schema:
  id:
    type: id
  title:
    type: string
    required: true
  date:
    type: date
    index: true
  body:
    type: markdown
    storage:
      type: kv
      namespace: content
  tags:
    type: records
    table: post_tags
    inherit_ids: [post_id]
    schema:
      tag:
        type: id
```

The following SQL is generated:

```sql
CREATE TABLE IF NOT EXISTS posts (
  id TEXT NOT NULL,
  title TEXT NOT NULL,
  date TEXT NOT NULL,
  body TEXT NOT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS index_posts_id ON posts(id);
CREATE INDEX IF NOT EXISTS index_posts_date ON posts(date(date));

CREATE TABLE IF NOT EXISTS post_tags (
  post_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
  PRIMARY KEY (post_id, tag)
);
CREATE INDEX IF NOT EXISTS index_post_tags_tag ON post_tags(tag);
```

## Type Mappings

| rudis-cms Type | SQLite Type | Notes |
|----------------|-------------|-------|
| `id` | `TEXT NOT NULL` | Primary key |
| `string` | `TEXT` | `NOT NULL` if required |
| `boolean` | `INTEGER` | 0 or 1 |
| `date` | `TEXT` | ISO 8601 format |
| `datetime` | `TEXT` | ISO 8601 format |
| `hash` | `TEXT` | BLAKE3 hash |
| `markdown` | `TEXT` | JSON with storage pointer |
| `image` | `TEXT` | JSON with storage pointer |
| `file` | `TEXT` | JSON with storage pointer |

## Storage Pointers

Content fields (markdown, image, file) are stored as JSON:

```json
{
  "hash": "abc123def456...",
  "size": 12345,
  "content_type": "text/markdown",
  "pointer": "kv://namespace-id/key"
}
```

## Foreign Keys

Child tables created by `records` type include:
- Foreign key constraint with `ON DELETE CASCADE`
- Composite primary key including parent IDs

## Viewing Generated SQL

Use the CLI to view generated SQL:

```bash
rudis-cms -c config.yaml show-schema sql
```
