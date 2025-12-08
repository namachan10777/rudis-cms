# Schema Definition

The schema defines how your content is structured and stored in the database.

## Field Definition

Each field in the schema has a name and configuration:

```yaml
schema:
  field_name:
    type: string
    required: true
    index: true
```

## Common Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | - | Field type (required) |
| `required` | bool | false | Whether the field is required |
| `index` | bool | false | Create a database index |

## Nested Records

You can define nested tables using the `records` type:

```yaml
schema:
  id:
    type: id
  comments:
    type: records
    table: post_comments
    inherit_ids: [post_id]
    schema:
      comment_id:
        type: id
      text:
        type: string
```

This creates a separate `post_comments` table with a foreign key relationship.

### inherit_ids

The `inherit_ids` option specifies which parent IDs to include in the child table:

```yaml
comments:
  type: records
  inherit_ids: [post_id]  # Include post_id as foreign key
```

For deeply nested records, you can inherit multiple IDs:

```yaml
replies:
  type: records
  inherit_ids: [post_id, comment_id]
```

## Example

```yaml
schema:
  id:
    type: id
  title:
    type: string
    required: true
  published:
    type: datetime
    index: true
  draft:
    type: boolean
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
