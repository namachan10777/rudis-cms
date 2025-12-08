# Field Types

rudis-cms supports various field types for different data needs.

## Basic Types

### id

Primary identifier for the record.

```yaml
id:
  type: id
```

- Stored as `TEXT` in SQLite
- Automatically indexed
- Must be unique within the table

### string

Text content.

```yaml
title:
  type: string
  required: true
```

- Stored as `TEXT` in SQLite

### boolean

True/false values.

```yaml
published:
  type: boolean
  index: true
```

- Stored as `INTEGER` (0/1) in SQLite

### date

Date without time.

```yaml
published_date:
  type: date
  index: true
```

- Stored as `TEXT` in ISO 8601 format (YYYY-MM-DD)
- Indexed using `date()` function

### datetime

Date with time.

```yaml
created_at:
  type: datetime
  index: true
```

- Stored as `TEXT` in ISO 8601 format
- Indexed using `datetime()` function

### hash

Content hash for change detection.

```yaml
hash:
  type: hash
```

- Automatically computed from file content
- Useful for cache invalidation

## Content Types

### markdown

Markdown content with optional image extraction.

```yaml
body:
  type: markdown
  required: true
  storage:
    type: kv
    namespace: content-namespace
  image:
    table: post_images
    inherit_ids: [post_id]
    embed_svg_threshold: 8192
    storage:
      type: r2
      bucket: my-bucket
      prefix: images
  config: {}
```

Options:
- `storage`: Where to store the compiled markdown
- `image`: Configuration for extracted images
- `image.embed_svg_threshold`: SVG files smaller than this (bytes) are embedded inline
- `config`: Additional markdown processing options

### image

Single image field.

```yaml
og_image:
  type: image
  storage:
    type: r2
    bucket: my-bucket
    prefix: og-images
```

### file

Generic file attachment.

```yaml
attachment:
  type: file
  storage:
    type: r2
    bucket: my-bucket
    prefix: attachments
```

## Relational Types

### records

Nested table with multiple records.

```yaml
tags:
  type: records
  table: post_tags
  inherit_ids: [post_id]
  schema:
    tag:
      type: id
```

Creates a separate table with foreign key relationship.
