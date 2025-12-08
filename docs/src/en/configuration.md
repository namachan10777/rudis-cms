# Configuration

rudis-cms uses a YAML configuration file to define your content structure.

## Basic Structure

```yaml
glob: "posts/**/*.md"      # File pattern to match
name: posts                 # Collection name
table: posts               # Main table name
database_id: xxx-xxx       # Cloudflare D1 database ID
syntax:
  type: markdown           # or "yaml"
  column: body             # Column for markdown content (markdown only)
schema:
  # Field definitions...
```

## Top-Level Options

| Option | Required | Description |
|--------|----------|-------------|
| `glob` | Yes | Glob pattern for content files |
| `name` | Yes | Collection name |
| `table` | Yes | Main database table name |
| `database_id` | Yes | Cloudflare D1 database ID |
| `preview_database_id` | No | Separate D1 database for preview |
| `syntax` | Yes | Content format configuration |
| `schema` | Yes | Field definitions |

## Syntax Options

### Markdown

```yaml
syntax:
  type: markdown
  column: body    # Field name for the markdown content
```

With markdown syntax, frontmatter fields are mapped to schema fields, and the body content is stored in the specified column.

### YAML

```yaml
syntax:
  type: yaml
```

With YAML syntax, the entire file is parsed as YAML and mapped to schema fields.

## Preview Database

You can specify a separate database for preview/draft content:

```yaml
database_id: production-db-id
preview_database_id: preview-db-id
```

Use the `--preview` flag to deploy to the preview database instead.
