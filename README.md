# rudis-cms

A headless CMS that compiles Markdown/YAML documents into Cloudflare D1, R2, and KV.

[![Documentation](https://img.shields.io/badge/docs-mdBook-blue)](https://namachan10777.github.io/rudis-cms/)

## Features

- **Schema-driven**: Define your content structure in YAML, automatically generates SQL tables
- **Markdown with embedded images**: Images are extracted, optimized, and uploaded to R2
- **Multiple storage backends**: R2 (objects), KV (key-value), and inline storage
- **Incremental updates**: Only upload changed content, skip unchanged files
- **Local development**: Dump mode for testing with local SQLite

## Installation

```bash
git clone https://github.com/namachan10777/rudis-cms
cd rudis-cms
cargo install --path .
```

## Quick Start

1. Create a `config.yaml`:

```yaml
glob: "posts/**/*.md"
table: posts
database_id: your-d1-database-id
syntax:
  type: markdown
  column: body
schema:
  id:
    type: id
  title:
    type: string
    required: true
  body:
    type: markdown
    storage:
      type: kv
      namespace: your-kv-namespace
```

2. Deploy:

```bash
rudis-cms -c config.yaml batch
```

## Documentation

- [English](https://namachan10777.github.io/rudis-cms/en/)
- [日本語](https://namachan10777.github.io/rudis-cms/ja/)

## License

MIT
