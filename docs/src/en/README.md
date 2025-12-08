# rudis-cms

**rudis-cms** is a headless CMS that compiles Markdown/YAML documents into Cloudflare D1 (SQLite), R2 (object storage), and KV.

## Features

- **Schema-driven**: Define your content structure in YAML, and rudis-cms generates SQL tables automatically
- **Markdown with embedded images**: Images in Markdown are automatically extracted, optimized, and uploaded to R2
- **Multiple storage backends**: Support for R2 (objects), KV (key-value), and inline storage
- **Incremental updates**: Only upload changed content, skip unchanged files
- **Local development**: Dump mode for testing with local SQLite and file storage

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Markdown/YAML  │────▶│  rudis-cms   │────▶│  Cloudflare D1  │
│     Documents   │     │              │     │  (SQLite)       │
└─────────────────┘     │              │     └─────────────────┘
                        │              │     ┌─────────────────┐
                        │              │────▶│  Cloudflare R2  │
                        │              │     │  (Objects)      │
                        └──────────────┘     └─────────────────┘
                                             ┌─────────────────┐
                                        ────▶│  Cloudflare KV  │
                                             │  (Key-Value)    │
                                             └─────────────────┘
```

## Use Cases

- Static site generators with dynamic content
- Blog platforms with structured metadata
- Documentation sites with search functionality
- Any application needing structured content on Cloudflare's edge
