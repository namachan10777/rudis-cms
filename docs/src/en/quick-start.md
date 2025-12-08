# Quick Start

This guide will walk you through setting up a simple blog with rudis-cms.

## 1. Create Configuration

Create a `config.yaml` file:

```yaml
glob: "posts/**/*.md"
name: posts
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
  date:
    type: date
    index: true
    required: true
  body:
    type: markdown
    required: true
    storage:
      type: kv
      namespace: your-kv-namespace-id
    image:
      table: post_images
      inherit_ids: [post_id]
      storage:
        type: r2
        bucket: your-bucket-name
        prefix: images
```

## 2. Create Content

Create a Markdown file at `posts/hello-world.md`:

```markdown
---
id: hello-world
title: Hello World
date: 2024-01-01
---

This is my first post!

![Sample image](./image.png)
```

## 3. Deploy

Set your environment variables and run:

```bash
export CF_ACCOUNT_ID=your-account-id
export CF_API_TOKEN=your-api-token
export R2_ACCESS_KEY_ID=your-r2-key
export R2_SECRET_ACCESS_KEY=your-r2-secret

rudis-cms --config config.yaml batch
```

## 4. View Progress

rudis-cms shows a progress display during deployment:

```
📋 Loading configuration...
🔧 Compiling schema...
📄 Processing documents...
⬆️ Uploading to storage...
✅ Completed!

📊 Results:
├── ✅ posts/hello-world.md
│   ├── ⬆️ kv://namespace/hello-world
│   └── ⬆️ r2://bucket/images/image.png

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   📄 Entries:    1 total
   ✅ Successful: 1
   ⬆️ Uploads:    2
   ⏱️ Duration:   1.23s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Local Development

For local testing without Cloudflare, use dump mode:

```bash
rudis-cms --config config.yaml dump --storage ./storage --db ./db
```

This creates local SQLite databases and stores files on disk.
