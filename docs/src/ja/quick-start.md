# クイックスタート

このガイドでは、rudis-cmsでシンプルなブログをセットアップする方法を説明します。

## 1. 設定ファイルの作成

`config.yaml`ファイルを作成します：

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

## 2. コンテンツの作成

`posts/hello-world.md`にMarkdownファイルを作成します：

```markdown
---
id: hello-world
title: Hello World
date: 2024-01-01
---

これは最初の投稿です！

![サンプル画像](./image.png)
```

## 3. デプロイ

環境変数を設定して実行します：

```bash
export CF_ACCOUNT_ID=your-account-id
export CF_API_TOKEN=your-api-token
export R2_ACCESS_KEY_ID=your-r2-key
export R2_SECRET_ACCESS_KEY=your-r2-secret

rudis-cms --config config.yaml batch
```

## 4. 進捗表示

rudis-cmsはデプロイ中に進捗を表示します：

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

## ローカル開発

Cloudflareを使わずにローカルでテストするには、dumpモードを使用します：

```bash
rudis-cms --config config.yaml dump --storage ./storage --db ./db
```

これにより、ローカルのSQLiteデータベースが作成され、ファイルはディスクに保存されます。
