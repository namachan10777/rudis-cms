# フィールド型

rudis-cmsは様々なデータニーズに対応するフィールド型をサポートしています。

## 基本型

### id

レコードの主キー識別子。

```yaml
id:
  type: id
```

- SQLiteでは`TEXT`として保存
- 自動的にインデックス化
- テーブル内で一意である必要がある

### string

テキストコンテンツ。

```yaml
title:
  type: string
  required: true
```

- SQLiteでは`TEXT`として保存

### boolean

真偽値。

```yaml
published:
  type: boolean
  index: true
```

- SQLiteでは`INTEGER`（0/1）として保存

### date

時刻なしの日付。

```yaml
published_date:
  type: date
  index: true
```

- ISO 8601形式（YYYY-MM-DD）で`TEXT`として保存
- `date()`関数を使用してインデックス化

### datetime

時刻付きの日付。

```yaml
created_at:
  type: datetime
  index: true
```

- ISO 8601形式で`TEXT`として保存
- `datetime()`関数を使用してインデックス化

### hash

変更検出用のコンテンツハッシュ。

```yaml
hash:
  type: hash
```

- ファイル内容から自動計算
- キャッシュ無効化に有用

## コンテンツ型

### markdown

オプションの画像抽出機能付きMarkdownコンテンツ。

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

オプション：
- `storage`: コンパイル済みMarkdownの保存先
- `image`: 抽出された画像の設定
- `image.embed_svg_threshold`: これより小さい（バイト）SVGファイルはインライン埋め込み
- `config`: 追加のMarkdown処理オプション

### image

単一画像フィールド。

```yaml
og_image:
  type: image
  storage:
    type: r2
    bucket: my-bucket
    prefix: og-images
```

### file

汎用ファイル添付。

```yaml
attachment:
  type: file
  storage:
    type: r2
    bucket: my-bucket
    prefix: attachments
```

## リレーショナル型

### records

複数レコードを持つネストされたテーブル。

```yaml
tags:
  type: records
  table: post_tags
  inherit_ids: [post_id]
  schema:
    tag:
      type: id
```

外部キー関係を持つ別テーブルを作成します。
