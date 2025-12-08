# 設定

rudis-cmsはYAML設定ファイルを使用してコンテンツ構造を定義します。

## 基本構造

```yaml
glob: "posts/**/*.md"      # マッチするファイルパターン
name: posts                 # コレクション名
table: posts               # メインテーブル名
database_id: xxx-xxx       # Cloudflare D1データベースID
syntax:
  type: markdown           # または "yaml"
  column: body             # Markdownコンテンツのカラム（markdown専用）
schema:
  # フィールド定義...
```

## トップレベルオプション

| オプション | 必須 | 説明 |
|-----------|------|------|
| `glob` | はい | コンテンツファイルのglobパターン |
| `name` | はい | コレクション名 |
| `table` | はい | メインデータベーステーブル名 |
| `database_id` | はい | Cloudflare D1データベースID |
| `preview_database_id` | いいえ | プレビュー用の別D1データベース |
| `syntax` | はい | コンテンツフォーマット設定 |
| `schema` | はい | フィールド定義 |

## シンタックスオプション

### Markdown

```yaml
syntax:
  type: markdown
  column: body    # Markdownコンテンツのフィールド名
```

Markdownシンタックスでは、フロントマターフィールドがスキーマフィールドにマッピングされ、本文は指定されたカラムに保存されます。

### YAML

```yaml
syntax:
  type: yaml
```

YAMLシンタックスでは、ファイル全体がYAMLとして解析され、スキーマフィールドにマッピングされます。

## プレビューデータベース

プレビュー/下書きコンテンツ用に別のデータベースを指定できます：

```yaml
database_id: production-db-id
preview_database_id: preview-db-id
```

`--preview`フラグを使用すると、本番ではなくプレビューデータベースにデプロイします。
