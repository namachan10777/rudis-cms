# SQLスキーマ

rudis-cmsはスキーマ定義に基づいてSQLテーブルを自動生成します。

## 生成されるテーブル

以下のようなスキーマの場合：

```yaml
table: posts
schema:
  id:
    type: id
  title:
    type: string
    required: true
  created_at:
    type: created_at
  updated_at:
    type: updated_at
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

以下のSQLが生成されます：

```sql
CREATE TABLE IF NOT EXISTS posts (
  id TEXT NOT NULL,
  title TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
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

## 型マッピング

| rudis-cms型 | SQLite型 | 備考 |
|-------------|----------|------|
| `id` | `TEXT NOT NULL` | 主キー |
| `string` | `TEXT` | requiredの場合`NOT NULL` |
| `boolean` | `INTEGER` | 0または1 |
| `date` | `TEXT` | ISO 8601形式 |
| `datetime` | `TEXT` | ISO 8601形式 |
| `hash` | `TEXT` | BLAKE3ハッシュ |
| `created_at` | `TEXT NOT NULL` | INSERT時に自動設定 |
| `updated_at` | `TEXT NOT NULL` | INSERT時と実際の内容変更時に自動設定 |
| `markdown` | `TEXT` | ストレージポインター付きJSON |
| `image` | `TEXT` | ストレージポインター付きJSON |
| `file` | `TEXT` | ストレージポインター付きJSON |

タイムスタンプ列名にはスキーマのキーが使われるため、`created_at`や`updated_at`という名前に固定されません。既存テーブルへタイムスタンプ型を追加する場合は、次回同期前に外部migrationで列追加と既存行の補完を行ってください。生成される`CREATE TABLE IF NOT EXISTS`は既存テーブルを変更しません。

## ストレージポインター

コンテンツフィールド（markdown、image、file）はJSONとして保存されます：

```json
{
  "hash": "abc123def456...",
  "size": 12345,
  "content_type": "text/markdown",
  "pointer": "kv://namespace-id/key"
}
```

## 外部キー

`records`型で作成される子テーブルには以下が含まれます：
- `ON DELETE CASCADE`付きの外部キー制約
- 親IDを含む複合主キー

## 生成されたSQLの確認

CLIを使用して生成されたSQLを確認できます：

```bash
rudis-cms -c config.yaml show-schema sql
```
