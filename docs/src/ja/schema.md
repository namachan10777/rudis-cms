# スキーマ定義

スキーマは、コンテンツの構造とデータベースへの保存方法を定義します。

## フィールド定義

スキーマ内の各フィールドには名前と設定があります：

```yaml
schema:
  field_name:
    type: string
    required: true
    index: true
```

## 共通オプション

| オプション | 型 | デフォルト | 説明 |
|-----------|------|---------|------|
| `type` | string | - | フィールド型（必須） |
| `required` | bool | false | フィールドが必須かどうか |
| `index` | bool | false | データベースインデックスを作成 |

## ネストされたレコード

`records`型を使用してネストされたテーブルを定義できます：

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

これにより、外部キー関係を持つ別の`post_comments`テーブルが作成されます。

### inherit_ids

`inherit_ids`オプションは、子テーブルに含める親IDを指定します：

```yaml
comments:
  type: records
  inherit_ids: [post_id]  # post_idを外部キーとして含める
```

深くネストされたレコードの場合、複数のIDを継承できます：

```yaml
replies:
  type: records
  inherit_ids: [post_id, comment_id]
```

## 例

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
