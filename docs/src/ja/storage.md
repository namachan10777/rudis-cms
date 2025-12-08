# ストレージオプション

rudis-cmsは異なるコンテンツタイプに対応する複数のストレージバックエンドをサポートしています。

## R2（オブジェクトストレージ）

画像などのバイナリファイル用のCloudflare R2。

```yaml
storage:
  type: r2
  bucket: my-bucket
  prefix: images/posts
```

| オプション | 必須 | 説明 |
|-----------|------|------|
| `bucket` | はい | R2バケット名 |
| `prefix` | いいえ | オブジェクトのキープレフィックス |

オブジェクトはハッシュに基づくコンテンツアドレスキーで保存され、重複排除が保証されます。

## KV（キーバリュー）

コンパイル済みMarkdownなどのテキストコンテンツ用のCloudflare KV。

```yaml
storage:
  type: kv
  namespace: content-namespace-id
```

| オプション | 必須 | 説明 |
|-----------|------|------|
| `namespace` | はい | KV名前空間ID |

## Inline

コンテンツをデータベースに直接保存。

```yaml
storage:
  type: inline
```

別途ストレージが不要な小さなコンテンツに最適。

## Asset

直接配信される静的アセット用。

```yaml
storage:
  type: asset
  prefix: static
```

## ストレージポインター形式

データベースでは、ストレージ参照はポインター情報を含むJSONとして保存されます：

```json
{
  "hash": "abc123...",
  "pointer": "r2://bucket/prefix/key",
  "content_type": "image/png",
  "size": 12345
}
```

ポインター形式はストレージタイプを示します：
- `r2://bucket/key` - R2オブジェクト
- `kv://namespace/key` - KVエントリ
- `asset://path` - アセットファイル

## 重複排除

rudis-cmsはコンテンツハッシュ（BLAKE3）を使用してアップロードを重複排除します：

1. アップロード前にファイルハッシュを計算
2. 同じハッシュのオブジェクトが存在する場合、アップロードをスキップ
3. 進捗表示では新規アップロードは`⬆️`、スキップは`⏭️`

すべてのオブジェクトを強制的に再アップロードするには`-f` / `--force`フラグを使用します。
