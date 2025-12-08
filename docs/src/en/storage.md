# Storage Options

rudis-cms supports multiple storage backends for different content types.

## R2 (Object Storage)

Cloudflare R2 for binary files like images.

```yaml
storage:
  type: r2
  bucket: my-bucket
  prefix: images/posts
```

| Option | Required | Description |
|--------|----------|-------------|
| `bucket` | Yes | R2 bucket name |
| `prefix` | No | Key prefix for objects |

Objects are stored with content-addressed keys based on their hash, ensuring deduplication.

## KV (Key-Value)

Cloudflare KV for text content like compiled markdown.

```yaml
storage:
  type: kv
  namespace: content-namespace-id
```

| Option | Required | Description |
|--------|----------|-------------|
| `namespace` | Yes | KV namespace ID |

## Inline

Store content directly in the database.

```yaml
storage:
  type: inline
```

Best for small content that doesn't need separate storage.

## Asset

For static assets served directly.

```yaml
storage:
  type: asset
  prefix: static
```

## Storage Pointer Format

In the database, storage references are stored as JSON with pointer information:

```json
{
  "hash": "abc123...",
  "pointer": "r2://bucket/prefix/key",
  "content_type": "image/png",
  "size": 12345
}
```

The pointer format indicates the storage type:
- `r2://bucket/key` - R2 object
- `kv://namespace/key` - KV entry
- `asset://path` - Asset file

## Deduplication

rudis-cms uses content hashing (BLAKE3) to deduplicate uploads:

1. Before upload, the file hash is computed
2. If an object with the same hash exists, upload is skipped
3. Progress shows `⬆️` for new uploads and `⏭️` for skipped

Use the `-f` / `--force` flag to force re-upload of all objects.
