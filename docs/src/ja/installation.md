# インストール

## ソースからビルド

rudis-cmsはRustで書かれています。ソースからビルドできます：

```bash
git clone https://github.com/namachan10777/rudis-cms
cd rudis-cms
cargo install --path .
```

## 要件

- Rust 2024 edition (1.85+)
- Cloudflareアカウント（以下が必要）:
  - D1データベース
  - R2バケット（オプション、オブジェクトストレージ用）
  - KV名前空間（オプション、キーバリューストレージ用）

## 環境変数

Cloudflareへのデプロイには以下の環境変数が必要です：

| 変数 | 説明 |
|------|------|
| `CF_ACCOUNT_ID` | CloudflareアカウントID |
| `CF_API_TOKEN` | D1、R2、KVの権限を持つAPIトークン |
| `R2_ACCESS_KEY_ID` | R2アクセスキーID |
| `R2_SECRET_ACCESS_KEY` | R2シークレットアクセスキー |

### Cloudflare APIトークンの作成

1. [Cloudflareダッシュボード](https://dash.cloudflare.com/) > マイプロフィール > APIトークン へ移動
2. 以下の権限でカスタムトークンを作成：
   - Account > D1 > Edit
   - Account > Workers KV Storage > Edit
   - Account > R2 > Edit

### R2認証情報の取得

1. R2 > R2 APIトークンの管理 へ移動
2. 読み書きアクセス権を持つ新しいAPIトークンを作成
