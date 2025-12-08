# Installation

## From Source

rudis-cms is written in Rust. You can build it from source:

```bash
git clone https://github.com/namachan10777/rudis-cms
cd rudis-cms
cargo install --path .
```

## Requirements

- Rust 2024 edition (1.85+)
- Cloudflare account with:
  - D1 database
  - R2 bucket (optional, for object storage)
  - KV namespace (optional, for key-value storage)

## Environment Variables

rudis-cms requires the following environment variables for Cloudflare deployment:

| Variable | Description |
|----------|-------------|
| `CF_ACCOUNT_ID` | Your Cloudflare account ID |
| `CF_API_TOKEN` | API token with D1, R2, and KV permissions |
| `R2_ACCESS_KEY_ID` | R2 access key ID |
| `R2_SECRET_ACCESS_KEY` | R2 secret access key |

### Creating a Cloudflare API Token

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/) > My Profile > API Tokens
2. Create a custom token with the following permissions:
   - Account > D1 > Edit
   - Account > Workers KV Storage > Edit
   - Account > R2 > Edit

### Getting R2 Credentials

1. Go to R2 > Manage R2 API Tokens
2. Create a new API token with read/write access
