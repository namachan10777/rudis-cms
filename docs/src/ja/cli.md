# CLIコマンド

## グローバルオプション

```bash
rudis-cms --config <CONFIG> <COMMAND>
```

| オプション | 短縮形 | 説明 |
|-----------|-------|------|
| `--config` | `-c` | 設定ファイルのパス（必須） |

## コマンド

### batch

Cloudflareにコンテンツをデプロイ。

```bash
rudis-cms -c config.yaml batch [OPTIONS]
```

| オプション | 短縮形 | 説明 |
|-----------|-------|------|
| `--force` | `-f` | すべてのオブジェクトを強制的に再アップロード |
| `--preview` | `-p` | プレビューデータベースにデプロイ |

例：
```bash
# 通常のデプロイ
rudis-cms -c config.yaml batch

# すべてを強制的に再アップロード
rudis-cms -c config.yaml batch -f

# プレビューデータベースにデプロイ
rudis-cms -c config.yaml batch --preview
```

### dump

開発/テスト用にローカルファイルにエクスポート。

```bash
rudis-cms -c config.yaml dump --storage <PATH> --db <PATH>
```

| オプション | 説明 |
|-----------|------|
| `--storage` | ストレージファイルのディレクトリ |
| `--db` | SQLiteデータベースのディレクトリ |

例：
```bash
rudis-cms -c config.yaml dump --storage ./local-storage --db ./local-db
```

### show-schema

生成されたスキーマを表示。

```bash
rudis-cms -c config.yaml show-schema <SUBCOMMAND>
```

#### show-schema sql

SQL DDL文を表示。

```bash
rudis-cms -c config.yaml show-schema sql [OPTIONS]
```

| オプション | 説明 |
|-----------|------|
| `--fetch-objects` | オブジェクト取得クエリを含める |

#### show-schema typescript

TypeScript型定義を生成。

```bash
rudis-cms -c config.yaml show-schema typescript [OPTIONS]
```

| オプション | 説明 |
|-----------|------|
| `--save <DIR>` | ディレクトリに保存 |
| `--valibot` | Valibotスキーマを生成 |

例：
```bash
# 標準出力に表示
rudis-cms -c config.yaml show-schema typescript

# Valibotバリデーション付きで保存
rudis-cms -c config.yaml show-schema typescript --save ./generated --valibot
```

## 終了コード

| コード | 説明 |
|-------|------|
| 0 | 成功 |
| 1 | エラー |

## 環境変数

必要な環境変数については[インストール](./installation.md)を参照してください。
