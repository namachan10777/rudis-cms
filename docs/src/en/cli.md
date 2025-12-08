# CLI Commands

## Global Options

```bash
rudis-cms --config <CONFIG> <COMMAND>
```

| Option | Short | Description |
|--------|-------|-------------|
| `--config` | `-c` | Path to configuration file (required) |

## Commands

### batch

Deploy content to Cloudflare.

```bash
rudis-cms -c config.yaml batch [OPTIONS]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--force` | `-f` | Force re-upload all objects |
| `--preview` | `-p` | Deploy to preview database |

Example:
```bash
# Normal deployment
rudis-cms -c config.yaml batch

# Force re-upload everything
rudis-cms -c config.yaml batch -f

# Deploy to preview database
rudis-cms -c config.yaml batch --preview
```

### dump

Export to local files for development/testing.

```bash
rudis-cms -c config.yaml dump --storage <PATH> --db <PATH>
```

| Option | Description |
|--------|-------------|
| `--storage` | Directory for storage files |
| `--db` | Directory for SQLite database |

Example:
```bash
rudis-cms -c config.yaml dump --storage ./local-storage --db ./local-db
```

### show-schema

Display generated schemas.

```bash
rudis-cms -c config.yaml show-schema <SUBCOMMAND>
```

#### show-schema sql

Show SQL DDL statements.

```bash
rudis-cms -c config.yaml show-schema sql [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--fetch-objects` | Include fetch objects query |

#### show-schema typescript

Generate TypeScript type definitions.

```bash
rudis-cms -c config.yaml show-schema typescript [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--save <DIR>` | Save to directory |
| `--valibot` | Generate Valibot schemas |

Example:
```bash
# Print to stdout
rudis-cms -c config.yaml show-schema typescript

# Save with Valibot validation
rudis-cms -c config.yaml show-schema typescript --save ./generated --valibot
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Error |

## Environment Variables

See [Installation](./installation.md) for required environment variables.
