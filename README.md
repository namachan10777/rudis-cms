# rudis-cms

A headless CMS that compiles Markdown/YAML documents into Cloudflare D1, R2, and KV.

[![Documentation](https://img.shields.io/badge/docs-mdBook-blue)](https://namachan10777.github.io/rudis-cms/)

## Features

- **Schema-driven**: Define your content structure in YAML, automatically generates SQL tables
- **Markdown with embedded images**: Images are extracted, optimized, and uploaded to R2
- **Multiple storage backends**: R2 (objects), KV (key-value), and inline storage
- **Incremental updates**: Only upload changed content, skip unchanged files
- **Local development**: Dump mode for testing with local SQLite

## Installation

```bash
git clone https://github.com/namachan10777/rudis-cms
cd rudis-cms
cargo install --path .
```

## Quick Start

1. Create a `config.yaml`:

```yaml
glob: "posts/**/*.md"
table: posts
database_id: your-d1-database-id
syntax:
  type: markdown
  column: body
schema:
  id:
    type: id
  title:
    type: string
    required: true
  body:
    type: markdown
    storage:
      type: kv
      namespace: your-kv-namespace
```

2. Deploy:

```bash
rudis-cms -c config.yaml batch
```

## Syntax Highlighting

Fenced code blocks are highlighted with tree-sitter (via the bundled
`treesitteract` crate). Write one of the tokens below as the code fence info
string (e.g. ` ```rust `). Tokens are **case-insensitive**. Unknown or omitted
languages fall back to plain (escaped) text.

| Language   | Fence tokens                    |
| ---------- | ------------------------------- |
| Rust       | `rust`, `rs`                    |
| Python     | `python`, `py`                  |
| Go         | `go`, `golang`                  |
| C          | `c`, `h`                        |
| C++        | `cpp`, `c++`, `cc`, `cxx`, `hpp`|
| Java       | `java`                          |
| JSON       | `json`, `jsonc`                 |
| CSS        | `css`                           |
| HTML       | `html`, `htm`                   |
| Shell/Bash | `bash`, `sh`, `shell`, `zsh`    |
| Ruby       | `ruby`, `rb`                    |
| C#         | `csharp`, `c#`, `cs`            |
| TOML       | `toml`                          |
| YAML       | `yaml`, `yml`                   |
| Lua        | `lua`                           |
| Scala      | `scala`, `sbt`                  |
| Haskell    | `haskell`, `hs`                 |
| OCaml      | `ocaml`, `ml`                   |
| Elixir     | `elixir`, `ex`, `exs`           |
| Regex      | `regex`                         |
| Markdown   | `markdown`, `md`                |
| JavaScript | `javascript`, `js`, `jsx`, `mjs`, `cjs` |
| TypeScript | `typescript`, `ts`              |
| TSX        | `tsx`, `typescriptreact`        |
| PHP        | `php`                           |
| SQL        | `sql`                           |
| XML        | `xml`                           |
| Nix        | `nix`                           |
| Zig        | `zig`                           |
| Svelte     | `svelte`                        |
| Swift      | `swift`                         |

## Documentation

- [English](https://namachan10777.github.io/rudis-cms/en/)
- [日本語](https://namachan10777.github.io/rudis-cms/ja/)

## License

MIT
