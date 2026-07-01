//! tree-sitter ベースのシンタックスハイライタ。
//!
//! `language` 名(fence のトークン)と source を渡して [`Highlighter`] を作り、
//! [`Highlighter::next_event`] を繰り返し呼ぶことで syntect の `ScopeStack` 風の
//! スタックマシン型イベント列([`Event`])を得る。改行は [`Event::Break`] として単体で出力される。
//!
//! 文法とハイライトクエリは個別の `tree-sitter-*` grammar クレートから供給する
//! (単一の tree-sitter コアバージョンでバンドル)。対応言語は [`Highlighter::new`] のトークン一覧を参照。

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use tree_sitter::Language;
use tree_sitter_highlight::{Highlight, HighlightConfiguration, HighlightEvent};

/// `configure` に渡す認識キャプチャ名。tree-sitter / Helix 系で広く使われる名前を網羅し、
/// grammar ごとに異なるキャプチャ名を接頭辞マッチで拾えるようにする。
/// `Highlight(idx)` はこの配列のインデックスに対応する。
const NAMES: &[&str] = &[
    "attribute",
    "boolean",
    "character",
    "comment",
    "comment.documentation",
    "constant",
    "constant.builtin",
    "constant.numeric",
    "constant.character",
    "constructor",
    "embedded",
    "error",
    "escape",
    "function",
    "function.builtin",
    "function.macro",
    "function.method",
    "keyword",
    "keyword.control",
    "keyword.function",
    "keyword.operator",
    "keyword.return",
    "keyword.directive",
    "label",
    "module",
    "namespace",
    "number",
    "operator",
    "property",
    "punctuation",
    "punctuation.bracket",
    "punctuation.delimiter",
    "punctuation.special",
    "string",
    "string.escape",
    "string.regexp",
    "string.special",
    "tag",
    "tag.builtin",
    "type",
    "type.builtin",
    "variable",
    "variable.builtin",
    "variable.parameter",
    "variable.member",
    "markup",
    "markup.heading",
    "markup.bold",
    "markup.italic",
    "markup.link",
    "markup.list",
    "markup.raw",
];

/// キャプチャ名(ドット表記)を [`Scope`] に変換する。トップレベル分類のみ variant にし、
/// 細分名は完全名を保持した `Other` にフォールバックする(class 出力のフィデリティ維持)。
fn scope_from_name(name: &str) -> Scope {
    match name {
        "attribute" => Scope::Attribute,
        "type" => Scope::Type,
        "constructor" => Scope::Constructor,
        "constant" => Scope::Constant,
        "string" => Scope::String,
        "escape" => Scope::Escape,
        "comment" => Scope::Comment,
        "variable" => Scope::Variable,
        "label" => Scope::Label,
        "punctuation" => Scope::Punctuation,
        "keyword" => Scope::Keyword,
        "operator" => Scope::Operator,
        "function" => Scope::Function,
        "tag" => Scope::Tag,
        "namespace" | "module" => Scope::Namespace,
        "number" => Scope::Number,
        "property" => Scope::Property,
        "boolean" => Scope::Boolean,
        "markup" => Scope::Markup,
        other => Scope::Other(other.to_owned()),
    }
}

/// `Highlight(idx)` を対応する [`Scope`] に変換する。
fn scope_from_index(idx: usize) -> Scope {
    NAMES
        .get(idx)
        .map(|name| scope_from_name(name))
        .unwrap_or_else(|| Scope::Other(String::new()))
}

/// grammar とクエリから [`HighlightConfiguration`] を構築する(`NAMES` で configure 済み)。
fn build(
    language: impl Into<Language>,
    highlights: &str,
    injections: &str,
    locals: &str,
) -> HighlightConfiguration {
    let mut config =
        HighlightConfiguration::new(language.into(), "source", highlights, injections, locals)
            .expect("valid highlight configuration");
    config.configure(NAMES);
    config
}

/// 単純な言語(単一クエリ、base 連結不要)の `LazyLock<HighlightConfiguration>` を定義する。
macro_rules! simple_lang {
    ($name:ident, $lang:expr, $highlights:expr) => {
        static $name: LazyLock<HighlightConfiguration> =
            LazyLock::new(|| build($lang, $highlights, "", ""));
    };
}

simple_lang!(
    RUST,
    tree_sitter_rust::LANGUAGE,
    tree_sitter_rust::HIGHLIGHTS_QUERY
);
simple_lang!(
    PYTHON,
    tree_sitter_python::LANGUAGE,
    tree_sitter_python::HIGHLIGHTS_QUERY
);
simple_lang!(
    GO,
    tree_sitter_go::LANGUAGE,
    tree_sitter_go::HIGHLIGHTS_QUERY
);
simple_lang!(C, tree_sitter_c::LANGUAGE, tree_sitter_c::HIGHLIGHT_QUERY);
simple_lang!(
    JAVA,
    tree_sitter_java::LANGUAGE,
    tree_sitter_java::HIGHLIGHTS_QUERY
);
simple_lang!(
    JSON,
    tree_sitter_json::LANGUAGE,
    tree_sitter_json::HIGHLIGHTS_QUERY
);
simple_lang!(
    CSS,
    tree_sitter_css::LANGUAGE,
    tree_sitter_css::HIGHLIGHTS_QUERY
);
simple_lang!(
    HTML,
    tree_sitter_html::LANGUAGE,
    tree_sitter_html::HIGHLIGHTS_QUERY
);
simple_lang!(
    BASH,
    tree_sitter_bash::LANGUAGE,
    tree_sitter_bash::HIGHLIGHT_QUERY
);
simple_lang!(
    RUBY,
    tree_sitter_ruby::LANGUAGE,
    tree_sitter_ruby::HIGHLIGHTS_QUERY
);
simple_lang!(
    CSHARP,
    tree_sitter_c_sharp::LANGUAGE,
    tree_sitter_c_sharp::HIGHLIGHTS_QUERY
);
simple_lang!(
    TOML,
    tree_sitter_toml_ng::LANGUAGE,
    tree_sitter_toml_ng::HIGHLIGHTS_QUERY
);
simple_lang!(
    YAML,
    tree_sitter_yaml::LANGUAGE,
    tree_sitter_yaml::HIGHLIGHTS_QUERY
);
simple_lang!(
    LUA,
    tree_sitter_lua::LANGUAGE,
    tree_sitter_lua::HIGHLIGHTS_QUERY
);
simple_lang!(
    SCALA,
    tree_sitter_scala::LANGUAGE,
    tree_sitter_scala::HIGHLIGHTS_QUERY
);
simple_lang!(
    HASKELL,
    tree_sitter_haskell::LANGUAGE,
    tree_sitter_haskell::HIGHLIGHTS_QUERY
);
simple_lang!(
    OCAML,
    tree_sitter_ocaml::LANGUAGE_OCAML,
    tree_sitter_ocaml::HIGHLIGHTS_QUERY
);
simple_lang!(
    ELIXIR,
    tree_sitter_elixir::LANGUAGE,
    tree_sitter_elixir::HIGHLIGHTS_QUERY
);
simple_lang!(
    REGEX,
    tree_sitter_regex::LANGUAGE,
    tree_sitter_regex::HIGHLIGHTS_QUERY
);
simple_lang!(
    MARKDOWN,
    tree_sitter_md::LANGUAGE,
    tree_sitter_md::HIGHLIGHT_QUERY_BLOCK
);
simple_lang!(
    PHP,
    tree_sitter_php::LANGUAGE_PHP,
    tree_sitter_php::HIGHLIGHTS_QUERY
);
simple_lang!(
    SQL,
    tree_sitter_sequel::LANGUAGE,
    tree_sitter_sequel::HIGHLIGHTS_QUERY
);
simple_lang!(
    XML,
    tree_sitter_xml::LANGUAGE_XML,
    tree_sitter_xml::XML_HIGHLIGHT_QUERY
);
simple_lang!(
    NIX,
    tree_sitter_nix::LANGUAGE,
    tree_sitter_nix::HIGHLIGHTS_QUERY
);
simple_lang!(
    ZIG,
    tree_sitter_zig::LANGUAGE,
    tree_sitter_zig::HIGHLIGHTS_QUERY
);
simple_lang!(
    SVELTE,
    tree_sitter_svelte_ng::LANGUAGE,
    tree_sitter_svelte_ng::HIGHLIGHTS_QUERY
);
simple_lang!(
    SWIFT,
    tree_sitter_swift::LANGUAGE,
    tree_sitter_swift::HIGHLIGHTS_QUERY
);

// C++ は C のハイライトクエリを前置する。
static CPP: LazyLock<HighlightConfiguration> = LazyLock::new(|| {
    let highlights = format!(
        "{}\n{}",
        tree_sitter_c::HIGHLIGHT_QUERY,
        tree_sitter_cpp::HIGHLIGHT_QUERY
    );
    build(tree_sitter_cpp::LANGUAGE, &highlights, "", "")
});

// TypeScript / TSX は JavaScript のクエリを base として連結する(TSX はさらに JSX 分)。
static TYPESCRIPT: LazyLock<HighlightConfiguration> = LazyLock::new(|| {
    let highlights = format!(
        "{}\n{}",
        tree_sitter_javascript::HIGHLIGHT_QUERY,
        tree_sitter_typescript::HIGHLIGHTS_QUERY
    );
    let locals = format!(
        "{}\n{}",
        tree_sitter_javascript::LOCALS_QUERY,
        tree_sitter_typescript::LOCALS_QUERY
    );
    build(
        tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
        &highlights,
        "",
        &locals,
    )
});

static TSX: LazyLock<HighlightConfiguration> = LazyLock::new(|| {
    let highlights = format!(
        "{}\n{}\n{}",
        tree_sitter_javascript::HIGHLIGHT_QUERY,
        tree_sitter_javascript::JSX_HIGHLIGHT_QUERY,
        tree_sitter_typescript::HIGHLIGHTS_QUERY
    );
    let locals = format!(
        "{}\n{}",
        tree_sitter_javascript::LOCALS_QUERY,
        tree_sitter_typescript::LOCALS_QUERY
    );
    build(
        tree_sitter_typescript::LANGUAGE_TSX,
        &highlights,
        "",
        &locals,
    )
});

static JAVASCRIPT: LazyLock<HighlightConfiguration> = LazyLock::new(|| {
    let highlights = format!(
        "{}\n{}",
        tree_sitter_javascript::HIGHLIGHT_QUERY,
        tree_sitter_javascript::JSX_HIGHLIGHT_QUERY
    );
    build(
        tree_sitter_javascript::LANGUAGE,
        &highlights,
        "",
        tree_sitter_javascript::LOCALS_QUERY,
    )
});

/// トークン(fence の言語名/エイリアス)から `HighlightConfiguration` を引く。
fn config_for(token: &str) -> Option<&'static HighlightConfiguration> {
    let config: &'static LazyLock<HighlightConfiguration> =
        match token.to_ascii_lowercase().as_str() {
            "rust" | "rs" => &RUST,
            "python" | "py" => &PYTHON,
            "go" | "golang" => &GO,
            "c" | "h" => &C,
            "cpp" | "c++" | "cc" | "cxx" | "hpp" => &CPP,
            "java" => &JAVA,
            "json" | "jsonc" => &JSON,
            "css" => &CSS,
            "html" | "htm" => &HTML,
            "bash" | "sh" | "shell" | "zsh" => &BASH,
            "ruby" | "rb" => &RUBY,
            "csharp" | "c#" | "cs" => &CSHARP,
            "toml" => &TOML,
            "yaml" | "yml" => &YAML,
            "lua" => &LUA,
            "scala" | "sbt" => &SCALA,
            "haskell" | "hs" => &HASKELL,
            "ocaml" | "ml" => &OCAML,
            "elixir" | "ex" | "exs" => &ELIXIR,
            "regex" => &REGEX,
            "markdown" | "md" => &MARKDOWN,
            "php" => &PHP,
            "sql" => &SQL,
            "xml" => &XML,
            "nix" => &NIX,
            "zig" => &ZIG,
            "svelte" => &SVELTE,
            "swift" => &SWIFT,
            "javascript" | "js" | "jsx" | "mjs" | "cjs" => &JAVASCRIPT,
            "typescript" | "ts" => &TYPESCRIPT,
            "tsx" | "typescriptreact" => &TSX,
            _ => return None,
        };
    Some(&**config)
}

/// ハイライタの生成・実行時に起きうるエラー。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 未対応の language トークンが渡された。
    #[error("unknown language: {0}")]
    UnknownLanguage(String),
    /// tree-sitter-highlight の実行に失敗した。
    #[error(transparent)]
    Highlight(#[from] tree_sitter_highlight::Error),
}

/// `Clear` op で巻き戻すスタックの量。syntect の `ClearAmount` 相当。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClearAmount {
    /// 上位 N 要素をクリアする。
    TopN(usize),
    /// スタック全体をクリアする。
    All,
}

/// ハイライトのクラス。tree-sitter キャプチャ名のトップレベル分類に対応する。
///
/// 細分名(`keyword.control.conditional` 等)は `Other` に完全名を保持する。
/// `Other(String)` を持つため `Copy` は付けない。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    Attribute,
    Type,
    Constructor,
    Constant,
    String,
    Escape,
    Comment,
    Variable,
    Label,
    Punctuation,
    Keyword,
    Operator,
    Function,
    Tag,
    Namespace,
    Number,
    Property,
    Boolean,
    Markup,
    /// 上記以外・細分名のスコープ(完全名を保持)。
    Other(String),
}

impl Scope {
    /// キャプチャ名(ドット表記、例: `"keyword.control"`)を返す。
    /// `Other(s)` は `s` をそのまま返す。
    pub fn name(&self) -> &str {
        match self {
            Scope::Attribute => "attribute",
            Scope::Type => "type",
            Scope::Constructor => "constructor",
            Scope::Constant => "constant",
            Scope::String => "string",
            Scope::Escape => "escape",
            Scope::Comment => "comment",
            Scope::Variable => "variable",
            Scope::Label => "label",
            Scope::Punctuation => "punctuation",
            Scope::Keyword => "keyword",
            Scope::Operator => "operator",
            Scope::Function => "function",
            Scope::Tag => "tag",
            Scope::Namespace => "namespace",
            Scope::Number => "number",
            Scope::Property => "property",
            Scope::Boolean => "boolean",
            Scope::Markup => "markup",
            Scope::Other(s) => s,
        }
    }
}

/// スタックマシンのイベント。syntect の `ScopeStackOp` 語彙に `Text` / `Break` を統合したもの。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event<'a> {
    /// スコープをスタックに積む。
    Push(Scope),
    /// スタックから n 要素取り除く(tree-sitter バックエンドでは常に 1)。
    Pop(usize),
    /// スタックを指定量クリアする(現バックエンドでは未生成・将来拡張用)。
    Clear(ClearAmount),
    /// `Clear` で退避した状態を復元する(現バックエンドでは未生成・将来拡張用)。
    Restore,
    /// source からそのまま切り出した葉テキスト(改行を含まない)。
    Text(&'a str),
    /// 改行。行境界ごとに単体で出力される。
    Break,
    /// 何もしない。
    Noop,
}

/// language トークンと source から生成する、pull 型のスタックマシン型ハイライタ。
pub struct Highlighter<'a> {
    events: Vec<Event<'a>>,
    cursor: usize,
}

impl<'a> Highlighter<'a> {
    /// `language`(fence のトークン)と `source` からハイライタを生成し、全イベントを事前に構築する。
    /// 未対応トークンは [`Error::UnknownLanguage`]。
    pub fn new(language: &str, source: &'a str) -> Result<Self, Error> {
        let config =
            config_for(language).ok_or_else(|| Error::UnknownLanguage(language.to_owned()))?;

        let mut highlighter = tree_sitter_highlight::Highlighter::new();
        let mut events = Vec::new();
        // highlight() が返すイテレータは highlighter/config/source を借用する自己参照になるため、
        // ここで全消費して Vec<Event<'a>> に落とす。Text は外部 source(&'a str)の
        // バイトオフセット slice なので、生成後に core 側オブジェクトを破棄しても安全。
        for event in highlighter.highlight(config, source.as_bytes(), None, |_| None)? {
            match event? {
                HighlightEvent::HighlightStart(Highlight(idx)) => {
                    events.push(Event::Push(scope_from_index(idx)));
                }
                HighlightEvent::HighlightEnd => {
                    events.push(Event::Pop(1));
                }
                HighlightEvent::Source { start, end } => {
                    push_source(&mut events, &source[start..end]);
                }
            }
        }

        Ok(Highlighter { events, cursor: 0 })
    }

    /// 次のイベントを 1 つ返す。末尾に達したら `None`。
    pub fn next_event(&mut self) -> Option<Event<'a>> {
        let event = self.events.get(self.cursor).cloned();
        if event.is_some() {
            self.cursor += 1;
        }
        event
    }
}

/// source の一部を `'\n'` で分割し、非空セグメントを `Text`、境界を `Break` として積む。
fn push_source<'a>(events: &mut Vec<Event<'a>>, text: &'a str) {
    let mut first = true;
    for segment in text.split('\n') {
        if !first {
            events.push(Event::Break);
        }
        first = false;
        if !segment.is_empty() {
            events.push(Event::Text(segment));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect<'a>(language: &str, source: &'a str) -> Vec<Event<'a>> {
        let mut hl = Highlighter::new(language, source).expect("highlighter");
        let mut events = Vec::new();
        while let Some(e) = hl.next_event() {
            events.push(e);
        }
        events
    }

    fn has_scope_prefix(events: &[Event<'_>], prefix: &str) -> bool {
        events.iter().any(|e| match e {
            Event::Push(scope) => scope.name().starts_with(prefix),
            _ => false,
        })
    }

    #[test]
    fn typescript_is_fully_highlighted() {
        // inkjet の TS では拾えなかった JS レベルの keyword / comment / string も拾えること。
        let events = collect("typescript", "// hi\nconst s = \"x\";\n");
        assert!(has_scope_prefix(&events, "keyword"), "keyword: {events:?}");
        assert!(has_scope_prefix(&events, "comment"), "comment: {events:?}");
        assert!(has_scope_prefix(&events, "string"), "string: {events:?}");
    }

    #[test]
    fn tsx_emits_tag_or_attribute() {
        let events = collect("tsx", "const a = <div className=\"a\">x</div>;\n");
        assert!(
            has_scope_prefix(&events, "tag") || has_scope_prefix(&events, "attribute"),
            "{events:?}"
        );
    }

    #[test]
    fn many_languages_are_highlighted() {
        for (lang, src) in [
            ("rust", "fn f() {}"),
            ("python", "def f():\n    pass\n"),
            ("go", "package main"),
            ("c", "int main() { return 0; }"),
            ("cpp", "int main() { return 0; }"),
            ("java", "class A {}"),
            ("bash", "echo hi"),
            ("ruby", "def f; end"),
            ("json", "{\"a\": 1}"),
            ("css", "a { color: red; }"),
        ] {
            let events = collect(lang, src);
            assert!(
                events.iter().any(|e| matches!(e, Event::Push(_))),
                "{lang} should be highlighted: {events:?}"
            );
        }
    }

    #[test]
    fn all_registered_languages_build() {
        // 各言語の HighlightConfiguration(LazyLock)を強制構築し、クエリが
        // tree-sitter 0.26 でコンパイルできることを確認する(パニックしないこと)。
        for token in [
            "rust",
            "python",
            "go",
            "c",
            "cpp",
            "java",
            "json",
            "css",
            "html",
            "bash",
            "ruby",
            "csharp",
            "toml",
            "yaml",
            "lua",
            "scala",
            "haskell",
            "ocaml",
            "elixir",
            "regex",
            "markdown",
            "javascript",
            "typescript",
            "tsx",
            "php",
            "sql",
            "xml",
            "nix",
            "zig",
            "svelte",
            "swift",
        ] {
            assert!(
                Highlighter::new(token, "a\n").is_ok(),
                "{token} should build"
            );
        }
    }

    #[test]
    fn text_and_breaks_reconstruct_source() {
        let source = "fn a() {}\nfn b() {}\n";
        let events = collect("rust", source);
        let mut reconstructed = String::new();
        for e in &events {
            match e {
                Event::Text(t) => reconstructed.push_str(t),
                Event::Break => reconstructed.push('\n'),
                _ => {}
            }
        }
        assert_eq!(reconstructed, source);
    }

    #[test]
    fn unknown_language_errors() {
        let result = Highlighter::new("definitely-not-a-lang", "x");
        assert!(matches!(result, Err(Error::UnknownLanguage(l)) if l == "definitely-not-a-lang"));
    }

    #[test]
    fn break_count_matches_line_boundaries() {
        let events = collect("rust", "a\nb\nc\n");
        let breaks = events.iter().filter(|e| matches!(e, Event::Break)).count();
        assert_eq!(breaks, 3, "{events:?}");
    }

    #[test]
    fn scope_name_round_trips() {
        assert_eq!(scope_from_name("keyword").name(), "keyword");
        assert_eq!(
            scope_from_name("keyword.control.conditional").name(),
            "keyword.control.conditional"
        );
    }
}
