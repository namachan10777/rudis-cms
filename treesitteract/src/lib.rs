//! tree-sitter ベースのシンタックスハイライタ。
//!
//! `language` 名と source を渡して [`Highlighter`] を作り、[`Highlighter::next_event`]
//! を繰り返し呼ぶことで syntect の `ScopeStack` 風のスタックマシン型イベント列
//! ([`Event`]) を得る。改行は [`Event::Break`] として単体で出力される。

use serde::{Deserialize, Serialize};
use tree_sitter_highlight::{Highlight, HighlightConfiguration, HighlightEvent};

/// tree-sitter 標準のキャプチャ名。`Scope` の非 `Other` 変種と同順で並べる。
/// この配列のインデックスが `Highlight(idx)` に対応する。
const NAMES: &[&str] = &[
    "attribute",
    "comment",
    "constant",
    "constant.builtin",
    "constructor",
    "embedded",
    "function",
    "function.builtin",
    "keyword",
    "module",
    "number",
    "operator",
    "property",
    "punctuation",
    "punctuation.bracket",
    "punctuation.delimiter",
    "punctuation.special",
    "string",
    "string.special",
    "tag",
    "type",
    "type.builtin",
    "variable",
    "variable.builtin",
    "variable.parameter",
];

/// `Highlight(idx)` を対応する [`Scope`] に変換する。`NAMES` と同順。
fn scope_from_index(idx: usize) -> Scope {
    match idx {
        0 => Scope::Attribute,
        1 => Scope::Comment,
        2 => Scope::Constant,
        3 => Scope::ConstantBuiltin,
        4 => Scope::Constructor,
        5 => Scope::Embedded,
        6 => Scope::Function,
        7 => Scope::FunctionBuiltin,
        8 => Scope::Keyword,
        9 => Scope::Module,
        10 => Scope::Number,
        11 => Scope::Operator,
        12 => Scope::Property,
        13 => Scope::Punctuation,
        14 => Scope::PunctuationBracket,
        15 => Scope::PunctuationDelimiter,
        16 => Scope::PunctuationSpecial,
        17 => Scope::String,
        18 => Scope::StringSpecial,
        19 => Scope::Tag,
        20 => Scope::Type,
        21 => Scope::TypeBuiltin,
        22 => Scope::Variable,
        23 => Scope::VariableBuiltin,
        24 => Scope::VariableParameter,
        // NAMES に登録した名前しか Highlight にならないため通常到達しない。
        _ => Scope::Other(NAMES.get(idx).map(|s| (*s).to_owned()).unwrap_or_default()),
    }
}

/// ハイライタの生成・実行時に起きうるエラー。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 未対応の language 名が渡された。
    #[error("unknown language: {0}")]
    UnknownLanguage(String),
    /// highlights / locals クエリのコンパイルに失敗した。
    #[error(transparent)]
    Query(#[from] tree_sitter::QueryError),
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

/// ハイライトのクラス。tree-sitter 標準キャプチャ名に対応する。
///
/// `Other` は将来拡張・未知キャプチャ用の予約であり、現在の tree-sitter-highlight
/// バックエンドでは `NAMES` に登録した名前のみが返るため通常は生成されない。
// `Other(String)` を持つため `Copy` は付けられない(スケッチの `Copy` は削除)。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    Attribute,
    Comment,
    Constant,
    ConstantBuiltin,
    Constructor,
    Embedded,
    Function,
    FunctionBuiltin,
    Keyword,
    Module,
    Number,
    Operator,
    Property,
    Punctuation,
    PunctuationBracket,
    PunctuationDelimiter,
    PunctuationSpecial,
    String,
    StringSpecial,
    Tag,
    Type,
    TypeBuiltin,
    Variable,
    VariableBuiltin,
    VariableParameter,
    /// 未知・将来拡張用のスコープ(通常未使用)。
    Other(String),
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

/// language 名と source から生成する、pull 型のスタックマシン型ハイライタ。
pub struct Highlighter<'a> {
    events: Vec<Event<'a>>,
    cursor: usize,
}

impl<'a> Highlighter<'a> {
    /// `language`(`"typescript"`/`"ts"`/`"tsx"`/`"typescriptreact"`)と `source` から
    /// ハイライタを生成し、全イベントを事前に構築する。
    pub fn new(language: &str, source: &'a str) -> Result<Self, Error> {
        // tree-sitter-typescript の highlights/locals クエリは JavaScript grammar の
        // クエリを前置する前提で TS 固有分しか持たない(`const` 等は JS 側)。
        // そのため JS のクエリを base として連結する(TSX はさらに JSX 分を足す)。
        let (ts_language, highlights, locals) = match language {
            "typescript" | "ts" => (
                tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
                format!(
                    "{}\n{}",
                    tree_sitter_javascript::HIGHLIGHT_QUERY,
                    tree_sitter_typescript::HIGHLIGHTS_QUERY,
                ),
                format!(
                    "{}\n{}",
                    tree_sitter_javascript::LOCALS_QUERY,
                    tree_sitter_typescript::LOCALS_QUERY,
                ),
            ),
            "tsx" | "typescriptreact" => (
                tree_sitter_typescript::LANGUAGE_TSX,
                format!(
                    "{}\n{}\n{}",
                    tree_sitter_javascript::HIGHLIGHT_QUERY,
                    tree_sitter_javascript::JSX_HIGHLIGHT_QUERY,
                    tree_sitter_typescript::HIGHLIGHTS_QUERY,
                ),
                format!(
                    "{}\n{}",
                    tree_sitter_javascript::LOCALS_QUERY,
                    tree_sitter_typescript::LOCALS_QUERY,
                ),
            ),
            other => return Err(Error::UnknownLanguage(other.to_owned())),
        };

        let mut config =
            HighlightConfiguration::new(ts_language.into(), language, &highlights, "", &locals)?;
        config.configure(NAMES);

        let mut highlighter = tree_sitter_highlight::Highlighter::new();
        let mut events = Vec::new();
        // highlight() が返すイテレータは highlighter/config/source を借用する自己参照に
        // なるため、ここで全消費して Vec<Event<'a>> に落とす。Text は source からの
        // slice(= &'a str)なので、生成後に core 側オブジェクトを破棄しても安全。
        let iter = highlighter.highlight(&config, source.as_bytes(), None, |_| None)?;
        for event in iter {
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

    #[test]
    fn keyword_text_and_break_are_emitted() {
        let events = collect("typescript", "const x = 1;\n");
        assert!(
            events.contains(&Event::Push(Scope::Keyword)),
            "keyword scope should appear: {events:?}"
        );
        assert!(
            events.iter().any(|e| matches!(e, Event::Text(_))),
            "text should appear: {events:?}"
        );
        assert!(
            events.contains(&Event::Break),
            "break should appear: {events:?}"
        );
    }

    #[test]
    fn text_and_breaks_reconstruct_source() {
        let source = "const x = 1;\nlet y = x + 2;\n";
        let events = collect("typescript", source);
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
    fn tsx_emits_tag_or_attribute() {
        let events = collect("tsx", "const a = <div className=\"a\">x</div>;\n");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::Push(Scope::Tag) | Event::Push(Scope::Attribute))),
            "tsx tag/attribute scope should appear: {events:?}"
        );
    }

    #[test]
    fn unknown_language_errors() {
        let result = Highlighter::new("unknownlang", "x");
        assert!(matches!(result, Err(Error::UnknownLanguage(l)) if l == "unknownlang"));
    }

    #[test]
    fn break_count_matches_line_boundaries() {
        // 3 つの改行 -> Break は 3 つ。
        let source = "a\nb\nc\n";
        let events = collect("typescript", source);
        let breaks = events.iter().filter(|e| matches!(e, Event::Break)).count();
        assert_eq!(breaks, 3, "{events:?}");
    }
}
