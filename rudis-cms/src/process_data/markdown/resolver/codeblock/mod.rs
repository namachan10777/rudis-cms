use html_escape::encode_safe;
use indexmap::indexmap;
use treesitteract::{Event, Highlighter, Scope};

use crate::process_data::markdown::Node;

struct StackRow<E> {
    leafs: Vec<Node<E>>,
    classes: String,
}

/// tree-sitter のスコープ名(ドット表記)を空白区切りの CSS クラスへ変換する。
/// 例: `"punctuation.bracket"` -> `"punctuation bracket"`。
fn scope_to_classes(scope: &Scope) -> String {
    scope.name().replace('.', " ")
}

pub fn highlight_impl<E>(src: &str, lang: &str) -> Result<Vec<Node<E>>, treesitteract::Error> {
    let mut highlighter = Highlighter::new(lang, src)?;

    // 末尾を根とするスタック。Push で子スコープを積み、Pop で span にまとめて親へ返す。
    let mut stack: Vec<StackRow<E>> = vec![StackRow {
        leafs: Vec::new(),
        classes: String::new(),
    }];

    while let Some(event) = highlighter.next_event() {
        match event {
            Event::Push(scope) => stack.push(StackRow {
                leafs: Vec::new(),
                classes: scope_to_classes(&scope),
            }),
            Event::Pop(_) => {
                let row = stack.pop().unwrap();
                stack.last_mut().unwrap().leafs.push(Node::Eager {
                    tag: "span".into(),
                    attrs: indexmap! {
                        "class".into() => row.classes.into(),
                    },
                    children: row.leafs,
                });
            }
            // Node::Text はレンダリング時に生書き出しされるため、格納前にエスケープする。
            Event::Text(text) => stack
                .last_mut()
                .unwrap()
                .leafs
                .push(Node::Text(encode_safe(text).into_owned())),
            Event::Break => stack
                .last_mut()
                .unwrap()
                .leafs
                .push(Node::Text("\n".into())),
            // tree-sitter バックエンドでは生成されない。
            Event::Clear(_) | Event::Restore | Event::Noop => {}
        }
    }

    Ok(stack.pop().unwrap().leafs)
}

/// `lang` が `None`・未対応言語・パース失敗のときは、エスケープ済みプレーンテキストへ
/// フォールバックする。treesitteract は TypeScript / TSX のみ対応。
pub fn highlight<S: AsRef<str>, E>(src: &str, lang: &Option<S>) -> Vec<Node<E>> {
    if let Some(lang) = lang
        && let Ok(children) = highlight_impl(src, lang.as_ref())
    {
        return children;
    }
    vec![Node::Text(encode_safe(src).into_owned())]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Node ツリーを平坦化して、含まれる class 文字列を集める。
    fn collect_classes<E>(nodes: &[Node<E>], out: &mut Vec<String>) {
        for node in nodes {
            if let Node::Eager {
                attrs, children, ..
            } = node
            {
                if let Some(class) = attrs.get("class").and_then(|v| v.to_str()) {
                    out.push(class.to_string());
                }
                collect_classes(children, out);
            }
        }
    }

    /// Node ツリーの Text を連結する。
    fn collect_text<E>(nodes: &[Node<E>], out: &mut String) {
        for node in nodes {
            match node {
                Node::Text(t) => out.push_str(t),
                Node::Eager { children, .. } => collect_text(children, out),
                Node::Lazy { children, .. } => collect_text(children, out),
            }
        }
    }

    #[test]
    fn typescript_produces_keyword_span() {
        let nodes = highlight::<&str, ()>("const x = 1;", &Some("typescript"));
        let mut classes = Vec::new();
        collect_classes(&nodes, &mut classes);
        assert!(
            classes.iter().any(|c| c.split(' ').any(|c| c == "keyword")),
            "keyword span should appear: {classes:?}"
        );
    }

    #[test]
    fn unsupported_language_falls_back_to_escaped_text() {
        let nodes = highlight::<&str, ()>("let x: Vec<T> = 1;", &Some("definitely-not-a-lang"));
        let mut classes = Vec::new();
        collect_classes(&nodes, &mut classes);
        assert!(
            classes.is_empty(),
            "no span for unsupported lang: {classes:?}"
        );
        let mut text = String::new();
        collect_text(&nodes, &mut text);
        assert!(text.contains("&lt;T&gt;"), "text must be escaped: {text:?}");
    }

    #[test]
    fn no_language_falls_back_to_escaped_text() {
        let nodes = highlight::<&str, ()>("a < b && c", &None::<&str>);
        let mut classes = Vec::new();
        collect_classes(&nodes, &mut classes);
        assert!(classes.is_empty());
        let mut text = String::new();
        collect_text(&nodes, &mut text);
        assert!(text.contains("a &lt; b &amp;&amp; c"), "escaped: {text:?}");
    }
}
