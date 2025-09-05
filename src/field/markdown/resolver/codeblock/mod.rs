use std::sync::LazyLock;

use html_escape::encode_safe;
use indexmap::indexmap;
use syntect::parsing::{BasicScopeStackOp, SyntaxDefinition, SyntaxSet};

use crate::field::markdown::Node;

fn load_syntax_set() -> anyhow::Result<SyntaxSet> {
    syntect::dumps::from_dump_file(".blindcms/syntect.packdump").map_err(Into::into)
}

fn create_syntax_set() -> SyntaxSet {
    match load_syntax_set() {
        Ok(ss) => ss,
        Err(_) => {
            let ts_syntax = include_str!("./syntax/TypeScript.sublime-syntax");
            let ts_syntax = SyntaxDefinition::load_from_str(ts_syntax, true, None).unwrap();
            let ts_react_syntax = include_str!("./syntax/TypeScriptReact.sublime-syntax");
            let ts_react_syntax =
                SyntaxDefinition::load_from_str(ts_react_syntax, true, None).unwrap();

            let mut builder = SyntaxSet::load_defaults_newlines().into_builder();
            builder.add(ts_syntax);
            builder.add(ts_react_syntax);
            let ss = builder.build();
            let _ = std::fs::create_dir_all(".blindcms");
            let _ = syntect::dumps::dump_to_file(&ss, ".blindcms/syntect.packdump");
            ss
        }
    }
}

static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(create_syntax_set);

struct StackRow<E> {
    leafs: Vec<Node<E>>,
    classes: String,
}

fn scope_to_classes(scope: syntect::parsing::Scope, prefix: Option<&str>) -> String {
    let mut s = String::new();
    let repo = syntect::parsing::SCOPE_REPO.lock().unwrap();
    for i in 0..(scope.len()) {
        let atom = scope.atom_at(i as usize);
        let atom_s = repo.atom_str(atom);
        if i != 0 {
            s.push(' ')
        }
        match prefix {
            None => {}
            Some(prefix) => {
                s.push_str(prefix);
            }
        }
        s.push_str(atom_s);
    }
    s
}

pub fn highlight_impl<S: AsRef<str>, E>(
    src: &str,
    lang: &Option<S>,
) -> Result<Vec<Node<E>>, syntect::Error> {
    // Try to find syntax by language name, fallback to plain text
    let syntax = if let Some(lang) = lang {
        SYNTAX_SET
            .find_syntax_by_name(lang.as_ref())
            .or_else(|| SYNTAX_SET.find_syntax_by_extension(lang.as_ref()))
            .or_else(|| SYNTAX_SET.find_syntax_by_first_line(src))
            .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text())
    } else {
        SYNTAX_SET.find_syntax_plain_text()
    };

    let mut parse_state = syntect::parsing::ParseState::new(syntax);
    let mut scope_stack = syntect::parsing::ScopeStack::new();
    let mut stack = Vec::<StackRow<E>>::new();

    for line in src.lines() {
        let mut cur_index = 0;

        let ops = parse_state.parse_line(line, &SYNTAX_SET)?;
        for (i, op) in ops {
            if i > cur_index {
                stack
                    .last_mut()
                    .unwrap()
                    .leafs
                    .push(Node::Text(encode_safe(&line[cur_index..i]).into_owned()));
                cur_index = i;
            }
            scope_stack.apply_with_hook(&op, |op, _| match op {
                BasicScopeStackOp::Push(scope) => {
                    stack.push(StackRow {
                        leafs: Vec::new(),
                        classes: scope_to_classes(scope, None),
                    });
                }
                BasicScopeStackOp::Pop => {
                    let row = stack.pop().unwrap();
                    stack.last_mut().unwrap().leafs.push(Node::Eager {
                        tag: "span".into(),
                        attrs: indexmap! {
                            "class".into() => row.classes.into(),
                        },
                        children: row.leafs,
                    });
                }
            })?;
        }
        stack
            .last_mut()
            .unwrap()
            .leafs
            .push(Node::Text(format!("{}\n", encode_safe(&line[cur_index..]))));
    }

    Ok(stack.pop().unwrap().leafs)
}

pub fn highlight<S: AsRef<str>, E>(src: &str, lang: &Option<S>) -> Vec<Node<E>> {
    if let Ok(children) = highlight_impl(src, lang) {
        children
    } else {
        vec![Node::Text(src.into())]
    }
}
