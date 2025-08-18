use std::sync::LazyLock;

use syntect::parsing::{SyntaxDefinition, SyntaxSet};

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

pub fn highlight<S: AsRef<str>>(src: &str, lang: &Option<S>) -> Result<String, syntect::Error> {
    use syntect::html::{ClassStyle, ClassedHTMLGenerator};

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

    let mut generator =
        ClassedHTMLGenerator::new_with_class_style(syntax, &SYNTAX_SET, ClassStyle::Spaced);

    for line in src.lines() {
        generator.parse_html_for_line_which_includes_newline(&format!("{line}\n"))?;
    }

    Ok(generator.finalize())
}
