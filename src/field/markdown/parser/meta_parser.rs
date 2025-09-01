use crate::field::markdown::types::{AttrValue, Name};
use std::{collections::HashMap, str::FromStr};

use valuable::Valuable;
use winnow::{
    Parser as _,
    ascii::space0,
    combinator::{alt, opt, separated_pair},
    token::take_while,
};

#[derive(Debug, thiserror::Error)]
#[error("failed to parse codeblock meta {0}")]
pub struct Error(String);

#[derive(Debug, Clone, Valuable, Default)]
pub struct CodeblockMeta {
    pub lang: Option<String>,
    pub attrs: HashMap<Name, AttrValue>,
}

type ParseResult<T> = Result<T, winnow::error::ErrMode<winnow::error::ContextError>>;

fn identifier<'a>(input: &mut &'a str) -> ParseResult<&'a str> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '_' || c == '-').parse_next(input)
}

fn quoted_string(input: &mut &str) -> ParseResult<String> {
    alt((parse_double_quoted_string, parse_single_quoted_string)).parse_next(input)
}

fn parse_double_quoted_string(input: &mut &str) -> ParseResult<String> {
    let _: char = '"'.parse_next(input)?;
    let mut result = String::new();

    loop {
        if input.starts_with('"') {
            let _: char = '"'.parse_next(input)?;
            break;
        }

        if input.starts_with('\\') {
            let _: char = '\\'.parse_next(input)?;
            if input.is_empty() {
                return Err(winnow::error::ErrMode::Backtrack(
                    winnow::error::ContextError::new(),
                ));
            }
            let escaped_char: char = winnow::token::any.parse_next(input)?;
            match escaped_char {
                '"' => result.push('"'),
                '\\' => result.push('\\'),
                'n' => result.push('\n'),
                't' => result.push('\t'),
                'r' => result.push('\r'),
                _ => {
                    result.push('\\');
                    result.push(escaped_char);
                }
            }
        } else {
            let ch: char = winnow::token::any.parse_next(input)?;
            result.push(ch);
        }
    }

    Ok(result)
}

fn parse_single_quoted_string(input: &mut &str) -> ParseResult<String> {
    let _: char = '\''.parse_next(input)?;
    let mut result = String::new();

    loop {
        if input.starts_with('\'') {
            let _: char = '\''.parse_next(input)?;
            break;
        }

        if input.starts_with('\\') {
            let _: char = '\\'.parse_next(input)?;
            if input.is_empty() {
                return Err(winnow::error::ErrMode::Backtrack(
                    winnow::error::ContextError::new(),
                ));
            }
            let escaped_char: char = winnow::token::any.parse_next(input)?;
            match escaped_char {
                '\'' => result.push('\''),
                '\\' => result.push('\\'),
                'n' => result.push('\n'),
                't' => result.push('\t'),
                'r' => result.push('\r'),
                _ => {
                    result.push('\\');
                    result.push(escaped_char);
                }
            }
        } else {
            let ch: char = winnow::token::any.parse_next(input)?;
            result.push(ch);
        }
    }

    Ok(result)
}

fn attribute_value(input: &mut &str) -> ParseResult<AttrValue> {
    alt((
        quoted_string.map(AttrValue::OwnedStr),
        identifier.map(|s: &str| {
            if s == "true" {
                AttrValue::Bool(true)
            } else if s == "false" {
                AttrValue::Bool(false)
            } else if let Ok(i) = s.parse::<i64>() {
                AttrValue::Integer(i)
            } else {
                AttrValue::OwnedStr(s.into())
            }
        }),
    ))
    .parse_next(input)
}

fn attribute_with_value(input: &mut &str) -> ParseResult<(String, AttrValue)> {
    separated_pair(
        identifier.map(|s: &str| s.to_string()),
        '=',
        attribute_value,
    )
    .parse_next(input)
}

fn attribute_flag(input: &mut &str) -> ParseResult<(String, AttrValue)> {
    identifier
        .map(|s: &str| (s.to_string(), AttrValue::Bool(true)))
        .parse_next(input)
}

fn attribute(input: &mut &str) -> ParseResult<(String, AttrValue)> {
    alt((attribute_with_value, attribute_flag)).parse_next(input)
}

fn attributes_block(input: &mut &str) -> ParseResult<HashMap<Name, AttrValue>> {
    let _: char = '{'.parse_next(input)?;
    space0.parse_next(input)?;

    let mut attrs = HashMap::new();

    while !input.starts_with('}') && !input.is_empty() {
        let (key, value) = attribute.parse_next(input)?;
        attrs.insert(key.into(), value);

        space0.parse_next(input)?;

        // Skip optional comma
        if input.starts_with(',') {
            let _: char = ','.parse_next(input)?;
            space0.parse_next(input)?;
        }

        // If we see space followed by non-brace, continue parsing
        if !input.starts_with('}') && !input.is_empty() {
            // Continue to next attribute
            continue;
        }
        break;
    }

    space0.parse_next(input)?;
    let _: char = '}'.parse_next(input)?;

    Ok(attrs)
}

pub fn parse_codeblock_info(input: &str) -> ParseResult<CodeblockMeta> {
    use winnow::Parser;

    let mut input = input.trim();

    // Parse optional language
    let lang = opt(identifier).parse_next(&mut input)?;

    // Skip whitespace
    space0.parse_next(&mut input)?;

    // Parse optional attributes block
    let attrs = opt(attributes_block).parse_next(&mut input)?;

    Ok(CodeblockMeta {
        lang: lang.map(|l| l.to_string()),
        attrs: attrs.unwrap_or_default(),
    })
}

impl FromStr for CodeblockMeta {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_codeblock_info(s).map_err(|e| Error(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::field::markdown::types::AttrValue;

    use super::parse_codeblock_info;

    #[test]
    fn test_parse_language_only() {
        let result = parse_codeblock_info("rust").unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert!(result.attrs.is_empty());
    }

    #[test]
    fn test_parse_language_with_simple_attributes() {
        let result = parse_codeblock_info("rust {linenos=true}").unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert_eq!(result.attrs.len(), 1);
        assert_eq!(result.attrs.get("linenos").unwrap(), &true.into());
    }

    #[test]
    fn test_parse_python_with_quoted_values() {
        let result = parse_codeblock_info(r#"python {start=1 theme="solarized"}"#).unwrap();
        assert_eq!(result.lang, Some("python".to_string()));
        assert_eq!(result.attrs.len(), 2);
        assert_eq!(result.attrs.get("start").unwrap(), &AttrValue::Integer(1));
        assert_eq!(
            result.attrs.get("theme").unwrap().to_str(),
            Some("solarized")
        );
    }

    #[test]
    fn test_parse_flags() {
        let result = parse_codeblock_info("javascript {linenos, readonly}").unwrap();
        assert_eq!(result.lang, Some("javascript".to_string()));
        assert_eq!(result.attrs.len(), 2);
        match result.attrs.get("linenos").unwrap() {
            AttrValue::Bool(true) => (),
            _ => panic!("Expected True flag"),
        }
        match result.attrs.get("readonly").unwrap() {
            AttrValue::Bool(true) => (),
            _ => panic!("Expected True flag"),
        }
    }

    #[test]
    fn test_parse_no_language_with_attributes() {
        let result = parse_codeblock_info("{linenos=true}").unwrap();
        assert_eq!(result.lang, None);
        assert_eq!(result.attrs.len(), 1);
        assert_eq!(result.attrs.get("linenos").unwrap(), &true.into());
    }

    #[test]
    fn test_parse_empty() {
        let result = parse_codeblock_info("").unwrap();
        assert_eq!(result.lang, None);
        assert!(result.attrs.is_empty());
    }

    #[test]
    fn test_parse_mixed_attributes() {
        let result = parse_codeblock_info("rust {linenos=true, readonly, theme=\"dark\"}").unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert_eq!(result.attrs.len(), 3);

        assert_eq!(result.attrs.get("linenos").unwrap(), &true.into());

        match result.attrs.get("readonly").unwrap() {
            AttrValue::Bool(true) => (),
            _ => panic!("Expected True flag"),
        }

        assert_eq!(result.attrs.get("theme").unwrap(), &"dark".into());
    }

    #[test]
    fn test_parse_escaped_quotes() {
        let result = parse_codeblock_info(r#"rust {title="He said \"Hello\""}"#).unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert_eq!(result.attrs.len(), 1);
        assert_eq!(
            result.attrs.get("title").unwrap(),
            &r#"He said "Hello""#.into()
        );
    }

    #[test]
    fn test_parse_escaped_backslashes() {
        let result = parse_codeblock_info(r#"rust {path="C:\\Program Files\\test"}"#).unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert_eq!(result.attrs.len(), 1);
        assert_eq!(
            result.attrs.get("path").unwrap(),
            &r#"C:\Program Files\test"#.into()
        );
    }

    #[test]
    fn test_parse_special_escapes() {
        let result = parse_codeblock_info(r#"rust {msg="Line1\nLine2\tTabbed"}"#).unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert_eq!(result.attrs.len(), 1);
        assert_eq!(
            result.attrs.get("msg").unwrap(),
            &"Line1\nLine2\tTabbed".into()
        );
    }

    #[test]
    fn test_parse_single_quotes_with_escapes() {
        let result = parse_codeblock_info(r#"rust {msg='Don\'t worry'}"#).unwrap();
        assert_eq!(result.lang, Some("rust".to_string()));
        assert_eq!(result.attrs.len(), 1);
        assert_eq!(result.attrs.get("msg").unwrap(), &"Don't worry".into());
    }
}
