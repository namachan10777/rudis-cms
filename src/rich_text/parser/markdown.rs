use std::{collections::HashMap, sync::LazyLock};

use maplit::hashmap;
use pulldown_cmark::{
    Alignment, BlockQuoteKind, CodeBlockKind, Event, HeadingLevel, LinkType, MetadataBlockKind,
    Tag, TagEnd,
};
use tracing::warn;

use crate::rich_text::{
    AlertKind, AttrValue, Extracted, MdAst, MdRoot, Name, codeblock::meta_parser::CodeblockMeta,
    parser::Error,
};

pub struct MarkdownParser {}

impl MarkdownParser {
    pub fn new() -> MarkdownParser {
        MarkdownParser {}
    }
}

struct ParserImpl<'src> {
    parser: pulldown_cmark::Parser<'src>,
    lookahead: Vec<Event<'src>>,
    frontmatter: Option<Result<serde_json::Value, String>>,
    footnote_definitions: HashMap<String, MdAst>,
}

impl<'src> ParserImpl<'src> {
    fn next_event(&mut self) -> Option<Event<'src>> {
        if let Some(event) = self.lookahead.pop() {
            return Some(event);
        }
        match self.parser.next() {
            Some(event) => Some(event),
            None => None,
        }
    }

    fn return_event(&mut self, event: Event<'src>) {
        self.lookahead.push(event);
    }
}

const KATEX_DISPLAY_MATH_OPTS: LazyLock<katex::Opts> =
    LazyLock::new(|| katex::Opts::builder().display_mode(true).build().unwrap());

const KATEX_INLINE_MATH_OPTS: LazyLock<katex::Opts> =
    LazyLock::new(|| katex::Opts::builder().display_mode(false).build().unwrap());

fn is_end<'src>(tag: &Tag<'src>, event: &Event<'src>) -> bool {
    matches!(
        (tag, event),
        (Tag::BlockQuote(_), Event::End(TagEnd::BlockQuote(_)))
            | (Tag::CodeBlock(_), Event::End(TagEnd::CodeBlock))
            | (Tag::DefinitionList, Event::End(TagEnd::DefinitionList))
            | (
                Tag::DefinitionListDefinition,
                Event::End(TagEnd::DefinitionListDefinition)
            )
            | (
                Tag::DefinitionListTitle,
                Event::End(TagEnd::DefinitionListTitle)
            )
            | (Tag::Emphasis, Event::End(TagEnd::Emphasis))
            | (
                Tag::FootnoteDefinition(_),
                Event::End(TagEnd::FootnoteDefinition)
            )
            | (Tag::Heading { .. }, Event::End(TagEnd::Heading(_)))
            | (Tag::HtmlBlock, Event::End(TagEnd::HtmlBlock))
            | (Tag::Image { .. }, Event::End(TagEnd::Image))
            | (Tag::Item, Event::End(TagEnd::Item))
            | (Tag::Link { .. }, Event::End(TagEnd::Link))
            | (Tag::List(_), Event::End(TagEnd::List(_)))
            | (Tag::MetadataBlock(_), Event::End(TagEnd::MetadataBlock(_)))
            | (Tag::Paragraph, Event::End(TagEnd::Paragraph))
            | (Tag::Strikethrough, Event::End(TagEnd::Strikethrough))
            | (Tag::Strong, Event::End(TagEnd::Strong))
            | (Tag::Subscript, Event::End(TagEnd::Subscript))
            | (Tag::Superscript, Event::End(TagEnd::Superscript))
            | (Tag::Table(_), Event::End(TagEnd::Table))
            | (Tag::TableCell, Event::End(TagEnd::TableCell))
            | (Tag::TableHead, Event::End(TagEnd::TableHead))
            | (Tag::TableRow, Event::End(TagEnd::TableRow))
    )
}

fn text_content(children: Vec<MdAst>) -> String {
    children
        .into_iter()
        .filter_map(|child| match child {
            MdAst::Text(text) => Some(text),
            MdAst::Eager { children, .. } => Some(text_content(children)),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn heading_to_id(content: &str) -> String {
    content
        .chars()
        .map(|c| if c.is_whitespace() { '-' } else { c })
        .collect()
}

fn parse_until_next_heading<'src>(
    current_level: HeadingLevel,
    parser: &mut ParserImpl<'src>,
) -> Vec<MdAst> {
    let mut children = Vec::new();
    while let Some(event) = parser.next_event() {
        if matches!(event, Event::Start(Tag::Heading { level, .. }) if level >= current_level) {
            parser.return_event(event);
            break;
        }
        parser.return_event(event);
        children.extend(parse(parser));
    }
    children
}

enum MaybeMany<T> {
    One(Option<T>),
    Many(Vec<T>),
}

impl<T> MaybeMany<T> {
    pub fn one(v: T) -> Self {
        Self::One(Some(v))
    }

    pub fn none() -> Self {
        Self::One(None)
    }

    pub fn many(v: Vec<T>) -> Self {
        Self::Many(v)
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::One(None))
    }
}

impl<T> Iterator for MaybeMany<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MaybeMany::One(opt) => opt.take(),
            MaybeMany::Many(many) => many.pop(),
        }
    }
}

fn write_item_centering(aligns: &[Alignment], children: &mut [MdAst]) {
    for row in children {
        let MdAst::Eager { children, .. } = row else {
            unreachable!()
        };
        for (td, align) in children.iter_mut().zip(aligns.iter()) {
            let MdAst::Eager { attrs, .. } = td else {
                unreachable!()
            };
            match align {
                Alignment::None => {}
                Alignment::Center => {
                    attrs.insert("class".into(), "table-cell-center".into());
                }
                Alignment::Left => {
                    attrs.insert("class".into(), "table-cell-left".into());
                }
                Alignment::Right => {
                    attrs.insert("class".into(), "table-cell-right".into());
                }
            }
        }
    }
}

fn fix_thead(thead: MdAst) -> MdAst {
    let MdAst::Eager {
        attrs, children, ..
    } = thead
    else {
        unreachable!()
    };

    let children = children
        .into_iter()
        .map(|td| {
            let MdAst::Eager {
                attrs, children, ..
            } = td
            else {
                unreachable!();
            };
            MdAst::Eager {
                tag: "th".into(),
                attrs,
                children,
            }
        })
        .collect();

    MdAst::Eager {
        tag: "thead".into(),
        attrs,
        children: vec![MdAst::Eager {
            tag: "tr".into(),
            attrs: hashmap! {},
            children,
        }],
    }
}

fn construct_table(aligns: &[Alignment], mut children: Vec<MdAst>) -> MdAst {
    write_item_centering(aligns, &mut children);
    let mut children = children.into_iter();

    let thead = children.next().expect("unexpected markdown table syntax?");
    let thead = fix_thead(thead);

    MdAst::Eager {
        tag: "table".into(),
        attrs: hashmap! {},
        children: vec![
            thead,
            MdAst::Eager {
                tag: "tbody".into(),
                attrs: hashmap! {},
                children: children.collect(),
            },
        ],
    }
}

fn parse_spanned<'src>(parser: &mut ParserImpl<'src>, tag: Tag<'src>) -> MaybeMany<MdAst> {
    let mut children = Vec::new();
    while let Some(event) = parser.next_event() {
        if is_end(&tag, &event) {
            break;
        }
        parser.return_event(event);
        children.extend(parse(parser));
    }
    match tag {
        Tag::BlockQuote(None) => MaybeMany::one(MdAst::Eager {
            tag: "blockquote".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::BlockQuote(Some(kind)) => {
            let kind = match kind {
                BlockQuoteKind::Caution => AlertKind::Caution,
                BlockQuoteKind::Important => AlertKind::Important,
                BlockQuoteKind::Note => AlertKind::Note,
                BlockQuoteKind::Warning => AlertKind::Warning,
                BlockQuoteKind::Tip => AlertKind::Tip,
            };
            MaybeMany::one(MdAst::Lazy {
                extracted: Extracted::Alert { kind },
                children,
            })
        }
        Tag::CodeBlock(CodeBlockKind::Indented) => MaybeMany::one(MdAst::Lazy {
            extracted: Extracted::Codeblock {
                meta: Default::default(),
            },
            children,
        }),
        Tag::CodeBlock(CodeBlockKind::Fenced(meta)) => {
            let meta = meta.parse().unwrap_or_default();
            MaybeMany::one(MdAst::Lazy {
                extracted: Extracted::Codeblock { meta },
                children,
            })
        }
        Tag::DefinitionList => MaybeMany::one(MdAst::Eager {
            tag: "ul".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::DefinitionListTitle => MaybeMany::one(MdAst::Eager {
            tag: "dfn".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::DefinitionListDefinition => MaybeMany::one(MdAst::Eager {
            tag: "p".into(),
            attrs: hashmap! {"class".into() => "dfn-description".into()},
            children,
        }),
        Tag::FootnoteDefinition(id) => {
            parser.footnote_definitions.insert(
                id.to_string(),
                MdAst::Eager {
                    tag: "p".into(),
                    attrs: hashmap! {},
                    children,
                },
            );
            MaybeMany::none()
        }
        Tag::Heading {
            level,
            id,
            classes,
            attrs,
        } => {
            let id = id.map(|s| s.into_static().into()).unwrap_or_else(|| {
                let text = text_content(children.clone());
                AttrValue::OwnedStr(heading_to_id(&text))
            });

            let attrs = attrs.into_iter().map(|(name, option)| {
                (
                    name.into_static().into(),
                    option
                        .map(|s| s.parse().unwrap())
                        .unwrap_or(AttrValue::Bool(true)),
                )
            });
            let class = classes.join(" ");
            let attrs = attrs
                .chain([
                    ("class".into(), AttrValue::OwnedStr(class)),
                    ("id".into(), id),
                ])
                .collect::<HashMap<Name, AttrValue>>();
            let heading = Extracted::Heading {
                level: level as u8,
                attrs,
            };
            let mut body = vec![MdAst::Lazy {
                extracted: heading,
                children,
            }];
            body.extend(parse_until_next_heading(level, parser));
            MaybeMany::one(MdAst::Eager {
                tag: "section".into(),
                attrs: hashmap! {},
                children: body,
            })
        }
        Tag::Image {
            dest_url,
            title,
            id,
            ..
        } => MaybeMany::one(MdAst::Lazy {
            extracted: Extracted::Image {
                title: title.to_string(),
                id: id.to_string(),
                url: dest_url.to_string(),
            },
            children,
        }),
        Tag::HtmlBlock => MaybeMany::many(children),
        Tag::List(first_number) => {
            let e = match first_number {
                None => MdAst::Eager {
                    tag: "ul".into(),
                    attrs: hashmap! {},
                    children,
                },
                Some(1) => MdAst::Eager {
                    tag: "ol".into(),
                    attrs: hashmap! {},
                    children,
                },
                Some(n) => MdAst::Eager {
                    tag: "ol".into(),
                    attrs: hashmap! {"start".into() => (n as i64).into()},
                    children,
                },
            };
            MaybeMany::one(e)
        }
        Tag::Item => MaybeMany::one(MdAst::Eager {
            tag: "li".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::MetadataBlock(kind) => {
            match kind {
                MetadataBlockKind::PlusesStyle => {
                    parser.frontmatter =
                        Some(toml::from_str(&text_content(children)).map_err(|e| e.to_string()));
                }
                MetadataBlockKind::YamlStyle => {
                    parser.frontmatter = Some(
                        serde_yaml::from_str(&text_content(children)).map_err(|e| e.to_string()),
                    );
                }
            };
            MaybeMany::none()
        }
        Tag::Paragraph => MaybeMany::one(MdAst::Eager {
            tag: "p".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Emphasis => MaybeMany::one(MdAst::Eager {
            tag: "em".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Strikethrough => MaybeMany::one(MdAst::Eager {
            tag: "s".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Strong => MaybeMany::one(MdAst::Eager {
            tag: "strong".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Subscript => MaybeMany::one(MdAst::Eager {
            tag: "sub".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Superscript => MaybeMany::one(MdAst::Eager {
            tag: "sup".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Table(aligns) => MaybeMany::one(construct_table(&aligns, children)),
        Tag::TableCell => MaybeMany::one(MdAst::Eager {
            tag: "td".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::TableHead => MaybeMany::one(MdAst::Eager {
            tag: "thead".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::TableRow => MaybeMany::one(MdAst::Eager {
            tag: "tr".into(),
            attrs: hashmap! {},
            children,
        }),
        Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        } => {
            let link_type = match link_type {
                LinkType::Autolink => crate::rich_text::LinkType::Autolink,
                LinkType::Collapsed
                | LinkType::Reference
                | LinkType::Shortcut
                | LinkType::Inline => crate::rich_text::LinkType::Normal,
                LinkType::CollapsedUnknown
                | LinkType::ShortcutUnknown
                | LinkType::ReferenceUnknown => crate::rich_text::LinkType::Broken,
                LinkType::Email => crate::rich_text::LinkType::Email,
                LinkType::WikiLink { .. } => crate::rich_text::LinkType::Wikilink,
            };
            let extracted = Extracted::Link {
                link_type,
                dest_url: dest_url.to_string(),
                title: title.to_string(),
                id: id.to_string(),
            };
            MaybeMany::one(MdAst::Lazy {
                extracted,
                children,
            })
        }
    }
}

fn parse<'src>(parser: &mut ParserImpl<'src>) -> MaybeMany<MdAst> {
    let Some(event) = parser.next_event() else {
        return MaybeMany::none();
    };
    let raw = match event {
        Event::Text(text) => MdAst::Text(text.into_string()),
        Event::Html(html) => MdAst::Raw(html.into_string()),
        Event::InlineHtml(html) => MdAst::Raw(html.into_string()),
        Event::Code(code) => MdAst::Eager {
            tag: "code".into(),
            attrs: hashmap! {},
            children: vec![MdAst::Text(code.into_string())],
        },
        Event::DisplayMath(math) => {
            match katex::render_with_opts(&math, KATEX_DISPLAY_MATH_OPTS.as_ref()) {
                Ok(katex) => MdAst::Eager {
                    tag: "div".into(),
                    attrs: hashmap! {},
                    children: vec![MdAst::Raw(katex)],
                },
                Err(e) => {
                    warn!(%e, "failed to parse katex math");
                    MdAst::Lazy {
                        extracted: Extracted::Codeblock {
                            meta: CodeblockMeta {
                                lang: Some("tex".into()),
                                attrs: Default::default(),
                            },
                        },
                        children: vec![MdAst::Text(math.to_string())],
                    }
                }
            }
        }
        Event::InlineMath(math) => {
            match katex::render_with_opts(&math, KATEX_INLINE_MATH_OPTS.as_ref()) {
                Ok(katex) => MdAst::Eager {
                    tag: "span".into(),
                    attrs: hashmap! {},
                    children: vec![MdAst::Raw(katex)],
                },
                Err(e) => {
                    warn!(%e, "failed to parse katex math");
                    MdAst::Eager {
                        tag: "span".into(),
                        attrs: hashmap! {},
                        children: vec![MdAst::Text(math.into_string())],
                    }
                }
            }
        }
        Event::FootnoteReference(r) => MdAst::Lazy {
            extracted: Extracted::FootnoteReference {
                id: r.into_string(),
            },
            children: Default::default(),
        },
        Event::HardBreak => MdAst::Eager {
            tag: "br".into(),
            attrs: hashmap! {},
            children: vec![],
        },
        Event::Rule => MdAst::Eager {
            tag: "hr".into(),
            attrs: hashmap! {},
            children: vec![],
        },
        Event::SoftBreak => MdAst::Eager {
            tag: "wbr".into(),
            attrs: hashmap! {},
            children: vec![],
        },
        Event::TaskListMarker(marker) => MdAst::Eager {
            tag: "input".into(),
            attrs: hashmap! {
                "type".to_string() => "checkbox".into(),
                "disabled".to_string() => true.into(),
                "checked".to_string() => marker.into(),
            },
            children: vec![],
        },
        Event::End(_) => unreachable!(),
        Event::Start(tag) => return parse_spanned(parser, tag),
    };
    MaybeMany::one(raw)
}

impl super::Parser for MarkdownParser {
    fn parse(&self, src: &str) -> Result<MdRoot, Error> {
        let options = pulldown_cmark::Options::all()
            .difference(pulldown_cmark::Options::ENABLE_OLD_FOOTNOTES);
        let mut parser = ParserImpl {
            lookahead: Default::default(),
            parser: pulldown_cmark::Parser::new_ext(src, options),
            frontmatter: None,
            footnote_definitions: Default::default(),
        };
        let mut children = Vec::new();
        loop {
            let elements = parse(&mut parser);
            if elements.is_empty() {
                break;
            }
            children.extend(elements);
        }
        let frontmatter = if let Some(frontmatter) = parser.frontmatter {
            Some(frontmatter.map_err(|e| Error::InvalidFrontmatter(e.to_string()))?)
        } else {
            None
        };
        Ok(MdRoot {
            children,
            frontmatter,
            footnote_definitions: parser.footnote_definitions,
        })
    }
}
