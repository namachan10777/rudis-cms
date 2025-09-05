use std::sync::LazyLock;

use indexmap::{IndexMap, indexmap};
use pulldown_cmark::{
    Alignment, BlockQuoteKind, CodeBlockKind, Event, HeadingLevel, LinkType, Tag, TagEnd,
};
use tracing::warn;

use crate::field::markdown::{
    AlertKind, Node,
    parser::meta_parser::CodeblockMeta,
    raw_to_expanded,
    types::{AttrValue, Name},
};

use super::KeepRaw;

struct ParserImpl<'src> {
    parser: pulldown_cmark::Parser<'src>,
    lookahead: Vec<Event<'src>>,
    footnotes: IndexMap<String, Vec<Node<KeepRaw>>>,
}

impl<'src> ParserImpl<'src> {
    fn next_event(&mut self) -> Option<Event<'src>> {
        if let Some(event) = self.lookahead.pop() {
            return Some(event);
        }
        self.parser.next()
    }

    fn return_event(&mut self, event: Event<'src>) {
        self.lookahead.push(event);
    }
}

static KATEX_DISPLAY_MATH_OPTS: LazyLock<katex::Opts> =
    LazyLock::new(|| katex::Opts::builder().display_mode(true).build().unwrap());

static KATEX_INLINE_MATH_OPTS: LazyLock<katex::Opts> =
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

fn text_content(children: Vec<Node<KeepRaw>>) -> String {
    children
        .into_iter()
        .filter_map(|child| match child {
            Node::Text(text) => Some(text),
            Node::Eager { children, .. } => Some(text_content(children)),
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
) -> Vec<Node<KeepRaw>> {
    let mut children = Vec::new();
    while let Some(event) = parser.next_event() {
        if matches!(event, Event::Start(Tag::Heading { level, .. }) if level >= current_level) {
            parser.return_event(event);
            break;
        }
        parser.return_event(event);
        children.extend(parse_element(parser));
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

fn write_item_centering(aligns: &[Alignment], children: &mut [Node<KeepRaw>]) {
    for row in children {
        let Node::Eager { children, .. } = row else {
            unreachable!()
        };
        for (td, align) in children.iter_mut().zip(aligns.iter()) {
            let Node::Eager { attrs, .. } = td else {
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

fn fix_thead(thead: Node<KeepRaw>) -> Node<KeepRaw> {
    let Node::Eager {
        attrs, children, ..
    } = thead
    else {
        unreachable!()
    };

    let children = children
        .into_iter()
        .map(|td| {
            let Node::Eager {
                attrs, children, ..
            } = td
            else {
                unreachable!();
            };
            Node::Eager {
                tag: "th".into(),
                attrs,
                children,
            }
        })
        .collect();

    Node::Eager {
        tag: "thead".into(),
        attrs,
        children: vec![Node::Eager {
            tag: "tr".into(),
            attrs: Default::default(),
            children,
        }],
    }
}

fn construct_table(aligns: &[Alignment], mut children: Vec<Node<KeepRaw>>) -> Node<KeepRaw> {
    write_item_centering(aligns, &mut children);
    let mut children = children.into_iter();

    let thead = children.next().expect("unexpected markdown table syntax?");
    let thead = fix_thead(thead);

    Node::Eager {
        tag: "table".into(),
        attrs: Default::default(),
        children: vec![
            thead,
            Node::Eager {
                tag: "tbody".into(),
                attrs: Default::default(),
                children: children.collect(),
            },
        ],
    }
}

fn parse_spanned<'src>(parser: &mut ParserImpl<'src>, tag: Tag<'src>) -> MaybeMany<Node<KeepRaw>> {
    let mut children = Vec::new();
    while let Some(event) = parser.next_event() {
        if is_end(&tag, &event) {
            break;
        }
        parser.return_event(event);
        children.extend(parse_element(parser));
    }
    match tag {
        Tag::BlockQuote(None) => MaybeMany::one(Node::Eager {
            tag: "blockquote".into(),
            attrs: Default::default(),
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
            MaybeMany::one(Node::Lazy {
                keep: KeepRaw::Alert { kind },
                children,
            })
        }
        Tag::CodeBlock(CodeBlockKind::Indented) => MaybeMany::one(Node::Lazy {
            keep: KeepRaw::Codeblock {
                meta: Default::default(),
            },
            children,
        }),
        Tag::CodeBlock(CodeBlockKind::Fenced(meta)) => {
            let meta = meta.parse().unwrap_or_default();
            MaybeMany::one(Node::Lazy {
                keep: KeepRaw::Codeblock { meta },
                children,
            })
        }
        Tag::DefinitionList => MaybeMany::one(Node::Eager {
            tag: "ul".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::DefinitionListTitle => MaybeMany::one(Node::Eager {
            tag: "dfn".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::DefinitionListDefinition => MaybeMany::one(Node::Eager {
            tag: "p".into(),
            attrs: indexmap! {"class".into() => "dfn-description".into()},
            children,
        }),
        Tag::FootnoteDefinition(id) => {
            parser.footnotes.insert(id.to_string(), children);
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
                .collect::<IndexMap<Name, AttrValue>>();
            let heading = KeepRaw::Heading {
                level: level.into(),
                attrs,
            };
            let mut body = vec![Node::Lazy {
                keep: heading,
                children,
            }];
            body.extend(parse_until_next_heading(level, parser));
            MaybeMany::one(Node::Eager {
                tag: "section".into(),
                attrs: Default::default(),
                children: body,
            })
        }
        Tag::Image {
            dest_url,
            title,
            id,
            ..
        } => MaybeMany::one(Node::Lazy {
            keep: KeepRaw::Image {
                title: title.to_string(),
                id: id.to_string(),
                url: dest_url.to_string(),
            },
            children,
        }),
        Tag::HtmlBlock => MaybeMany::many(children),
        Tag::List(first_number) => {
            let e = match first_number {
                None => Node::Eager {
                    tag: "ul".into(),
                    attrs: Default::default(),
                    children,
                },
                Some(1) => Node::Eager {
                    tag: "ol".into(),
                    attrs: Default::default(),
                    children,
                },
                Some(n) => Node::Eager {
                    tag: "ol".into(),
                    attrs: indexmap! {"start".into() => (n as i64).into()},
                    children,
                },
            };
            MaybeMany::one(e)
        }
        Tag::Item => MaybeMany::one(Node::Eager {
            tag: "li".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::MetadataBlock(_) => MaybeMany::one(Node::Text(Default::default())),
        Tag::Paragraph => MaybeMany::one(Node::Eager {
            tag: "p".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Emphasis => MaybeMany::one(Node::Eager {
            tag: "em".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Strikethrough => MaybeMany::one(Node::Eager {
            tag: "s".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Strong => MaybeMany::one(Node::Eager {
            tag: "strong".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Subscript => MaybeMany::one(Node::Eager {
            tag: "sub".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Superscript => MaybeMany::one(Node::Eager {
            tag: "sup".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Table(aligns) => MaybeMany::one(construct_table(&aligns, children)),
        Tag::TableCell => MaybeMany::one(Node::Eager {
            tag: "td".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::TableHead => MaybeMany::one(Node::Eager {
            tag: "thead".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::TableRow => MaybeMany::one(Node::Eager {
            tag: "tr".into(),
            attrs: Default::default(),
            children,
        }),
        Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        } => {
            let link_type = match link_type {
                LinkType::Autolink => super::super::LinkType::Autolink,
                LinkType::Collapsed
                | LinkType::Reference
                | LinkType::Shortcut
                | LinkType::Inline => super::super::LinkType::Normal,
                LinkType::CollapsedUnknown
                | LinkType::ShortcutUnknown
                | LinkType::ReferenceUnknown => super::super::LinkType::Broken,
                LinkType::Email => super::super::LinkType::Email,
                LinkType::WikiLink { .. } => super::super::LinkType::Wikilink,
            };
            let extracted = KeepRaw::Link {
                link_type,
                dest_url: dest_url.to_string(),
                title: title.to_string(),
                id: id.to_string(),
            };
            MaybeMany::one(Node::Lazy {
                keep: extracted,
                children,
            })
        }
    }
}

fn parse_element<'src>(parser: &mut ParserImpl<'src>) -> MaybeMany<Node<KeepRaw>> {
    let Some(event) = parser.next_event() else {
        return MaybeMany::none();
    };
    let raw = match event {
        Event::Text(text) => {
            if url::Url::parse(&text).is_ok() {
                Node::Lazy {
                    keep: KeepRaw::Link {
                        link_type: crate::field::markdown::LinkType::Autolink,
                        dest_url: text.to_string(),
                        title: text.to_string(),
                        id: Default::default(),
                    },
                    children: vec![Node::Text(text.into_string())],
                }
            } else {
                Node::Text(text.into_string())
            }
        }
        Event::Html(html) | Event::InlineHtml(html) => {
            return MaybeMany::many(raw_to_expanded(&html));
        }
        Event::Code(code) => Node::Eager {
            tag: "code".into(),
            attrs: Default::default(),
            children: vec![Node::Text(code.into_string())],
        },
        Event::DisplayMath(math) => {
            match katex::render_with_opts(&math, KATEX_DISPLAY_MATH_OPTS.as_ref()) {
                Ok(katex) => Node::Eager {
                    tag: "div".into(),
                    attrs: Default::default(),
                    children: raw_to_expanded(&katex),
                },
                Err(e) => {
                    warn!(%e, "failed to parse katex math");
                    Node::Lazy {
                        keep: KeepRaw::Codeblock {
                            meta: CodeblockMeta {
                                lang: Some("tex".into()),
                                attrs: Default::default(),
                            },
                        },
                        children: vec![Node::Text(math.to_string())],
                    }
                }
            }
        }
        Event::InlineMath(math) => {
            match katex::render_with_opts(&math, KATEX_INLINE_MATH_OPTS.as_ref()) {
                Ok(katex) => Node::Eager {
                    tag: "span".into(),
                    attrs: Default::default(),
                    children: raw_to_expanded(&katex),
                },
                Err(e) => {
                    warn!(%e, "failed to parse katex math");
                    Node::Eager {
                        tag: "span".into(),
                        attrs: Default::default(),
                        children: vec![Node::Text(math.into_string())],
                    }
                }
            }
        }
        Event::FootnoteReference(r) => Node::Lazy {
            keep: KeepRaw::FootnoteReference {
                id: r.into_string(),
            },
            children: Default::default(),
        },
        Event::HardBreak => Node::Eager {
            tag: "br".into(),
            attrs: Default::default(),
            children: vec![],
        },
        Event::Rule => Node::Eager {
            tag: "hr".into(),
            attrs: Default::default(),
            children: vec![],
        },
        Event::SoftBreak => Node::Eager {
            tag: "wbr".into(),
            attrs: Default::default(),
            children: vec![],
        },
        Event::TaskListMarker(marker) => Node::Eager {
            tag: "input".into(),
            attrs: indexmap! {
                "type".into() => "checkbox".into(),
                "disabled".into() => true.into(),
                "checked".into() => marker.into(),
            },
            children: vec![],
        },
        Event::End(_) => unreachable!(),
        Event::Start(tag) => return parse_spanned(parser, tag),
    };
    MaybeMany::one(raw)
}

pub fn parse(src: &str) -> super::RichTextDocumentRaw {
    use pulldown_cmark::Options;
    let options = Options::ENABLE_DEFINITION_LIST
        | Options::ENABLE_GFM
        | Options::ENABLE_MATH
        | Options::ENABLE_MATH
        | Options::ENABLE_FOOTNOTES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_STRIKETHROUGH;
    let mut parser = ParserImpl {
        lookahead: Default::default(),
        parser: pulldown_cmark::Parser::new_ext(src, options),
        footnotes: Default::default(),
    };
    let mut root = Vec::new();
    loop {
        let elements = parse_element(&mut parser);
        if elements.is_empty() {
            break;
        }
        root.extend(elements);
    }
    super::RichTextDocumentRaw {
        root,
        footnotes: parser.footnotes,
    }
}
