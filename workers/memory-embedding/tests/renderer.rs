use memory_embedding::{
    contracts::{EmbeddingError, EmbeddingWorkItem, MemoryKey, RenderFailure},
    renderer::render,
};

const FORMAT: &str = "total-recall.memory-embedding.v1";
const KEY_SENTINEL: &str = "renderer-key-sentinel";

fn work_item(title: &str, content: &str, concepts: &[&str]) -> EmbeddingWorkItem {
    EmbeddingWorkItem::new(
        MemoryKey::try_new(KEY_SENTINEL, "42").expect("test key should be valid"),
        title,
        content,
        concepts
            .iter()
            .map(|concept| (*concept).to_owned())
            .collect(),
    )
}

#[test]
fn render_snapshots_compact_fixed_field_json_with_escaped_unicode_text_and_concepts() {
    let title = " \t\ntitle-whitespace-sentinel Cafe\u{301} \"quoted\"\nbackslash: \\ \n\t";
    let content = "\t \ncontent-whitespace-sentinel Unicode: café, 東京, 🚀\tend\n\t ";
    let whitespace_concept = "\n\t concept-whitespace-sentinel \t\n";
    let item = work_item(
        title,
        content,
        &[
            "",
            whitespace_concept,
            "duplicate",
            "duplicate",
            "quote\"",
            "line\nbreak",
        ],
    );

    let input = render(&item, usize::MAX).expect("canonical rendering should succeed");

    assert_eq!(input.key(), item.key());
    assert_eq!(
        input.text(),
        r#"{"format":"total-recall.memory-embedding.v1","title":" \t\ntitle-whitespace-sentinel Café \"quoted\"\nbackslash: \\ \n\t","content":"\t \ncontent-whitespace-sentinel Unicode: café, 東京, 🚀\tend\n\t ","concepts":["","\n\t concept-whitespace-sentinel \t\n","duplicate","duplicate","quote\"","line\nbreak"]}"#
    );
    assert_eq!(input.text().matches(FORMAT).count(), 1);

    let parsed = serde_json::from_str::<serde_json::Value>(input.text())
        .expect("rendered input must be valid JSON");
    assert_eq!(
        parsed,
        serde_json::json!({
            "format": FORMAT,
            "title": title,
            "content": content,
            "concepts": [
                "",
                whitespace_concept,
                "duplicate",
                "duplicate",
                "quote\"",
                "line\nbreak",
            ],
        })
    );
    assert_eq!(parsed["title"].as_str(), Some(title));
    assert_eq!(parsed["content"].as_str(), Some(content));
    assert_eq!(parsed["concepts"][1].as_str(), Some(whitespace_concept));
}

#[test]
fn render_excludes_memory_keys_and_provenance() {
    let item = work_item("title-sentinel", "content-sentinel", &["concept-sentinel"]);

    let input = render(&item, usize::MAX).expect("canonical rendering should succeed");

    for excluded in [
        KEY_SENTINEL,
        "\"id\"",
        "\"version\"",
        "\"type\"",
        "\"created_at\"",
        "\"updated_at\"",
        "\"files\"",
        "\"session_ids\"",
        "\"source_observation_ids\"",
        "\"provider\"",
        "\"model\"",
        "\"vector\"",
    ] {
        assert!(
            !input.text().contains(excluded),
            "canonical input included excluded value {excluded}"
        );
    }
}

#[test]
fn render_enforces_the_post_serialization_utf8_byte_boundary_without_leaking_content() {
    let item = work_item(
        "title-sentinel-é",
        "content-sentinel-東京🚀",
        &["", "concept-sentinel-é", "concept-sentinel-é"],
    );
    let unconstrained = render(&item, usize::MAX).expect("canonical rendering should succeed");
    let limit = unconstrained.text().len() - 1;

    assert_eq!(unconstrained.text().len(), limit + 1);
    assert_eq!(
        render(&item, limit + 1).expect("the exact byte boundary should be accepted"),
        unconstrained
    );

    let error = render(&item, limit).expect_err("one byte above the limit must fail");

    assert_eq!(error, EmbeddingError::render(RenderFailure::InputTooLarge));
    assert_eq!(error.to_string(), "render:input_too_large");
    for protected in [
        KEY_SENTINEL,
        "title-sentinel",
        "content-sentinel",
        "concept-sentinel",
    ] {
        assert!(
            !error.to_string().contains(protected),
            "render error leaked protected content {protected}"
        );
        assert!(
            !format!("{error:?}").contains(protected),
            "render error debug output leaked protected content {protected}"
        );
    }
}
