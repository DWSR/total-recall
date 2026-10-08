use std::fmt::Debug;

use knowledge_graph_store::{
    contracts::{
        Alias, AliasQuery, ContractError, FieldCode, MAX_ALIAS_DISPLAY_BYTES, MAX_ALIAS_KEY_BYTES,
        MAX_CONCEPT_ALIASES, ValidationReason,
    },
    normalization::{ALIAS_NORMALIZATION_VERSION, normalize_alias_key, normalize_alias_set},
};

fn alias(display_text: &str, preferred: bool) -> Alias {
    Alias {
        display_text: display_text.to_owned(),
        preferred,
    }
}

fn assert_invalid<T: Debug>(
    result: Result<T, ContractError>,
    expected_field: FieldCode,
    expected_reason: ValidationReason,
) -> ContractError {
    let error = result.expect_err("invalid alias input should be rejected");
    assert_eq!(error.field(), expected_field);
    assert_eq!(error.reason(), expected_reason);
    error
}

#[test]
fn alias_normalization_version_is_pinned() {
    assert_eq!(ALIAS_NORMALIZATION_VERSION, "icu4x-2.3.0-nfkc-fold-v1");
}

#[test]
fn collapses_multilingual_unicode_whitespace() {
    let key = normalize_alias_key(
        "\u{3000}Alpha\u{00a0}\u{2003}\u{0085}Beta\u{2028}\u{2029}Gamma\u{2009}\u{202f}Delta\u{3000}",
    )
    .expect("Unicode whitespace aliases should normalize");

    assert_eq!(key.as_str(), "alpha beta gamma delta");
}

#[test]
fn applies_nfkc_compatibility_mappings_and_collapses_new_whitespace() {
    assert_eq!(
        normalize_alias_key("ＡＢＣ ﬁ ①")
            .expect("compatibility forms should normalize")
            .as_str(),
        "abc fi 1"
    );
    assert_eq!(
        normalize_alias_key("one \u{00a8} two")
            .expect("NFKC-introduced whitespace should collapse")
            .as_str(),
        "one \u{0308} two"
    );
}

#[test]
fn uses_full_non_turkic_case_folding_with_expansions() {
    assert_eq!(
        normalize_alias_key("Straße ẞ Σ ς İ I ı")
            .expect("full case folding should normalize")
            .as_str(),
        "strasse ss σ σ i\u{0307} i ı"
    );
}

#[test]
fn rejects_aliases_whose_normalized_key_is_empty() {
    let error = assert_invalid(
        normalize_alias_key("\u{00a0}\u{3000}"),
        FieldCode::AliasKey,
        ValidationReason::Empty,
    );

    let query_error = assert_invalid(
        AliasQuery::try_from("\u{00a0}\u{3000}".to_owned()),
        FieldCode::AliasKey,
        ValidationReason::Empty,
    );
    assert!(!error.to_string().contains('\u{00a0}'));
    assert!(!format!("{query_error:?}").contains('\u{00a0}'));
}

#[test]
fn validates_display_and_normalized_key_utf8_byte_bounds() {
    assert_eq!(MAX_ALIAS_DISPLAY_BYTES, 512);
    assert_eq!(MAX_ALIAS_KEY_BYTES, 2_048);

    let display_at_bound = "x".repeat(MAX_ALIAS_DISPLAY_BYTES);
    assert_eq!(
        normalize_alias_key(&display_at_bound)
            .expect("512 display bytes should be accepted")
            .as_str(),
        display_at_bound
    );
    assert_invalid(
        normalize_alias_key(&"x".repeat(MAX_ALIAS_DISPLAY_BYTES + 1)),
        FieldCode::Alias,
        ValidationReason::TooLong,
    );

    // U+FDFA has the independent NFKC mapping "صلى الله عليه وسلم" (33 UTF-8 bytes).
    // Sixty-one mappings plus 35 ASCII bytes make exactly 2,048 key bytes.
    let expansion = "\u{fdfa}".repeat(61);
    let exact_bound = format!("{expansion}{}", "a".repeat(2_048 - 61 * 33));
    assert_eq!(exact_bound.len(), 218);
    assert_eq!(
        normalize_alias_key(&exact_bound)
            .expect("2,048 normalized key bytes should be accepted")
            .as_str()
            .len(),
        MAX_ALIAS_KEY_BYTES
    );

    let over_bound = format!("{expansion}{}", "a".repeat(2_049 - 61 * 33));
    assert_eq!(over_bound.len(), 219);
    assert_invalid(
        normalize_alias_key(&over_bound),
        FieldCode::AliasKey,
        ValidationReason::TooLong,
    );
}

#[test]
fn preserves_display_text_and_compares_normalized_alias_sets_by_identity() {
    let original_aliases = [alias("  Straße  ", true), alias("Ａlpha", false)];
    let equivalent_spellings = [alias("alpha", false), alias("STRASSE", true)];
    let different_preferred = [alias("STRASSE", false), alias("ALPHA", true)];
    let overlapping_but_different_set = [alias("straße", true), alias("beta", false)];

    let original = normalize_alias_set(&original_aliases).expect("valid alias set");
    let equivalent = normalize_alias_set(&equivalent_spellings).expect("equivalent alias set");
    let different_preferred =
        normalize_alias_set(&different_preferred).expect("valid preferred alias set");
    let overlapping =
        normalize_alias_set(&overlapping_but_different_set).expect("valid overlapping alias set");

    assert!(original == equivalent);
    assert!(original.identity() == equivalent.identity());
    assert!(original != different_preferred);
    assert!(original.identity() != different_preferred.identity());
    assert!(original.identity() != overlapping.identity());

    assert_eq!(original.aliases()[0].display_text(), "  Straße  ");
    assert_eq!(original.aliases()[0].normalized_key().as_str(), "strasse");
    assert!(original.aliases()[0].is_preferred());
    assert_eq!(
        original
            .identity()
            .normalized_keys()
            .map(|key| key.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "strasse"]
    );
    assert_eq!(original.identity().preferred_key().as_str(), "strasse");
}

#[test]
fn rejects_duplicate_normalized_keys_without_disclosing_alias_text() {
    let protected_alias = "protected-alias-ß";
    let aliases = [
        alias(protected_alias, true),
        alias("PROTECTED-ALIAS-SS", false),
    ];
    let error = assert_invalid(
        normalize_alias_set(&aliases),
        FieldCode::AliasSet,
        ValidationReason::InvalidShape,
    );

    assert!(!error.to_string().contains(protected_alias));
    assert!(!format!("{error:?}").contains(protected_alias));
}

#[test]
fn enforces_alias_count_and_exactly_one_preferred_alias() {
    assert_invalid(
        normalize_alias_set(&[]),
        FieldCode::AliasSet,
        ValidationReason::Empty,
    );

    let one_alias = [alias("one", true)];
    assert_eq!(
        normalize_alias_set(&one_alias)
            .expect("one preferred alias is valid")
            .aliases()
            .len(),
        1
    );

    let no_preferred = [alias("one", false)];
    assert_invalid(
        normalize_alias_set(&no_preferred),
        FieldCode::PreferredAlias,
        ValidationReason::InvalidShape,
    );

    let multiple_preferred = [alias("one", true), alias("two", true)];
    assert_invalid(
        normalize_alias_set(&multiple_preferred),
        FieldCode::PreferredAlias,
        ValidationReason::InvalidShape,
    );

    let at_limit = (0..MAX_CONCEPT_ALIASES)
        .map(|index| alias(&format!("alias-{index}"), index == 0))
        .collect::<Vec<_>>();
    assert_eq!(
        normalize_alias_set(&at_limit)
            .expect("64 unique aliases should be accepted")
            .aliases()
            .len(),
        MAX_CONCEPT_ALIASES
    );

    let over_limit = (0..=MAX_CONCEPT_ALIASES)
        .map(|index| alias(&format!("alias-{index}"), index == 0))
        .collect::<Vec<_>>();
    assert_invalid(
        normalize_alias_set(&over_limit),
        FieldCode::AliasSet,
        ValidationReason::TooLong,
    );
}

#[test]
fn alias_errors_and_key_debug_output_do_not_disclose_text() {
    let protected_alias = "protected-normalized-alias";
    let invalid = normalize_alias_key(&format!("{protected_alias}\0"));
    let error = assert_invalid(invalid, FieldCode::Alias, ValidationReason::ContainsNul);
    assert!(!error.to_string().contains(protected_alias));
    assert!(!format!("{error:?}").contains(protected_alias));

    let key = normalize_alias_key(protected_alias).expect("valid alias key");
    assert!(!format!("{key:?}").contains(protected_alias));
}
