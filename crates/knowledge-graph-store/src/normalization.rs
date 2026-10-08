//! Versioned graph text normalization.

use crate::contracts::{
    Alias, AliasKey, AliasSetIdentity, ContractError, FieldCode, MAX_ALIAS_KEY_BYTES,
    MAX_CONCEPT_ALIASES, ValidatedAlias, ValidatedAliasSet, ValidationReason,
    validate_alias_display, validate_bounded_text,
};
use icu_casemap::CaseMapperBorrowed;
use icu_normalizer::ComposingNormalizerBorrowed;
use std::collections::BTreeSet;

/// Stable identifier for normalized alias keys persisted by the graph store.
pub const ALIAS_NORMALIZATION_VERSION: &str = "icu4x-2.3.0-nfkc-fold-v1";

/// Collapses Unicode whitespace, applies NFKC, collapses again, then performs
/// full non-Turkic case folding.
pub fn normalize_alias_key(display_text: &str) -> Result<AliasKey, ContractError> {
    validate_alias_display(display_text)?;

    let collapsed_before_nfkc = collapse_unicode_whitespace(display_text);
    let nfkc = ComposingNormalizerBorrowed::new_nfkc().normalize(&collapsed_before_nfkc);
    let collapsed_after_nfkc = collapse_unicode_whitespace(&nfkc);
    let normalized_key = CaseMapperBorrowed::new()
        .fold_string(&collapsed_after_nfkc)
        .into_owned();
    validate_bounded_text(&normalized_key, FieldCode::AliasKey, MAX_ALIAS_KEY_BYTES)?;

    Ok(AliasKey::from_normalized(normalized_key))
}

/// Validates one complete alias set and preserves each input display spelling.
pub fn normalize_alias_set(aliases: &[Alias]) -> Result<ValidatedAliasSet, ContractError> {
    if aliases.is_empty() {
        return Err(ContractError::invalid(
            FieldCode::AliasSet,
            ValidationReason::Empty,
        ));
    }
    if aliases.len() > MAX_CONCEPT_ALIASES {
        return Err(ContractError::invalid(
            FieldCode::AliasSet,
            ValidationReason::TooLong,
        ));
    }

    let mut normalized_aliases = Vec::with_capacity(aliases.len());
    let mut normalized_keys = BTreeSet::new();
    let mut preferred_key = None;

    for alias in aliases {
        let normalized_key = normalize_alias_key(&alias.display_text)?;
        if !normalized_keys.insert(normalized_key.clone()) {
            return Err(ContractError::invalid(
                FieldCode::AliasSet,
                ValidationReason::InvalidShape,
            ));
        }
        if alias.preferred {
            if preferred_key.is_some() {
                return Err(ContractError::invalid(
                    FieldCode::PreferredAlias,
                    ValidationReason::InvalidShape,
                ));
            }
            preferred_key = Some(normalized_key.clone());
        }
        normalized_aliases.push(ValidatedAlias {
            display_text: alias.display_text.clone(),
            normalized_key,
            preferred: alias.preferred,
        });
    }

    let preferred_key = preferred_key.ok_or_else(|| {
        ContractError::invalid(FieldCode::PreferredAlias, ValidationReason::InvalidShape)
    })?;
    let identity = AliasSetIdentity::new(normalized_keys, preferred_key);

    Ok(ValidatedAliasSet {
        aliases: normalized_aliases,
        identity,
    })
}

fn collapse_unicode_whitespace(value: &str) -> String {
    let mut collapsed = String::with_capacity(value.len());

    for (index, word) in value.split_whitespace().enumerate() {
        if index > 0 {
            collapsed.push(' ');
        }
        collapsed.push_str(word);
    }

    collapsed
}
