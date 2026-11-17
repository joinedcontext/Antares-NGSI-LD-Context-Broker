// SPDX-License-Identifier: EUPL-1.2
//! 4.6.2: an Attribute name the body grammar accepts is addressable on the
//! path-based endpoints too (the URL check follows the same grammar).
#[test]
fn unicode_letter_name_accepted_on_path_like_in_body() {
    assert!(ngsild_jsonld::valid_name("température"), "body grammar");
    assert!(antares_model::check_attr_name("a/../b").is_err());
    assert!(
        antares_model::check_attr_name("température").is_ok(),
        "path check refuses a name the body grammar accepted"
    );
}
