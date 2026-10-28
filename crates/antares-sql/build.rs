// SPDX-License-Identifier: EUPL-1.2
// `sqlx::migrate!` embeds migrations/ at compile time; without this a change
// to a migration file alone leaves the old text compiled in.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
