# Frozen pre-port owner declarations

Captured from the recorded sibling checkouts before their storage ports; no owner implementation is linked or executed by these tests. `seven_owners.json` is the complete literal expected declaration wire value, transcribed from these sources. Arrays preserve item/index/step order. No source declares search or TTL fields; their absence is pinned too. Default streams seal `never` and have no retention; connectors alone seals at idle 172800000 ms. MCP legacy secrets outside the owner prefix are grants, not declared items.

The new transport representation maps each old document `source` or stream/blob `source_prefix` to `StepDeclaration.source`. It maps each old step ID verbatim to `transform.operation`, its closure family to documents/stream/blob, and uses operation contract version 1. Collection/body and step versions remain 1 -> 2. Source prefixes retain trailing slashes exactly. These selector descriptors are new; all existing declaration meaning is retained.

| Repository | Recorded source commit | Source path | Source blob |
| --- | --- | --- | --- |
| tinyjuice | `7b9d1affcbee5462df3067ddc425dfb9996d7adb` | `crates/tinyjuice-bus/src/storage.rs` | `cbb0b8d2f84f746ae4fb0fd9679307387e6327ea` |
| tinyconnectors | `cb297ef1e4d08d7b011dd27bdd32a104c48d67ed` | `crates/tinyconnectors-bus/src/storage/schema.rs` | `966cbed7dce639eb74e8cbecffb71690fec8d02b` |
| tinyconnectors | `cb297ef1e4d08d7b011dd27bdd32a104c48d67ed` | `crates/tinyconnectors-bus/src/storage/names.rs` | `549c071ed48e48d819b966153b26986aea4f618f` |
| tinymcp | `c68c1fe32bdcec15bcf2ec4aa3e8d10c613d7b0d` | `crates/tinymcp-bus/src/storage/schema.rs` | `4aeab30ca8a05ca420b24db24864864c4fd827fe` |
| tinymcp | `c68c1fe32bdcec15bcf2ec4aa3e8d10c613d7b0d` | `crates/tinymcp-bus/src/storage/migrate.rs` | `27c4a39e8d1f3809526fdb6fbadb8d3f26b98e87` |
| tinychannels | `7ce043029dbcd4adf6db8ae2e37482ad4172e98b` | `crates/tinychannels-bus/src/storage/schema.rs` | `2f9bc9fab6b455a6f80ab1b31827b4a467be65a6` |
| tinychannels | `7ce043029dbcd4adf6db8ae2e37482ad4172e98b` | `crates/tinychannels-bus/src/storage/migrate.rs` | `91cefe9a7c2c6c28f0a9197acc3921984bba685e` |
| tinywallet | `7695a4ac7e90b01d530bfcb8142726a3c0142c88` | `crates/tinywallet-bus/src/storage/mod.rs` | `863f63e77aff8ba70b79a8e27da6e47f489df150` |
| tinymemes | `b3a298fcee3a515ff2262dc9ef3d5ba61924fe58` | `crates/tinymemes-bus/src/storage.rs` | `7dd6f2c732a7c304887c00fd7fed233b6212da65` |
| tinybox | `8cb2a90c6dfaed85341576dd9f03ba5a323eeefd` | `crates/tinybox-bus/src/storage/mod.rs` | `f8f0b7fc07a18da380705925c9489c0897dee083` |
