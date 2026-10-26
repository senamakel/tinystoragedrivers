# tinystoragedrivers-fence public item destinations

Source symbols below include public definitions, associated methods, public fields, and re-export declarations. Line numbers refer to the audited epic source revision `ae2fc08`. A name repeated through a re-export is intentionally repeated: both public paths are API.

| Source | Public item(s) | v2 destination |
| --- | --- | --- |
| `src/lib.rs` | `LocalLeases`, `LocalLeaseGuard`, `is_pid_alive` | Concrete local lockfile helper only; no generic lease port or distributed fencing API is exported. |
| `src/lib.rs` | `LocalLeases::new`, `acquire`, `acquire_for_read` | `tinystoragedrivers-fence`: lock acquisition, stale/dead PID and same-process leak recovery, retrying release, and a lock-free read result only when lock creation fails on a full/read-only filesystem. |
