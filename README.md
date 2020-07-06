# pgdatetime

[![Apache-2.0 licensed](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Crate](https://img.shields.io/crates/v/pgdatetime.svg)](https://crates.io/crates/pgdatetime)
[![API](https://docs.rs/pgdatetime/badge.svg)](https://docs.rs/pgdatetime)

SQL date/time types written in Rust, compatible with PostgreSQL's date/time types.

See also: [Date/Time Types](https://www.postgresql.org/docs/current/datatype-datetime.html)

## Optional features

### `serde`

When this optional dependency is enabled, `Date`, `Time`, `Timestamp` and `Interval` implement the `serde::Serialize` and `serde::Deserialize` traits.

## License

This project is licensed under the Apache-2.0 license ([LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `pgdatetime` by you, shall be licensed as Apache-2.0, without any additional
terms or conditions.
