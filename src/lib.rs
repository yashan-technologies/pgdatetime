// Copyright 2020 CoD Technologies Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL date/time types written in Rust, compatible with PostgreSQL's date/time types.
//!
//! ## Optional features
//!
//! ### `serde`
//!
//! When this optional dependency is enabled, `Date`, `Time`, `Timestamp` and `Interval` implement the `serde::Serialize` and
//! `serde::Deserialize` traits.

mod common;
mod date;
mod error;
mod interval;
mod parse;
mod time;
mod timestamp;
mod timezone;
mod token;

#[cfg(feature = "serde")]
mod serialize;

pub use crate::date::Date;
pub use crate::error::DateTimeError;
pub use crate::interval::Interval;
pub use crate::time::Time;
pub use crate::timestamp::Timestamp;

/// Date unit.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DateUnit {
    Delta,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    Decade,
    Century,
    MilliSec,
    MicroSec,
    JULIAN,
    Dow,
    IsoDow,
    Doy,
    Tz,
    TzMinute,
    TzHour,
    Millennium,
    IsoYear,
    Epoch,
}

/// General trait for all date time types.
pub trait DateTime: Sized {
    /// Extracts specified field from date time.
    fn date_part(&self, ty: FieldType, unit: DateUnit) -> Result<f64, DateTimeError>;
    /// Checks whether date time is finite.
    fn is_finite(&self) -> bool;
    /// Checks whether date time is infinite.
    #[inline]
    fn is_infinite(&self) -> bool {
        !self.is_finite()
    }
    /// Truncates date type to specified units.
    fn truncate(&self, ty: FieldType, unit: DateUnit) -> Result<Self, DateTimeError>;
}

/// DateOrder defines the field order to be assumed when reading an
/// ambiguous date (anything not in YYYY-MM-DD format, with a four-digit
/// year field first, is taken to be ambiguous).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DateOrder {
    /// YMD specifies field order yy-mm-dd.
    YMD,
    /// DMY specifies field order dd-mm-yy ("European" convention).
    DMY,
    /// MDY specifies field order mm-dd-yy ("US" convention).
    MDY,
}

/// DateStyle defines the output formatting choice for date/time types.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DateStyle {
    /// Postgres specifies traditional Postgres format.
    Postgres,
    /// ISO specifies ISO-compliant format.
    ISO,
    /// SQL specifies Oracle/Ingres-compliant format.
    SQL,
    /// German specifies German-style dd.mm/yyyy.
    German,
    /// XSD Compatible with Iso.
    XSD,
}

/// Interval style.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum IntervalStyle {
    /// Postgres Like Postgres < 8.4 when DateStyle = 'iso'.
    Postgres,
    /// PostgresVerbose Like Postgres < 8.4 when DateStyle != 'iso'.
    PostgresVerbose,
    /// SQLStandard SQL standard interval literals.
    SQLStandard,
    /// ISO8601 ISO-8601-basic formatted intervals.
    ISO8601,
}

/// Field type.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FieldType {
    /// Unit type.
    Unit,
    /// Epoch type.
    Epoch,
}
