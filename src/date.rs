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

//! Date definition.

use crate::common::*;
use crate::error::DateTimeError;
use crate::interval::Interval;
use crate::parse::{decode_date_time, parse_date_time, timestamp2time};
use crate::time::Time;
use crate::timestamp::{Timestamp, DATE_TIME_BEGIN_VAL, DATE_TIME_END_VAL};
use crate::token::*;
use crate::{DateOrder, DateStyle, DateTime, DateUnit, FieldType};
use std::convert::TryFrom;
use std::mem::MaybeUninit;

// Range limits for dates and timestamps.
//
// We have traditionally allowed Julian day zero as a valid datetime value,
// so that is the lower bound for both dates and timestamps.
//
// The upper limit for dates is 5874897-12-31, which is a bit less than what
// the Julian-date code can allow.  For timestamps, the upper limit is
// 294276-12-31.  The int64 overflow limit would be a few days later; again,
// leaving some slop avoids worries about corner-case overflow, and provides
// a simpler user-visible definition.
// First allowed date, and first disallowed date, in Julian-date form.
const DATETIME_MIN_JULIAN: i32 = 0;
const DATE_END_JULIAN: i32 = 2_147_483_494; // == date2j(JULIAN_MAXYEAR, 1, 1)

// Range-check a date (given in Postgres, not Julian, numbering)
#[inline]
fn is_valid_date(d: i32) -> bool {
    (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= d
        && d < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE)
}

/// Date (no time of day).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Date(i32);

impl Date {
    /// Construct a `Date` value.
    #[inline]
    pub(crate) const fn new(v: i32) -> Self {
        Self(v)
    }

    ///  Gets value of `Date`.
    #[inline]
    pub(crate) const fn value(self) -> i32 {
        self.0
    }

    /// Constructs `Date` from year, month and day.
    #[inline]
    pub fn try_from_ymd(y: i32, m: i32, d: i32) -> Result<Self, DateTimeError> {
        let mut tm: PgTime = PgTime::new();
        let mut bc = false;
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
        // Handle negative years as BC
        if tm.year < 0 {
            bc = true;
            tm.year = -tm.year;
        }

        tm.validate_date(DTK_DATE_M, false, false, bc)?;

        // Prevent overflow in Julian-day routines.
        if !is_valid_julian(tm.year, tm.mon, tm.mday) {
            return Err(DateTimeError::overflow());
        }

        let date = date2julian(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;

        // Now check for just-out-of-range dates
        if !is_valid_date(date) {
            Err(DateTimeError::overflow())
        } else {
            Ok(Date::new(date))
        }
    }

    #[inline]
    pub(crate) const fn is_begin(self) -> bool {
        self.0 == std::i32::MIN
    }

    #[inline]
    pub(crate) const fn is_end(self) -> bool {
        self.0 == std::i32::MAX
    }

    #[inline]
    fn is_valid(self) -> bool {
        is_valid_date(self.0)
    }

    /// `Date` adds days.
    #[inline]
    pub fn add_days(self, days: i32) -> Result<Self, DateTimeError> {
        if self.is_infinite() {
            return Ok(self);
        }

        let result = match self.0.checked_add(days) {
            Some(v) => v,
            None => return Err(DateTimeError::overflow()),
        };

        let date_ret = Date::new(result);
        // Check for integer overflow and out-of-allowed-range.
        if date_ret.is_valid() {
            Ok(date_ret)
        } else {
            Err(DateTimeError::overflow())
        }
    }

    /// `Date` add `Interval`.
    #[inline]
    pub fn add_interval(self, span: Interval) -> Result<Timestamp, DateTimeError> {
        let date_stamp: Timestamp = TryFrom::try_from(self)?;
        date_stamp.add_interval(span)
    }

    /// `Date` adds `Time`.
    #[inline]
    pub fn add_time(self, time: Time) -> Result<Timestamp, DateTimeError> {
        let mut timestamp: Timestamp = TryFrom::try_from(self)?;
        if timestamp.is_finite() {
            timestamp = timestamp.add_time(time)?;
            if !timestamp.is_valid() {
                return Err(DateTimeError::overflow());
            }
        }

        Ok(timestamp)
    }

    /// `Date` subtracts `Date`.
    #[inline]
    pub fn sub_date(self, date: Date) -> Result<i32, DateTimeError> {
        if self.is_infinite() || date.is_infinite() {
            Err(DateTimeError::invalid(format!(
                "sub invalid date, left: {:?} right: {:?}",
                self, date
            )))
        } else {
            Ok(self.value() - date.value())
        }
    }

    /// `Date` subtracts days.
    #[inline]
    pub fn sub_days(self, days: i32) -> Result<Date, DateTimeError> {
        if self.is_infinite() {
            return Ok(self); // can't change infinity
        }

        let result = self.value().checked_sub(days);
        // Check for integer overflow and out-of-allowed-range.
        match result {
            Some(d) => {
                if is_valid_date(d) {
                    Ok(Date::new(d))
                } else {
                    Err(DateTimeError::invalid(format!(
                        "date sub result: {:?} is invalid",
                        d
                    )))
                }
            }
            None => Err(DateTimeError::overflow()),
        }
    }

    /// `Date` subtracts `Interval`.
    #[inline]
    pub fn sub_interval(self, span: Interval) -> Result<Timestamp, DateTimeError> {
        let date_stamp: Timestamp = TryFrom::try_from(self)?;
        date_stamp.sub_interval(span)
    }

    /// Parses `Date` string.
    #[allow(clippy::uninit_assumed_init)]
    pub fn try_from_str(s: &str, date_order: DateOrder) -> Result<Self, DateTimeError> {
        let s = s.as_bytes();
        let mut fields: [&[u8]; MAX_DATE_FIELDS] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut work_buf: [u8; MAX_DATE_LEN] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut f_types: [TokenField; MAX_DATE_FIELDS] =
            unsafe { MaybeUninit::uninit().assume_init() };

        let field_num =
            parse_date_time(s, &mut work_buf, &mut fields, &mut f_types, MAX_DATE_FIELDS)?;
        let mut tm = PgTime::new();
        let (_, d_type, _) = decode_date_time(
            &fields[0..field_num],
            &f_types[0..field_num],
            &mut tm,
            &mut None,
            date_order,
        )?;

        match d_type {
            TokenField::Date => {}
            //DTK_EPOCH => todo!(), // Timezone relate
            TokenField::Late => return Ok(Date::new(std::i32::MAX)),
            TokenField::Early => return Ok(Date::new(std::i32::MIN)),
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "date type: {:?} is invalid",
                    d_type
                )))
            }
        }

        // Prevent overflow in Julian-day routines.
        if !is_valid_julian(tm.year, tm.mon, tm.mday) {
            return Err(DateTimeError::invalid(format!(
                "date :{}-{}-{}) is invalid julian date",
                tm.year, tm.mon, tm.mday
            )));
        }

        let date = date2julian(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;

        // Now check for just-out-of-range dates .
        if !is_valid_date(date) {
            return Err(DateTimeError::invalid(format!(
                "date :{}-{}-{}) is invalid julian date",
                tm.year, tm.mon, tm.mday
            )));
        }

        Ok(Date::new(date))
    }

    /// Convert `Date` to string.
    #[inline]
    pub fn format(
        self,
        date_style: DateStyle,
        date_order: DateOrder,
    ) -> Result<String, DateTimeError> {
        if self.is_infinite() {
            format_special_date(self)
        } else {
            let mut buf = Vec::with_capacity(128);
            unsafe { buf.set_len(128) };
            let mut tm: PgTime = PgTime::new();

            let (y, m, d) = julian2date(self.value() + POSTGRES_EPOCH_JDATE);
            tm.set_ymd(y, m, d);
            let len = format_date(&tm, buf.as_mut_slice(), date_style, date_order)?;
            unsafe {
                buf.set_len(len);
                Ok(String::from_utf8_unchecked(buf))
            }
        }
    }
}

/// Convert reserved date values to string.
#[inline]
fn format_special_date(date: Date) -> Result<String, DateTimeError> {
    if date.is_begin() {
        let ret = unsafe { String::from_utf8_unchecked(EARLY.into()) };
        Ok(ret)
    } else if date.is_end() {
        let ret = unsafe { String::from_utf8_unchecked(LATE.into()) };
        Ok(ret)
    } else {
        Err(DateTimeError::invalid(format!(
            "date: {:?} is not valid",
            date
        )))
    }
}

/// Encode date as local time.
#[inline]
fn format_date(
    tm: &PgTime,
    s: &mut [u8],
    style: DateStyle,
    date_order: DateOrder,
) -> Result<usize, DateTimeError> {
    debug_assert!(tm.mon >= 1 && tm.mon <= MONTHS_PER_YEAR);
    let mut offset = 0;

    match style {
        DateStyle::ISO | DateStyle::XSD => {
            // compatible with ISO date formats
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            offset = pg_ltostr_zeropad(&mut s[offset..], year, 4);
            offset = set_one_byte(s, offset, b'-');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
            offset = set_one_byte(s, offset, b'-');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
        }
        DateStyle::SQL => {
            // compatible with Oracle/Ingres date formats.
            if date_order == DateOrder::DMY {
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
                offset = set_one_byte(s, offset, b'/');
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
            } else {
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
                offset = set_one_byte(s, offset, b'/');
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
            }
            offset = set_one_byte(s, offset, b'/');
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            offset += pg_ltostr_zeropad(&mut s[offset..], year, 4);
        }

        DateStyle::German => {
            // German-style date format.
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
            offset = set_one_byte(s, offset, b'.');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
            offset = set_one_byte(s, offset, b'.');
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            offset += pg_ltostr_zeropad(&mut s[offset..], year, 4);
        }

        _ => {
            // traditional date-only style for Postgres.
            if date_order == DateOrder::DMY {
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
                offset = set_one_byte(s, offset, b'-');
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
            } else {
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
                offset = set_one_byte(s, offset, b'.');
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
            }
            offset = set_one_byte(s, offset, b'.');
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            offset += pg_ltostr_zeropad(&mut s[offset..], year, 4);
        }
    }

    if tm.year <= 0 {
        offset = copy_slice(s, offset, b" BC");
    }

    Ok(offset)
}

/// Converts `Date` to `i32` type.
impl From<Date> for i32 {
    #[inline]
    fn from(d: Date) -> Self {
        d.value()
    }
}

/// Converts `i32` to `Date` type.
impl From<i32> for Date {
    #[inline]
    fn from(v: i32) -> Self {
        Date::new(v)
    }
}

/// Converts `timestamp` to `date`.
impl TryFrom<Timestamp> for Date {
    type Error = DateTimeError;

    #[inline]
    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        if value == DATE_TIME_BEGIN_VAL {
            Ok(DATE_BEGIN_VAL)
        } else if value == DATE_TIME_END_VAL {
            Ok(DATE_END_VAL)
        } else {
            let mut tm: PgTime = PgTime::new();
            let mut fsec = 0;
            timestamp2time(value.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;

            let result = date2julian(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;
            Ok(Date::new(result))
        }
    }
}

/// Infinity and minus infinity must be the max and min values of Date.
pub const DATE_BEGIN_VAL: Date = Date::new(std::i32::MIN);
pub const DATE_END_VAL: Date = Date::new(std::i32::MAX);

impl DateTime for Date {
    /// Extracts specified field from `Date`.
    #[inline]
    fn date_part(&self, ty: FieldType, unit: DateUnit) -> Result<f64, DateTimeError> {
        let timestamp: Timestamp = TryFrom::try_from(*self)?;
        timestamp.date_part(ty, unit)
    }

    /// Checks whether `Date` is finite.
    #[inline]
    fn is_finite(&self) -> bool {
        !self.is_begin() && !self.is_end()
    }

    /// Truncates `Date` to specified units.
    #[inline]
    fn truncate(&self, ty: FieldType, unit: DateUnit) -> Result<Self, DateTimeError> {
        let timestamp: Timestamp = TryFrom::try_from(*self)?;
        let tp = timestamp.truncate(ty, unit)?;
        TryFrom::try_from(tp)
    }
}

#[cfg(test)]
mod tests {
    use crate::date::Date;
    use crate::error::DateTimeError;
    use crate::token::MAX_DATE_FIELDS;
    use crate::{DateOrder, DateStyle, DateTime, DateUnit, FieldType};

    #[test]
    fn tets_date_from_str() -> Result<(), DateTimeError> {
        let ret = Date::try_from_str("y2.3", DateOrder::MDY);
        assert!(ret.is_err());
        let ret = Date::try_from_str("  2003-04-12 dow 123456-2", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("  2003-04-12 t 123456/2", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("  2003-04-12 t 123456-2", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("  2003-04-12 04:05:06 t 123456-2", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("  2003-04-12 04:05:06 America/New_Yor", DateOrder::MDY);
        assert!(ret.is_err());
        let ret = Date::try_from_str("  2003-04-12 04:05:06 America/New_Yor", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("1999-7--7-08", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("1999-at-08", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("1999-jan-jan", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("1999-Jqn-08", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("20000000000000000000", DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Date::try_from_str("j1999-3-4:3:5:5", DateOrder::MDY);
        assert!(ret.is_err());
        let mut str = Vec::with_capacity(1);
        str.push(5);
        let s = String::from_utf8(str);
        assert!(s.is_ok());
        let date = Date::try_from_str(s.unwrap().as_str(), DateOrder::MDY);
        assert!(date.is_err());

        let mut str = Vec::with_capacity((MAX_DATE_FIELDS + 1) * 2);
        for i in 0..(MAX_DATE_FIELDS + 1) * 2 {
            if i % 2 == 0 {
                str.push(b'7');
            } else {
                str.push(b' ');
            }
        }

        let s = String::from_utf8(str);
        assert!(s.is_ok());
        let date = Date::try_from_str(s.unwrap().as_str(), DateOrder::MDY);
        assert!(date.is_err());

        let date = Date::try_from_str("", DateOrder::MDY);
        assert!(date.is_err());

        let date = Date::try_from_str("  +", DateOrder::MDY);
        assert!(date.is_err());

        let date = Date::try_from_str("-/", DateOrder::MDY);
        assert!(date.is_err());

        let date = Date::try_from_str("1988/", DateOrder::MDY);
        assert!(date.is_err());

        let date: Date = Date::try_from_str("  990619  ", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-06-19".to_string());

        let date: Date = Date::try_from_str("1999-03-31 123456", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-03-31".to_string());

        let date: Date = Date::try_from_str("1999-03-31 1234", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-03-31".to_string());

        let date: Date = Date::try_from_str("-infinity", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "-infinity");

        let date: Date = Date::try_from_str("infinity", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "infinity");

        let date: Date = Date::try_from_str("1999-01-08", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let s = date.format(DateStyle::Postgres, DateOrder::YMD)?;
        assert_eq!(s, "01.08.1999".to_string());

        let s = date.format(DateStyle::SQL, DateOrder::YMD)?;
        assert_eq!(s, "01/08/1999".to_string());

        let s = date.format(DateStyle::German, DateOrder::YMD)?;
        assert_eq!(s, "08.01.1999");

        let s = date.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("January 8, 1999", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("1/8/1999", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("1/18/1999", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-18".to_string());

        let date: Date = Date::try_from_str("01/02/03", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2003-01-02".to_string());

        let date: Date = Date::try_from_str("1999-Jan-08", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("Jan-08-1999", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("08-Jan-1999", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("99-Jan-08", DateOrder::YMD)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let ret = Date::try_from_str("99-Jan-08", DateOrder::DMY);
        assert!(ret.is_err());
        let ret = Date::try_from_str("99-Jan-08", DateOrder::MDY);
        assert!(ret.is_err());

        let date: Date = Date::try_from_str("08-Jan-99", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("08-Jan-99", DateOrder::DMY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let ret = Date::try_from_str("08-Jan-99", DateOrder::YMD);
        assert!(ret.is_err());

        let date: Date = Date::try_from_str("Jan-08-99", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("Jan-08-99", DateOrder::DMY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let ret = Date::try_from_str("Jan-08-99", DateOrder::YMD);
        assert!(ret.is_err());

        let date: Date = Date::try_from_str("19990108", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("990108", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("1999.008", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("J2451187", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08".to_string());

        let date: Date = Date::try_from_str("January 8, 99 BC", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "0099-01-08 BC".to_string());

        let ret = Date::try_from_str("-*u1981", DateOrder::MDY);
        assert!(ret.is_err());

        let date = Date::try_from_str("test 8, 1999", DateOrder::MDY);
        assert!(date.is_err());
        Ok(())
    }

    #[test]
    fn test_date_range() -> Result<(), DateTimeError> {
        let date: Date = Date::try_from_str("January 1, 4713 BC", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "4713-01-01 BC".to_string());

        let date: Date = Date::try_from_str("5874897-12-31", DateOrder::MDY)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "5874897-12-31".to_string());

        let date = Date::try_from_str("January 1, 4714 BC", DateOrder::MDY);
        assert!(date.is_err());

        let date = Date::try_from_str("5874898-2-1", DateOrder::MDY);
        assert!(date.is_err());

        Ok(())
    }

    #[test]
    fn test_try_from_ymd() -> Result<(), DateTimeError> {
        let date = Date::try_from_ymd(2020, 12, 23)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2020-12-23");

        let date = Date::try_from_ymd(-2020, 12, 23)?;
        let s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2020-12-23 BC");

        let ret = Date::try_from_ymd(std::i32::MAX, std::i32::MAX, std::i32::MAX);
        assert!(ret.is_err());

        let ret = Date::try_from_ymd(5874899, 0, 0);
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_date_date_part() -> Result<(), DateTimeError> {
        let date = Date::try_from_str("2116-9-10", DateOrder::YMD)?;
        let ret = date.date_part(FieldType::Unit, DateUnit::Millennium)?;
        assert_eq!(ret, 3.0);

        let ret = date.date_part(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret, 22.0);

        let ret = date.date_part(FieldType::Unit, DateUnit::Decade)?;
        assert_eq!(ret, 211.0);

        let ret = date.date_part(FieldType::Unit, DateUnit::Year)?;
        assert_eq!(ret, 2116.0);
        Ok(())
    }

    #[test]
    fn test_date_trunc() -> Result<(), DateTimeError> {
        let date = Date::try_from_str("2116-9-10", DateOrder::YMD)?;
        let ret = date.truncate(FieldType::Unit, DateUnit::Millennium)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2001-01-01");

        let ret = date.truncate(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2101-01-01");

        let ret = date.truncate(FieldType::Unit, DateUnit::Decade)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2110-01-01");

        let ret = date.truncate(FieldType::Unit, DateUnit::Year)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2116-01-01");

        let ret = date.truncate(FieldType::Unit, DateUnit::Quarter)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2116-07-01");

        let ret = date.truncate(FieldType::Unit, DateUnit::Month)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2116-09-01");

        let ret = date.truncate(FieldType::Unit, DateUnit::Day)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2116-09-10");

        Ok(())
    }

    #[test]
    fn test_date_finite() -> Result<(), DateTimeError> {
        let date = Date::try_from_str("2116-9-10", DateOrder::YMD)?;
        assert!(date.is_finite());
        let date = Date::new(std::i32::MIN);
        assert!(date.is_infinite());
        Ok(())
    }

    #[test]
    fn test_date_to_str() -> Result<(), DateTimeError> {
        let date = Date::try_from_ymd(2000, 10, 1)?;
        let date_s = date.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!("2000-10-01".to_string(), date_s);
        Ok(())
    }
}
