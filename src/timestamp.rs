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

//! Timestamp definition.

use crate::common::*;
use crate::date::{DATE_BEGIN_VAL, DATE_END_VAL};
use crate::error::DateTimeError;
use crate::interval::Interval;
use crate::parse::{decode_date_time, parse_date_time, timestamp2time};
use crate::time::Time;
use crate::token::*;
use crate::{Date, DateOrder, DateStyle, DateTime, DateUnit, FieldType};
use std::convert::TryFrom;
use std::mem::MaybeUninit;

// Timestamp limits
const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000; // == (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY
const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000; // == (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY

pub const TIMESTAMP_END_JULIAN: i32 = 109_203_528; // == date2j(294277, 1, 1) */
                                                   // Range-check a timestamp

const TIMESTAMP_SCALES: [i64; MAX_TIMESTAMP_PRECISION + 1] = [
    1_000_000i64,
    100_000i64,
    10_000i64,
    1000i64,
    100i64,
    10i64,
    1i64,
];

const TIMESTAMP_OFFSETS: [i64; MAX_TIMESTAMP_PRECISION + 1] =
    [500_000i64, 50_000i64, 5000i64, 500i64, 50i64, 5i64, 0i64];

/// Timestamp represents absolute time.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Construct a `Timestamp` value.
    #[inline]
    pub(crate) const fn new(v: i64) -> Self {
        Self(v)
    }

    /// Gets value of `Timestamp`.
    #[inline]
    pub(crate) const fn value(self) -> i64 {
        self.0
    }

    #[inline]
    pub(crate) fn is_valid(self) -> bool {
        is_valid_timestamp(self.value())
    }

    #[inline]
    pub(crate) const fn is_begin(self) -> bool {
        self.0 == std::i64::MIN
    }

    #[inline]
    pub(crate) const fn is_end(self) -> bool {
        self.0 == std::i64::MAX
    }

    /// Construct `Timestamp` from year, month, day, hour, minute, second. Second can indicate millisecond and microsecond.
    #[inline]
    pub fn try_from_ymd_hms(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: f64,
    ) -> Result<Self, DateTimeError> {
        let mut tm: PgTime = PgTime::new();
        tm.year = year;
        tm.mon = month;
        tm.mday = day;

        // Note: we'll reject zero or negative year values.  Perhaps negatives
        // should be allowed to represent BC years?
        tm.validate_date(DTK_DATE_M, false, false, false)?;

        if !is_valid_julian(tm.year, tm.mon, tm.mday) {
            return Err(DateTimeError::overflow());
        }

        let date = date2julian(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;

        // This should match the checks in DecodeTimeOnly, except that since we're
        // dealing with a float "sec" value, we also explicitly reject NaN.  (An
        // infinity input should get rejected by the range comparisons, but we
        // can't be sure how those will treat a NaN.)
        if hour < 0
            || minute < 0
            || minute > MINS_PER_HOUR - 1
            || second.is_nan()
            || second < 0.0
            || second > SECS_PER_MINUTE as f64
            || hour > HOURS_PER_DAY
            || (hour == HOURS_PER_DAY && (minute > 0 || second > 0.0))
        {
            return Err(DateTimeError::overflow());
        }

        // This should match tm2time.
        // at here can not overflow.
        let time = (((hour * MINS_PER_HOUR + minute) * SECS_PER_MINUTE) as i64 * USECS_PER_SEC)
            + (second * USECS_PER_SEC as f64).round() as i64;

        let result: Option<i64> = (date as i64)
            .checked_mul(USECS_PER_DAY as i64)
            .and_then(|n| n.checked_add(time));
        let result = match result {
            Some(r) => r,
            None => return Err(DateTimeError::overflow()),
        };

        // check for just-barely overflow (okay except time-of-day wraps)
        // caution: we want to allow 1999-12-31 24:00:00
        if (result < 0 && date > 0) || (result > 0 && date < -1) {
            return Err(DateTimeError::overflow());
        }

        // final range check catches just-out-of-range timestamps.
        if !is_valid_timestamp(result) {
            Err(DateTimeError::overflow())
        } else {
            Ok(Timestamp::new(result))
        }
    }

    #[inline]
    fn from_pgtime_with_type(
        ty: TokenField,
        tm: &PgTime,
        f_sec: i64,
    ) -> Result<Timestamp, DateTimeError> {
        let v = match ty {
            TokenField::Date => tm.to_timestamp(f_sec, None)?,
            TokenField::Late => std::i64::MAX,
            TokenField::Early => std::i64::MIN,
            _ => return Err(DateTimeError::invalid(format!("type: {:?} is invalid", ty))),
        };
        Ok(Timestamp::new(v))
    }

    /// Parses `Timestamp` string. type_mode should be -1, or 1~6, else will be error.
    #[allow(clippy::uninit_assumed_init)]
    pub fn try_from_str(
        s: &str,
        type_mod: i32,
        date_order: DateOrder,
    ) -> Result<Timestamp, DateTimeError> {
        let s = s.as_bytes();
        let mut fields: [&[u8]; MAX_DATE_FIELDS] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut work_buf: [u8; MAX_DATE_LEN] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut f_types: [TokenField; MAX_DATE_FIELDS] =
            unsafe { MaybeUninit::uninit().assume_init() };

        let nf = parse_date_time(s, &mut work_buf, &mut fields, &mut f_types, MAX_DATE_FIELDS)?;
        let mut tm = PgTime::new();
        let (_, d_type, fsec) = decode_date_time(
            &fields[0..nf],
            &f_types[0..nf],
            &mut tm,
            &mut None,
            date_order,
        )?;

        let result = Self::from_pgtime_with_type(d_type, &tm, fsec)?.adjust_by_typmod(type_mod)?;
        Ok(result)
    }

    /// Converts `Timestamp` to string.
    #[inline]
    pub fn format(
        self,
        date_style: DateStyle,
        date_order: DateOrder,
    ) -> Result<String, DateTimeError> {
        if self.is_infinite() {
            format_special_timestamp(self)
        } else {
            let mut buf = Vec::with_capacity(128);
            unsafe { buf.set_len(128) };
            let mut tm: PgTime = PgTime::new();
            let mut fsec = 0;

            timestamp2time(self.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
            let len = format_date_time(
                &mut tm,
                date_order,
                fsec,
                false,
                date_style,
                buf.as_mut_slice(),
            )?;
            unsafe {
                buf.set_len(len);
                Ok(String::from_utf8_unchecked(buf))
            }
        }
    }

    /// Round off a timestamp to suit given typmod
    /// Works for either `Timestamp` or `Timestamptz`.
    #[inline]
    pub(crate) fn adjust_by_typmod(self, typmod: i32) -> Result<Self, DateTimeError> {
        if !self.is_infinite() && (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION as i32) {
            if typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION as i32 {
                return Err(DateTimeError::invalid(format!(
                    "type mod: {:?} is invalid",
                    typmod
                )));
            }

            if self.value() >= 0 {
                let v = ((self.value() + TIMESTAMP_OFFSETS[typmod as usize])
                    / TIMESTAMP_SCALES[typmod as usize])
                    * TIMESTAMP_SCALES[typmod as usize];
                Ok(Timestamp::new(v))
            } else {
                let v = -((((-self.value()) + TIMESTAMP_OFFSETS[typmod as usize])
                    / TIMESTAMP_SCALES[typmod as usize])
                    * TIMESTAMP_SCALES[typmod as usize]);
                Ok(Timestamp::new(v))
            }
        } else {
            Ok(self)
        }
    }

    /// `Timestamp` add `Interval`.
    /// Note that interval has provisions for qualitative year/month and day
    /// units, so try to do the right thing with them.
    /// To add a month, increment the month, and use the same day of month.
    /// Then, if the next month has fewer days, set the day of month
    /// to the last day of month.
    /// To add a day, increment the mday, and use the same time of day.
    /// Lastly, add in the "quantitative time".
    pub fn add_interval(self, span: Interval) -> Result<Self, DateTimeError> {
        let mut timestamp = self.0;
        if self.is_infinite() {
            return Ok(self);
        } else {
            if span.month() != 0 {
                let mut tm: PgTime = PgTime::new();
                let mut fsec = 0;

                timestamp2time(self.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
                tm.mon = tm.mon.wrapping_add(span.month());
                if tm.mon > MONTHS_PER_YEAR as i32 {
                    tm.year = tm.year.wrapping_add((tm.mon - 1) / MONTHS_PER_YEAR);
                    tm.mon = ((tm.mon - 1) % MONTHS_PER_YEAR) + 1;
                } else if tm.mon < 1 {
                    tm.year = tm.year.wrapping_add(tm.mon / MONTHS_PER_YEAR - 1);
                    tm.mon = tm.mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
                }

                let max_mon_days = days_of_month(tm.year, tm.mon);
                // adjust for end of month boundary problems.
                if tm.mday > max_mon_days {
                    tm.mday = max_mon_days;
                }

                timestamp = tm.to_timestamp(fsec, None)?;
            }

            if span.day() != 0 {
                let mut tm: PgTime = PgTime::new();
                let mut fsec = 0;
                timestamp2time(timestamp, &None, &mut tm, &mut fsec, &mut None, &None)?;
                // Add days by converting to and from Julian
                let julian = date2julian(tm.year, tm.mon, tm.mday).wrapping_add(span.day());
                let (y, m, d) = julian2date(julian);
                tm.set_ymd(y, m, d);
                timestamp = tm.to_timestamp(fsec, None)?;
            }

            timestamp = timestamp.wrapping_add(span.time());

            if !is_valid_timestamp(timestamp) {
                return Err(DateTimeError::invalid(format!(
                    "timestamp: {:?} is invalid",
                    timestamp
                )));
            }
        }

        Ok(Timestamp::new(timestamp))
    }

    /// `Timestamp` adds `Time`.
    #[inline]
    pub fn add_time(self, time: Time) -> Result<Timestamp, DateTimeError> {
        let span = From::from(time);
        self.add_interval(span)
    }

    /// `Timestamp` subtracts `Interval`.
    #[inline]
    pub fn sub_interval(self, span: Interval) -> Result<Timestamp, DateTimeError> {
        let neg_span = span.negate()?;
        self.add_interval(neg_span)
    }

    /// `Timestamp` subtracts `Date`.
    #[inline]
    pub fn sub_date(self, date: Date) -> Result<Interval, DateTimeError> {
        let timetsamp: Timestamp = Timestamp::try_from(date)?;
        self.sub_timestamp(timetsamp)
    }

    /// `Timestamp` subtracts `Time`.
    #[inline]
    pub fn sub_time(self, time: Time) -> Result<Timestamp, DateTimeError> {
        let span: Interval = Interval::from(time);
        self.sub_interval(span)
    }

    /// `Timestamp` subtracts `Timestamp`.
    #[inline]
    pub fn sub_timestamp(self, timestamp: Timestamp) -> Result<Interval, DateTimeError> {
        if self.is_infinite() || timestamp.is_infinite() {
            return Err(DateTimeError::invalid(format!(
                "sub have infinite, left: {:?}, right: {:?}",
                self, timestamp
            )));
        }

        let time = self.value() - timestamp.value();

        //	This is wrong, but removing it breaks a lot of regression tests.
        // For example:
        //
        //	test=> SET timezone = 'EST5EDT';
        //	test=> SELECT
        //	test-> ('2005-10-30 13:22:00-05'::timestamptz -
        //	test(>	'2005-10-29 13:22:00-04'::timestamptz);
        //	?column?
        //	----------------
        //	 1 day 01:00:00
        //	 (1 row)
        //
        //	so adding that to the first timestamp gets:
        //
        //	 test=> SELECT
        //	 test-> ('2005-10-29 13:22:00-04'::timestamptz +
        //	 test(> ('2005-10-30 13:22:00-05'::timestamptz -
        //	 test(>  '2005-10-29 13:22:00-04'::timestamptz)) at time zone 'EST';
        //		timezone
        //	--------------------
        //	2005-10-30 14:22:00
        //	(1 row)
        //----------

        let span = Interval::from_mdt(0, 0, time);
        let ret = span.justify_hours();
        Ok(ret)
    }

    /// Calculates time difference while retaining year/month fields.
    /// Note that this does not result in an accurate absolute time span
    /// since year and month are out of context once the arithmetic
    /// is done.
    #[inline]
    pub fn age(self, timestamp: Timestamp) -> Result<Interval, DateTimeError> {
        let mut tm: PgTime = PgTime::new();
        let mut tm1: PgTime = PgTime::new();
        let mut tm2: PgTime = PgTime::new();
        let mut fsec1 = 0;
        let mut fsec2 = 0;
        timestamp2time(self.value(), &None, &mut tm1, &mut fsec1, &mut None, &None)?;
        timestamp2time(
            timestamp.value(),
            &None,
            &mut tm2,
            &mut fsec2,
            &mut None,
            &None,
        )?;
        // form the symbolic difference.
        let mut fsec = fsec1 - fsec2;
        tm.sec = tm1.sec - tm2.sec;
        tm.min = tm1.min - tm2.min;
        tm.hour = tm1.hour - tm2.hour;
        tm.mday = tm1.mday - tm2.mday;
        tm.mon = tm1.mon - tm2.mon;
        tm.year = tm1.year - tm2.year;

        // flip sign if necessary.
        if self < timestamp {
            fsec = -fsec;
            tm.sec = -tm.sec;
            tm.min = -tm.min;
            tm.hour = -tm.hour;
            tm.mday = -tm.mday;
            tm.mon = -tm.mon;
            tm.year = -tm.year;
        }

        // propagate any negative fields into the next higher field.
        // why do not use / to compute the borrow num from upper.
        // for the borrow can not be more than 1 at normal.
        while fsec < 0 {
            fsec += USECS_PER_SEC;
            tm.sec -= 1;
        }

        while tm.sec < 0 {
            tm.sec += SECS_PER_MINUTE;
            tm.min -= 1;
        }

        while tm.min < 0 {
            tm.min += MINS_PER_HOUR;
            tm.hour -= 1;
        }

        while tm.hour < 0 {
            tm.hour += HOURS_PER_DAY;
            tm.mday -= 1;
        }

        while tm.mday < 0 {
            if self < timestamp {
                tm.mday += days_of_month(tm1.year, tm1.mon);
                tm.mon -= 1;
            } else {
                tm.mday += days_of_month(tm2.year, tm2.mon);
                tm.mon -= 1;
            }
        }

        while tm.mon < 0 {
            tm.mon += MONTHS_PER_YEAR;
            tm.year -= 1;
        }

        // recover sign if necessary... */
        if self < timestamp {
            fsec = -fsec;
            tm.sec = -tm.sec;
            tm.min = -tm.min;
            tm.hour = -tm.hour;
            tm.mday = -tm.mday;
            tm.mon = -tm.mon;
            tm.year = -tm.year;
        }
        let span = Interval::from_pgtime(&tm, fsec)?;

        Ok(span)
    }

    /// Used by timestamp_part and timestamptz_part when extracting from infinite
    /// timestamp[tz].  Returns +/-Infinity if that is the appropriate result,
    /// otherwise returns zero (which should be taken as meaning to return NULL).
    ///
    /// Errors thrown here for invalid units should exactly match those that
    /// would be thrown in the calling functions, else there will be unexpected
    /// discrepancies between finite- and infinite-input cases.
    #[inline]
    fn infinite_part(
        _ty: FieldType,
        unit: DateUnit,
        is_negative: bool,
        _is_tz: bool,
    ) -> Result<Option<f64>, DateTimeError> {
        match unit {
            // Oscillating units
            DateUnit::MicroSec
            | DateUnit::MilliSec
            | DateUnit::Second
            | DateUnit::Minute
            | DateUnit::Hour
            | DateUnit::Day
            | DateUnit::Month
            | DateUnit::Quarter
            | DateUnit::Week
            | DateUnit::Dow
            | DateUnit::IsoDow
            | DateUnit::Doy
            | DateUnit::Tz
            | DateUnit::TzMinute
            | DateUnit::TzHour => Ok(None),

            // Monotonically-increasing unit
            DateUnit::Year
            | DateUnit::Decade
            | DateUnit::Century
            | DateUnit::Millennium
            | DateUnit::JULIAN
            | DateUnit::IsoYear
            | DateUnit::Epoch => {
                if is_negative {
                    Ok(Some(-std::f64::INFINITY))
                } else {
                    Ok(Some(std::f64::INFINITY))
                }
            }

            _ => Err(DateTimeError::invalid(format!(
                "unit: {:?} is invalid",
                unit
            ))),
        }
    }
}

#[inline]
pub fn is_valid_timestamp(t: i64) -> bool {
    MIN_TIMESTAMP <= t && t < END_TIMESTAMP
}

/// Convert reserved timestamp data type to string.
#[inline]
fn format_special_timestamp(tp: Timestamp) -> Result<String, DateTimeError> {
    if tp.is_begin() {
        let ret = unsafe { String::from_utf8_unchecked(EARLY.into()) };
        Ok(ret)
    } else if tp.is_end() {
        let ret = unsafe { String::from_utf8_unchecked(LATE.into()) };
        Ok(ret)
    } else {
        Err(DateTimeError::invalid(format!(
            "timestamp: {:?} is not valid",
            tp
        )))
    }
}

/// Encode date and time interpreted as local time.
///
/// tm and fsec are the value to encode, print_tz determines whether to include
/// a time zone (the difference between timestamp and timestamptz types), tz is
/// the numeric time zone offset, tzn is the textual time zone, which if
/// specified will be used instead of tz by some styles, style is the date
/// style, str is where to write the output.
///
/// Supported date styles:
///  Postgres - day mon hh:mm:ss yyyy tz
///  SQL - mm/dd/yyyy hh:mm:ss.ss tz
///  ISO - yyyy-mm-dd hh:mm:ss+/-tz
///  German - dd.mm.yyyy hh:mm:ss tz
///  XSD - yyyy-mm-ddThh:mm:ss.ss+/-tz
pub fn format_date_time(
    tm: &mut PgTime,
    date_order: DateOrder,
    fsec: i64,
    print_tz: bool,
    style: DateStyle,
    s: &mut [u8],
) -> Result<usize, DateTimeError> {
    debug_assert!(tm.mon >= 1 && tm.mon <= MONTHS_PER_YEAR);
    // Negative tm_isdst means we have no valid time zone translation.
    let _print_tz = if tm.isdst < 0 { false } else { print_tz };

    let mut offset = 0;

    match style {
        DateStyle::ISO | DateStyle::XSD => {
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            // Compatible with ISO-8601 date formats
            offset += pg_ltostr_zeropad(&mut s[offset..], year, 4);
            offset = set_one_byte(s, offset, b'-');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
            offset = set_one_byte(s, offset, b'-');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
            offset = set_one_byte(s, offset, if style == DateStyle::ISO { b' ' } else { b'T' });
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.hour, 2);
            offset = set_one_byte(s, offset, b':');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.min, 2);
            offset = set_one_byte(s, offset, b':');
            offset += append_timestamp_seconds(&mut s[offset..], tm, fsec);
            /*            if print_tz {
                offset += encode_timezone(&mut s[offset..], tz, style);
            }*/
        }

        DateStyle::SQL => {
            // Compatible with Oracle/Ingres date formats.
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
            offset = set_one_byte(s, offset, b' ');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.hour, 2);
            offset = set_one_byte(s, offset, b':');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.min, 2);
            offset = set_one_byte(s, offset, b':');
            offset += append_timestamp_seconds(&mut s[offset..], tm, fsec);

            // Note: the uses of %.*s in this function would be risky if the
            // timezone names ever contain non-ASCII characters.  However, all
            // TZ abbreviations in the IANA database are plain ASCII.
            //offset += append_time_zone(&mut s[offset..], print_tz, tz, tzn, style)?;
        }

        DateStyle::German => {
            /* German variant on European style */
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
            offset = set_one_byte(s, offset, b'.');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.mon, 2);
            offset = set_one_byte(s, offset, b'.');
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            offset += pg_ltostr_zeropad(&mut s[offset..], year, 4);
            offset = set_one_byte(s, offset, b' ');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.hour, 2);
            offset = set_one_byte(s, offset, b':');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.min, 2);
            offset = set_one_byte(s, offset, b':');
            offset += append_timestamp_seconds(&mut s[offset..], tm, fsec);

            //offset += append_time_zone(&mut s[offset..], print_tz, tz, tzn, style)?;
        }

        _ => {
            // Backward-compatible with traditional Postgres abstime dates.
            let day = date2julian(tm.year, tm.mon, tm.mday);
            tm.wday = julian_to_week_day(day);
            let day_str = get_wday_name(tm.wday);
            offset = copy_slice(s, offset, day_str);
            offset = set_one_byte(s, offset, b' ');
            if date_order == DateOrder::DMY {
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
                offset = set_one_byte(s, offset, b' ');
                let month_str = get_month_name(tm.mon);
                offset = copy_slice(s, offset, month_str);
            } else {
                let month_str = get_month_name(tm.mon);
                offset = copy_slice(s, offset, month_str);
                offset = set_one_byte(s, offset, b' ');
                offset += pg_ltostr_zeropad(&mut s[offset..], tm.mday, 2);
            }
            offset = set_one_byte(s, offset, b' ');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.hour, 2);
            offset = set_one_byte(s, offset, b':');
            offset += pg_ltostr_zeropad(&mut s[offset..], tm.min, 2);
            offset = set_one_byte(s, offset, b':');
            offset += append_timestamp_seconds(&mut s[offset..], tm, fsec);
            offset = set_one_byte(s, offset, b' ');
            let year = if tm.year > 0 { tm.year } else { -(tm.year - 1) };
            offset += pg_ltostr_zeropad(&mut s[offset..], year, 4);

            /*           if print_tz {
                match tzn {
                    Some(z) => {
                        s[offset] = b' ';
                        offset += 1;
                        offset = copy_buf(s, offset, z);
                    }
                    None => {
                        // We have a time zone, but no string version. Use the
                        // numeric form, but be sure to include a leading space to
                        // avoid formatting something which would be rejected by
                        // the date/time parser later. - thomas 2001-10-19
                        offset = copy_one_byte(s, offset, b' ');
                        offset = encode_timezone(&mut s[offset..], tz, style);
                    }
                }
            }*/
        }
    }
    if tm.year <= 0 {
        offset = copy_slice(s, offset, b" BC");
    }
    Ok(offset)
}

/// Variant of above that's specialized to timestamp case.
///
/// Returns a pointer to the new end of string.  No NUL terminator is put
/// there; callers are responsible for NUL terminating str themselves.
#[inline]
fn append_timestamp_seconds(s: &mut [u8], tm: &PgTime, fsec: i64) -> usize {
    append_seconds(s, tm.sec, fsec, MAX_TIMESTAMP_PRECISION as i32, true)
}

/// Converts `timestamp` to `i64` type.
impl From<Timestamp> for i64 {
    #[inline]
    fn from(t: Timestamp) -> Self {
        t.value()
    }
}

/// Converts `i64` to `timestamp` type.
impl From<i64> for Timestamp {
    #[inline]
    fn from(v: i64) -> Self {
        Timestamp::new(v)
    }
}

/// Converts `Date` to `Timestamp`.
impl TryFrom<Date> for Timestamp {
    type Error = DateTimeError;

    #[inline]
    fn try_from(value: Date) -> Result<Self, Self::Error> {
        if value == DATE_BEGIN_VAL {
            Ok(DATE_TIME_BEGIN_VAL)
        } else if value == DATE_END_VAL {
            Ok(DATE_TIME_END_VAL)
        } else {
            // Date's range is wider than timestamp's, so check for boundaries.
            // Since dates have the same minimum values as timestamps, only upper
            // boundary need be checked for overflow.
            if value.value() >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
                return Err(DateTimeError::overflow());
            }

            // date is days since 2000, timestamp is microseconds since same.
            let result = (value.value() as i64) * USECS_PER_DAY;
            Ok(Timestamp::new(result))
        }
    }
}

/// The min value of date time.
pub(crate) const DATE_TIME_BEGIN_VAL: Timestamp = Timestamp::new(std::i64::MIN);
/// The max value of date time.
pub(crate) const DATE_TIME_END_VAL: Timestamp = Timestamp::new(std::i64::MAX);

impl DateTime for Timestamp {
    /// Extracts specified field from `Timestamp`.
    #[inline]
    fn date_part(&self, ty: FieldType, unit: DateUnit) -> Result<Option<f64>, DateTimeError> {
        if self.is_infinite() {
            return Self::infinite_part(ty, unit, self.is_begin(), false);
        }
        match ty {
            FieldType::Unit => {
                let mut tm: PgTime = PgTime::new();
                let mut fsec = 0;
                timestamp2time(self.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
                match unit {
                    DateUnit::MicroSec => Ok(Some(tm.sec as f64 * 1_000_000.0 + fsec as f64)),
                    DateUnit::MilliSec => Ok(Some(tm.sec as f64 * 1000.0 + fsec as f64 / 1000.0)),
                    DateUnit::Second => Ok(Some(tm.sec as f64 + fsec as f64 / 1_000_000.0)),
                    DateUnit::Minute => Ok(Some(tm.min as f64)),
                    DateUnit::Hour => Ok(Some(tm.hour as f64)),
                    DateUnit::Day => Ok(Some(tm.mday as f64)),
                    DateUnit::Month => Ok(Some(tm.mon as f64)),
                    DateUnit::Quarter => Ok(Some(((tm.mon - 1) / 3 + 1) as f64)),
                    DateUnit::Week => Ok(Some(date_to_iso_week(tm.year, tm.mon, tm.mday) as f64)),
                    DateUnit::Year => {
                        if tm.year > 0 {
                            Ok(Some(tm.year as f64))
                        } else {
                            // there is no year 0, just 1 BC and 1 AD.
                            Ok(Some((tm.year - 1) as f64))
                        }
                    }

                    DateUnit::Decade => {
                        // what is a decade wrt dates? let us assume that decade 199
                        // is 1990 thru 1999... decade 0 starts on year 1 BC, and -1
                        // is 11 BC thru 2 BC.
                        if tm.year >= 0 {
                            Ok(Some((tm.year / 10) as f64))
                        } else {
                            Ok(Some((-((8 - (tm.year - 1)) / 10)) as f64))
                        }
                    }
                    DateUnit::Century => {
                        // centuries AD, c>0: year in [ (c-1)* 100 + 1 : c*100 ]
                        // centuries BC, c<0: year in [ c*100 : (c+1) * 100 - 1]
                        // there is no number 0 century.
                        if tm.year > 0 {
                            Ok(Some(((tm.year + 99) / 100) as f64))
                        } else {
                            // caution: C division may have negative remainder.
                            Ok(Some((-((99 - (tm.year - 1)) / 100)) as f64))
                        }
                    }
                    DateUnit::Millennium => {
                        // see comments above.
                        if tm.year > 0 {
                            Ok(Some(((tm.year + 999) / 1000) as f64))
                        } else {
                            Ok(Some((-((999 - (tm.year - 1)) / 1000)) as f64))
                        }
                    }

                    DateUnit::JULIAN => {
                        let date = date2julian(tm.year, tm.mon, tm.mday);
                        let result = date as f64
                            + ((((tm.hour as i64 * MINS_PER_HOUR as i64) + tm.min as i64)
                                * SECS_PER_MINUTE as i64) as f64
                                + tm.sec as f64
                                + (fsec as f64 / 1_000_000.0))
                                / SECS_PER_DAY as f64;
                        Ok(Some(result))
                    }

                    DateUnit::IsoYear => {
                        let mut result = date_to_iso_year(tm.year, tm.mon, tm.mday);
                        // Adjust BC years
                        if result <= 0 {
                            result -= 1;
                        }
                        Ok(Some(result as f64))
                    }
                    DateUnit::Dow | DateUnit::IsoDow => {
                        let mut tm = PgTime::new();
                        let mut fsec = 0;
                        timestamp2time(self.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
                        let date = date2julian(tm.year, tm.mon, tm.mday);
                        let day = julian_to_week_day(date);
                        if unit == DateUnit::IsoDow && day == 0 {
                            Ok(Some(7_f64))
                        } else {
                            Ok(Some(day as f64))
                        }
                    }
                    DateUnit::Doy => {
                        let mut tm = PgTime::new();
                        let mut fsec = 0;
                        timestamp2time(self.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
                        Ok(Some(
                            (date2julian(tm.year, tm.mon, tm.mday) - date2julian(tm.year, 1, 1) + 1)
                                as f64,
                        ))
                    }
                    _ => Err(DateTimeError::invalid(format!(
                        "unit: {:?} is invalid",
                        unit
                    ))),
                }
            }
            FieldType::Epoch => {
                match unit {
                    DateUnit::Epoch => {
                        // Todo  time zone
                        Err(DateTimeError::invalid(format!(
                            "unit: {:?} is invalid",
                            unit
                        )))
                    }
                    _ => Err(DateTimeError::invalid(format!(
                        "unit: {:?} is invalid",
                        unit
                    ))),
                }
            }
        }
    }

    /// Checks whether `Timestamp` is finite.
    #[inline]
    fn is_finite(&self) -> bool {
        !self.is_begin() && !self.is_end()
    }

    /// Truncates `Timestamp` to specified units.
    #[inline]
    fn truncate(&self, ty: FieldType, unit: DateUnit) -> Result<Self, DateTimeError> {
        match ty {
            FieldType::Unit => {
                let mut tm = PgTime::new();
                let mut fsec = 0;

                timestamp2time(self.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
                match unit {
                    DateUnit::Week => (&mut tm).truncate_timestamp_week(&mut fsec),
                    DateUnit::Millennium => (&mut tm).truncate_timestamp_millennium(&mut fsec),
                    DateUnit::Century => (&mut tm).truncate_timestamp_century(&mut fsec),
                    DateUnit::Decade => (&mut tm).truncate_timestamp_decade(&mut fsec),
                    DateUnit::Year => (&mut tm).truncate_timestamp_year(&mut fsec),
                    DateUnit::Quarter => (&mut tm).truncate_timestamp_quarter(&mut fsec),
                    DateUnit::Month => (&mut tm).truncate_timestamp_month(&mut fsec),
                    DateUnit::Day => (&mut tm).truncate_day(&mut fsec),
                    DateUnit::Hour => (&mut tm).truncate_hour(&mut fsec),
                    DateUnit::Minute => (&mut tm).truncate_minute(&mut fsec),
                    DateUnit::Second => (&mut tm).truncate_sec(&mut fsec),
                    DateUnit::MilliSec => (&mut tm).truncate_milli_sec(&mut fsec),
                    DateUnit::MicroSec => (&mut tm).truncate_micro_sec(&mut fsec),
                    _ => {
                        return Err(DateTimeError::invalid(format!(
                            "unit: {:?} is invalid at truncate",
                            unit
                        )))
                    }
                }

                let ret = tm.to_timestamp(fsec, None)?;
                Ok(Timestamp::new(ret))
            }
            _ => Err(DateTimeError::invalid(format!(
                "type: {:?} is invalid at truncate",
                ty
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::PgTime;
    use crate::error::DateTimeError;
    use crate::timestamp::*;
    use crate::token::TokenField;
    use crate::{DateOrder, DateStyle, DateTime, DateUnit, FieldType, IntervalStyle};

    #[test]
    fn test_timestamp_from_str() -> Result<(), DateTimeError> {
        let time = Timestamp::try_from_str("y2000m2d12t1240 am", -2, DateOrder::MDY);
        assert!(time.is_err());

        let time = Timestamp::try_from_str("y2000m2d12t1240 am", 7, DateOrder::MDY);
        assert!(time.is_err());

        let time = Timestamp::try_from_str("y1", 3, DateOrder::YMD);
        assert!(time.is_err());

        let ret = Timestamp::try_from_str("y2000m2d12t2840 am", -1, DateOrder::MDY);
        assert!(ret.is_err());

        let tp = Timestamp::try_from_str("allballs", -1, DateOrder::MDY)?;
        let s = tp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "0002-11-30 00:00:00 BC".to_string());

        let ret = Timestamp::try_from_str("current", -1, DateOrder::MDY);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_str("y2000m12d23isodow23", -1, DateOrder::MDY);
        assert!(ret.is_err());

        let time = Timestamp::try_from_str("1999-01-08 04:05:06.456db", 3, DateOrder::YMD);
        assert!(time.is_err());

        let ret = Timestamp::try_from_str("J-2451187", -1, DateOrder::MDY);
        assert!(ret.is_err());

        let time = Timestamp::try_from_str("y2000m2d12t2040", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2000-02-12 20:40:00".to_string());

        let time = Timestamp::try_from_str("y2000m2d12t1240 am", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2000-02-12 00:40:00".to_string());

        let time = Timestamp::try_from_str("y2000m2d12t0740 pm", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2000-02-12 19:40:00".to_string());

        let ret = Timestamp::try_from_str("y2000m12t991223", -1, DateOrder::MDY);
        assert!(ret.is_err());

        let time = Timestamp::try_from_str("on y1989m3d10h12m30s20.345", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1989-03-10 12:30:20.345".to_string());

        let time = Timestamp::try_from_str("y1989m3d10h12m30s20.345", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1989-03-10 12:30:20.345".to_string());

        let time = Timestamp::try_from_str("y1989m3d10h12mm30s20.345", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1989-03-10 12:30:20.345".to_string());

        let time = Timestamp::try_from_str("J2451187.345", -1, DateOrder::MDY)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08 08:16:47.999999".to_string());

        let time = Timestamp::try_from_str("1999-01-08 040506.456789", 3, DateOrder::YMD)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08 04:05:06.457");

        let time = Timestamp::try_from_str("1999-01-08 040506.4567878", 6, DateOrder::YMD)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08 04:05:06.456788");

        let time = Timestamp::try_from_str("1999-01-08 0405", 3, DateOrder::YMD)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08 04:05:00");

        let time = Timestamp::try_from_str("1999-01-08 04:05:06.456", 3, DateOrder::YMD)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08 04:05:06.456");

        let s = time.format(DateStyle::Postgres, DateOrder::YMD)?;
        assert_eq!(s, "Fri Jan 08 04:05:06.456 1999");

        let s = time.format(DateStyle::Postgres, DateOrder::DMY)?;
        assert_eq!(s, "Fri 08 Jan 04:05:06.456 1999");

        let s = time.format(DateStyle::SQL, DateOrder::YMD)?;
        assert_eq!(s, "01/08/1999 04:05:06.456");

        let s = time.format(DateStyle::SQL, DateOrder::DMY)?;
        assert_eq!(s, "08/01/1999 04:05:06.456");

        let s = time.format(DateStyle::German, DateOrder::DMY)?;
        assert_eq!(s, "08.01.1999 04:05:06.456");

        let s = time.format(DateStyle::German, DateOrder::YMD)?;
        assert_eq!(s, "08.01.1999 04:05:06.456");

        let s = time.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08T04:05:06.456");

        let time = Timestamp::try_from_str("08.01.1999 04:05:06.456", 6, DateOrder::MDY)?;
        let s = time.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-08-01T04:05:06.456");

        let time = Timestamp::try_from_str("1999-01-08T04:05:06.456", 6, DateOrder::YMD)?;
        let s = time.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08T04:05:06.456");

        let time = Timestamp::try_from_str("Fri 08 Jan 04:05:06.4561289 1999", 6, DateOrder::DMY)?;
        let s = time.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08T04:05:06.456129");

        let time = Timestamp::try_from_str("Fri 08 Jan 04:05:06.4561289 1999", 0, DateOrder::DMY)?;
        let s = time.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08T04:05:06");

        let time = Timestamp::try_from_str("Fri 08 Jan 04:05:06.4561289 1999", 2, DateOrder::DMY)?;
        let s = time.format(DateStyle::XSD, DateOrder::YMD)?;
        assert_eq!(s, "1999-01-08T04:05:06.46");

        let time = Timestamp::try_from_str("infinity", 3, DateOrder::YMD)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "infinity".to_string());

        let time = Timestamp::try_from_str("-infinity", 3, DateOrder::YMD)?;
        let s = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "-infinity".to_string());

        Ok(())
    }

    #[test]
    fn test_timestamp_range() -> Result<(), DateTimeError> {
        let time = Timestamp::try_from_str("4713-01-01 00:00:00 bc", 3, DateOrder::YMD)?;
        let ret = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(ret, "4713-01-01 00:00:00 BC");

        let time = Timestamp::try_from_str("294276-12-31 23:59:59", 3, DateOrder::YMD)?;
        let ret = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(ret, "294276-12-31 23:59:59");

        let time = Timestamp::try_from_str("4714-12-31 24:00:00 bc", 3, DateOrder::YMD)?;
        let ret = time.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(ret, "4713-01-01 00:00:00 BC");

        let time = Timestamp::try_from_str("4715-01-01 01:00:00 bc", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Timestamp::try_from_str("294276-12-31 24:00:00", 3, DateOrder::YMD);
        assert!(time.is_err());

        Ok(())
    }

    #[test]
    fn test_from_ymd_hms() -> Result<(), DateTimeError> {
        let timestamp = Timestamp::try_from_ymd_hms(2015, 2, 20, 12, 23, 34.5678998)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-02-20 12:23:34.5679");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 2, 20, 12, 23, 60.0)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-02-20 12:24:00");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 1, 20, 12, 23, 34.5678998)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-01-20 12:23:34.5679");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 12, 20, 12, 23, 34.5678998)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-12-20 12:23:34.5679");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 12, 1, 12, 23, 34.5678998)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-12-01 12:23:34.5679");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 12, 31, 12, 23, 34.5678998)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-12-31 12:23:34.5679");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 12, 31, 0, 0, 0.0)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-12-31 00:00:00");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 12, 31, 0, 59, 0.0)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-12-31 00:59:00");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 12, 31, 0, 59, 59.999999)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-12-31 00:59:59.999999");

        let timestamp = Timestamp::try_from_ymd_hms(2015, 2, 20, 24, 00, 0.0)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2015-02-21 00:00:00");

        let ret = Timestamp::try_from_ymd_hms(-2015, 2, 20, 12, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(294277, 2, 20, 12, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 0, 20, 12, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 13, 20, 12, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 0, 12, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 32, 12, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 31, -1, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 31, 24, 23, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 31, 23, -1, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 31, 23, 60, 30.0);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 31, 23, 59, -0.1);
        assert!(ret.is_err());

        let ret = Timestamp::try_from_ymd_hms(2015, 1, 31, 23, 59, 60.1);
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_age() -> Result<(), DateTimeError> {
        let timestamp1 = Timestamp::try_from_str("2020-3-31 16:06:14.5678", 3, DateOrder::YMD)?;
        let timestamp2 = Timestamp::try_from_str("2011-7-10 13:03:5.56798", 3, DateOrder::YMD)?;
        let ret = timestamp1.age(timestamp2)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P8Y8M21DT3H3M9S");

        let ret = timestamp2.age(timestamp1)?;
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P-8Y-8M-21DT-3H-3M-9S");
        Ok(())
    }

    #[test]
    fn test_timestamp_date_part() -> Result<(), DateTimeError> {
        let timestamp = Timestamp::try_from_str("2001-02-16 20:38:40.4567890", 6, DateOrder::YMD)?;
        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret, Some(21.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::JULIAN)?;
        assert_eq!(ret, Some(2451957.860190472));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Millennium)?;
        assert_eq!(ret, Some(3.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Decade)?;
        assert_eq!(ret, Some(200.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Day)?;
        assert_eq!(ret, Some(16.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Dow)?;
        assert_eq!(ret, Some(5.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Doy)?;
        assert_eq!(ret, Some(47.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::IsoDow)?;
        assert_eq!(ret, Some(5.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::IsoYear)?;
        assert_eq!(ret, Some(2001.0));

        let ret = timestamp.date_part(FieldType::Epoch, DateUnit::Epoch);
        assert!(ret.is_err());

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Hour)?;
        assert_eq!(ret, Some(20.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Minute)?;
        assert_eq!(ret, Some(38.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Second)?;
        assert_eq!(ret, Some(40.456789));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::MilliSec)?;
        assert_eq!(ret, Some(40456.789));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::MicroSec)?;
        assert_eq!(ret, Some(40456789.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Month)?;
        assert_eq!(ret, Some(2.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Quarter)?;
        assert_eq!(ret, Some(1.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Week)?;
        assert_eq!(ret, Some(7.0));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Year)?;
        assert_eq!(ret, Some(2001.0));

        let ret = timestamp.date_part(FieldType::Epoch, DateUnit::Epoch);
        assert!(ret.is_err());
        let ret = timestamp.date_part(FieldType::Epoch, DateUnit::Day);
        assert!(ret.is_err());

        let timestamp = Timestamp::try_from_str("infinity", 6, DateOrder::YMD)?;
        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Millennium)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Decade)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Day)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Dow)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Doy)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::IsoDow)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::IsoYear)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let ret = timestamp.date_part(FieldType::Epoch, DateUnit::Epoch)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Hour)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Minute)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Second)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::MilliSec)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::MicroSec)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Month)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Quarter)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Week)?;
        assert_eq!(ret, None);

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Year)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let ret = timestamp.date_part(FieldType::Unit, DateUnit::JULIAN)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        let timestamp = Timestamp::try_from_str("-infinity", 6, DateOrder::YMD)?;
        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret, Some(-std::f64::INFINITY));

        let timestamp = Timestamp::try_from_str("infinity", 6, DateOrder::YMD)?;
        let ret = timestamp.date_part(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret, Some(std::f64::INFINITY));

        Ok(())
    }

    #[test]
    fn test_timestamp_truncate() -> Result<(), DateTimeError> {
        let timestamp = Timestamp::try_from_str("2116-02-16 20:38:40.4567890", 6, DateOrder::YMD)?;
        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Millennium)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2001-01-01 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Century)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2101-01-01 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Decade)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2110-01-01 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Year)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-01-01 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Quarter)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-01-01 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Month)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-01 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Week)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-10 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Day)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-16 00:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Hour)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-16 20:00:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Minute)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-16 20:38:00");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::Second)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-16 20:38:40");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::MilliSec)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-16 20:38:40.456");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::MicroSec)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2116-02-16 20:38:40.456789");

        let ret = timestamp.truncate(FieldType::Unit, DateUnit::JULIAN);
        assert!(ret.is_err());

        let ret = timestamp.truncate(FieldType::Epoch, DateUnit::Epoch);
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_timestamp_finite() -> Result<(), DateTimeError> {
        let timestamp = Timestamp::try_from_str("2001-02-16 20:38:40.4567890", 6, DateOrder::YMD)?;
        assert!(timestamp.is_finite());
        let timestamp = Timestamp::new(std::i64::MIN);
        assert!(timestamp.is_infinite());
        Ok(())
    }

    #[test]
    fn test_from_pgtime_for_type() -> Result<(), DateTimeError> {
        let d_type = TokenField::Isoyear;
        let tm = PgTime::new();
        let f_sec = 0;
        let ret = Timestamp::from_pgtime_with_type(d_type, &tm, f_sec);
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_convert() -> Result<(), DateTimeError> {
        let date = Date::try_from_ymd(2000, 10, 1)?;
        let timestamp1: Timestamp = TryFrom::try_from(date)?;
        let timestamp2 = Timestamp::try_from_ymd_hms(2000, 10, 1, 0, 0, 0.0)?;
        assert_eq!(timestamp1, timestamp2);
        let s = timestamp1.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2000-10-01 00:00:00");

        let date = Date::new(std::i32::MIN);
        let timestamp: Timestamp = TryFrom::try_from(date)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "-infinity");

        let date = Date::new(std::i32::MAX);
        let timestamp: Timestamp = TryFrom::try_from(date)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "infinity");

        let timestamp = Timestamp::try_from_ymd_hms(2008, 10, 1, 12, 10, 23.45)?;
        let date1: Date = TryFrom::try_from(timestamp)?;
        let date2 = Date::try_from_ymd(2008, 10, 1)?;
        assert_eq!(date1, date2);

        let timestamp = Timestamp::new(std::i64::MIN);
        let date: Date = TryFrom::try_from(timestamp)?;
        assert_eq!(date.format(DateStyle::ISO, DateOrder::YMD)?, "-infinity");

        let timestamp = Timestamp::new(std::i64::MIN + 20);
        let ret: Result<Date, DateTimeError> = TryFrom::try_from(timestamp);
        assert!(ret.is_err());

        let timestamp = Timestamp::new(std::i64::MAX);
        let date: Date = TryFrom::try_from(timestamp)?;
        assert_eq!(date.format(DateStyle::ISO, DateOrder::YMD)?, "infinity");

        let timestamp = Timestamp::try_from_ymd_hms(2008, 10, 1, 12, 10, 23.45)?;
        let time1: Option<Time> = TryFrom::try_from(timestamp)?;
        assert!(time1.is_some());
        let time2 = Time::try_from_hms(12, 10, 23.45)?;
        assert_eq!(time1.unwrap(), time2);

        let timestamp = Timestamp::new(std::i64::MIN);
        let time: Option<Time> = TryFrom::try_from(timestamp)?;
        assert!(time.is_none());
        Ok(())
    }
}
