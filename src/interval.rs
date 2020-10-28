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

//! Interval definition.

use crate::common::*;
use crate::error::DateTimeError;
use crate::parse::{decode_interval, decode_iso8601_interval, parse_date_time};
use crate::token::*;
use crate::{DateTime, DateUnit, FieldType, IntervalStyle, Time};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::ops::Add;

pub const INTERVAL_MASK_Y: i32 = TokenType::YEAR.mask();
pub const INTERVAL_MASK_M: i32 = TokenType::MONTH.mask();
pub const INTERVAL_MASK_YM: i32 = TokenType::YEAR.mask() | TokenType::MONTH.mask();
pub const INTERVAL_MASK_D: i32 = TokenType::DAY.mask();
pub const INTERVAL_MASK_H: i32 = TokenType::HOUR.mask();
pub const INTERVAL_MASK_DH: i32 = TokenType::DAY.mask() | TokenType::HOUR.mask();
pub const INTERVAL_MASK_MIN: i32 = TokenType::MINUTE.mask();
pub const INTERVAL_MASK_HM: i32 = TokenType::HOUR.mask() | TokenType::MINUTE.mask();
pub const INTERVAL_MASK_DHM: i32 =
    TokenType::DAY.mask() | TokenType::HOUR.mask() | TokenType::MINUTE.mask();
pub const INTERVAL_MASK_S: i32 = TokenType::SECOND.mask();
pub const INTERVAL_MASK_MS: i32 = TokenType::MINUTE.mask() | TokenType::SECOND.mask();
pub const INTERVAL_MASK_HMS: i32 =
    TokenType::HOUR.mask() | TokenType::MINUTE.mask() | TokenType::SECOND.mask();
pub const INTERVAL_MASK_DHMS: i32 = TokenType::DAY.mask()
    | TokenType::HOUR.mask()
    | TokenType::MINUTE.mask()
    | TokenType::SECOND.mask();

pub const INTERVAL_FULL_RANGE: i32 = 0x7FFF;
pub const INTERVAL_RANGE_MASK: i32 = 0x7FFF;

pub const INTERVAL_FULL_PRECISION: i32 = 0xFFFF;
pub const INTERVAL_PRECISION_MASK: i32 = 0xFFFF;

const INTERVAL_SCALES: [i64; MAX_INTERVAL_PRECISION + 1] =
    [1_000_000, 100_000, 10_000, 1000, 100, 10, 1];

const INTERVAL_OFFSETS: [i64; MAX_INTERVAL_PRECISION + 1] = [500_000, 50_000, 5_000, 500, 50, 5, 0];

#[inline]
const fn interval_range(t: i32) -> i32 {
    (t >> 16) & INTERVAL_RANGE_MASK
}

#[allow(dead_code)]
#[inline]
const fn interval_type_mod(p: i32, r: i32) -> i32 {
    ((r & INTERVAL_RANGE_MASK) << 16) | (p & INTERVAL_PRECISION_MASK)
}

#[inline]
const fn interval_precision(t: i32) -> i32 {
    t & INTERVAL_PRECISION_MASK
}

/// Interval represents delta time.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(C)]
pub struct Interval {
    /// All time units other than days, months and years.
    time: i64,
    /// Days, after time for alignment.
    day: i32,
    /// Months and years, after time for alignment.
    month: i32,
}

impl Interval {
    /// Construct a `Interval` value.
    #[inline]
    pub const fn new() -> Self {
        Self {
            time: 0,
            day: 0,
            month: 0,
        }
    }

    /// Construct a `Interval` value.
    #[inline]
    pub(crate) const fn from_mdt(month: i32, day: i32, time: i64) -> Self {
        Self { time, day, month }
    }

    #[inline]
    pub(crate) const fn time(&self) -> i64 {
        self.time
    }

    #[inline]
    pub(crate) const fn month(&self) -> i32 {
        self.month
    }

    #[inline]
    pub(crate) const fn day(&self) -> i32 {
        self.day
    }

    /// Constructs `Interval` from year, month, weak, day, hour, minute and second. second can indicate millisecond and microsecond.
    #[inline]
    pub fn try_from_ymwd_hms(
        years: i32,
        months: i32,
        weeks: i32,
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: f64,
    ) -> Result<Interval, DateTimeError> {
        // Reject out-of-range inputs.  We really ought to check the integer
        // inputs as well, but it's not entirely clear what limits to apply.
        if seconds.is_infinite() || seconds.is_nan() {
            return Err(DateTimeError::overflow());
        }

        let months = years.wrapping_mul(MONTHS_PER_YEAR).wrapping_add(months);
        let days = weeks.wrapping_mul(7).wrapping_add(days);
        let secs = unsafe { (seconds * USECS_PER_SEC as f64).to_int_unchecked() };
        let time = (hours as i64)
            .wrapping_mul(SECS_PER_HOUR as i64 * USECS_PER_SEC)
            .wrapping_add((minutes as i64).wrapping_mul(SECS_PER_MINUTE as i64 * USECS_PER_SEC))
            .wrapping_add(secs);

        Ok(Interval::from_mdt(months, days, time))
    }

    /// `Interval` subtracts `Interval`.
    #[inline]
    pub fn add_interval(self, span: Self) -> Result<Self, DateTimeError> {
        let month = self.month.checked_add(span.month);
        let m = match month {
            Some(m) => m,
            None => return Err(DateTimeError::overflow()),
        };

        let day = self.day.checked_add(span.day);
        let d = match day {
            Some(d) => d,
            None => return Err(DateTimeError::overflow()),
        };

        let time = self.time.checked_add(span.time);
        let t = match time {
            Some(t) => t,
            None => return Err(DateTimeError::overflow()),
        };
        Ok(Interval::from_mdt(m, d, t))
    }

    /// Negate of `Interval`.
    #[inline]
    pub fn negate(self) -> Result<Self, DateTimeError> {
        let t_ret = self.time().checked_neg();
        // overflow check copied from int4um.
        let time = match t_ret {
            Some(t) => t,
            None => return Err(DateTimeError::overflow()),
        };

        let d_ret = self.day().checked_neg();
        let day = match d_ret {
            Some(d) => d,
            None => return Err(DateTimeError::overflow()),
        };
        let m_ret = self.month().checked_neg();
        let month = match m_ret {
            Some(m) => m,
            None => return Err(DateTimeError::overflow()),
        };

        Ok(Self::from_mdt(month, day, time))
    }

    /// `Interval` subtracts `Interval`.
    #[inline]
    pub fn sub_interval(self, span: Self) -> Result<Self, DateTimeError> {
        let month = self.month.checked_sub(span.month);
        let m = match month {
            Some(m) => m,
            None => return Err(DateTimeError::overflow()),
        };

        let day = self.day.checked_sub(span.day);
        let d = match day {
            Some(d) => d,
            None => return Err(DateTimeError::overflow()),
        };

        let time = self.time.checked_sub(span.time);
        let t = match time {
            Some(t) => t,
            None => return Err(DateTimeError::overflow()),
        };
        Ok(Interval::from_mdt(m, d, t))
    }

    /// Adjusts `Interval`. so `time` of `Interval` contains less than a whole day, adding
    /// the excess to `day` of `Interval`.  This is useful for
    /// situations (such as non-TZ) where '1 day' = '24 hours' is valid,
    /// e.g. interval subtraction and division.
    #[inline]
    pub fn justify_hours(self) -> Self {
        let mut result = self;
        let (time, whole_day) = time_modulo(result.time, USECS_PER_DAY);
        result.time = time;
        result.day = result.day.wrapping_add(whole_day as i32); // could overflow.
        if result.day > 0 && result.time < 0 {
            result.time += USECS_PER_DAY;
            result.day -= 1;
        } else if result.day < 0 && result.time > 0 {
            result.time -= USECS_PER_DAY;
            result.day += 1;
        }
        result
    }

    /// Adjust `Interval` so `day` of `Interval` contains less than 30 days, adding
    /// the excess to 'month'.
    #[inline]
    pub fn justify_days(self) -> Self {
        let mut result = self;
        let whole_month = result.day / DAYS_PER_MONTH;
        result.day -= whole_month * DAYS_PER_MONTH;
        result.month = result.month.wrapping_add(whole_month);

        if result.month > 0 && result.day < 0 {
            result.day += DAYS_PER_MONTH;
            result.month -= 1;
        } else if result.month < 0 && result.day > 0 {
            result.day -= DAYS_PER_MONTH;
            result.month += 1;
        }
        result
    }

    /// Adjusts `Interval`. so `month`, `day`, and `time` portions are within
    /// customary bounds.  Specifically:
    /// 0 <= abs(time) < 24 hours
    /// 0 <= abs(day)  < 30 days
    ///  Also, the sign bit on all three fields is made equal, so either
    ///  all three fields are negative or all are positive.
    #[inline]
    pub fn justify_interval(self) -> Self {
        let mut result = self;
        let (time, whole_day) = time_modulo(result.time, USECS_PER_DAY);
        result.time = time;
        result.day = result.day.wrapping_add(whole_day as i32); // could overflow.
        let whole_month = result.day / DAYS_PER_MONTH;
        result.day = result.day.wrapping_sub(whole_month * DAYS_PER_MONTH);
        result.month = result.month.wrapping_add(whole_month);

        if result.month > 0 && (result.day < 0 || (result.day == 0 && result.time < 0)) {
            result.day += DAYS_PER_MONTH;
            result.month -= 1;
        } else if result.month < 0 && (result.day > 0 || (result.day == 0 && result.time > 0)) {
            result.day -= DAYS_PER_MONTH;
            result.month += 1;
        }

        if result.day > 0 && result.time < 0 {
            result.time += USECS_PER_DAY;
            result.day -= 1;
        } else if result.day < 0 && result.time > 0 {
            result.time -= USECS_PER_DAY;
            result.day += 1;
        }
        result
    }

    /// `Interval` multiply float64.
    #[inline]
    pub fn mul_f64(self, factor: f64) -> Result<Self, DateTimeError> {
        let orig_month = self.month;
        let orig_day = self.day;
        let result_month = self.month as f64 * factor;
        if result_month.is_nan()
            || result_month > std::i32::MAX as f64
            || result_month < std::i32::MIN as f64
        {
            return Err(DateTimeError::overflow());
        }

        let result_day = self.day as f64 * factor;
        if result_day.is_nan()
            || result_day > std::i32::MAX as f64
            || result_day < std::i32::MIN as f64
        {
            return Err(DateTimeError::overflow());
        }

        let mut result = Interval::from_mdt(result_month as i32, result_day as i32, 0);

        // The above correctly handles the whole-number part of the month and day
        // products, but we have to do something with any fractional part
        // resulting when the factor is non-integral.  We cascade the fractions
        // down to lower units using the conversion factors DAYS_PER_MONTH and
        // SECS_PER_DAY.  Note we do NOT cascade up, since we are not forced to do
        // so by the representation.  The user can choose to cascade up later,
        // using justify_hours and/or justify_days.

        // Fractional months full days into days.
        //
        // Floating point calculation are inherently imprecise, so these
        // calculations are crafted to produce the most reliable result possible.
        // TSROUND() is needed to more accurately produce whole numbers where
        // appropriate.
        let month_remainder = orig_month as f64 * factor - result.month as f64;
        let month_remainder_days = month_remainder * DAYS_PER_MONTH as f64;
        let month_remainder_days = timestamp_round(month_remainder_days);
        let sec_remainder = (orig_day as f64 * factor - result.day as f64 + month_remainder_days
            - month_remainder_days as i32 as f64)
            * SECS_PER_DAY as f64;
        let mut sec_remainder = timestamp_round(sec_remainder);

        // Might have 24:00:00 hours due to rounding, or >24 hours because of time
        // cascade from months and days.  It might still be >24 if the combination
        // of cascade and the seconds factor operation itself.
        if sec_remainder.abs() >= SECS_PER_DAY as f64 {
            result.day += (sec_remainder / SECS_PER_DAY as f64) as i32;
            sec_remainder -= (((sec_remainder / SECS_PER_DAY as f64) as i32) * SECS_PER_DAY) as f64;
        }

        // cascade units down
        result.day += month_remainder_days as i32;
        let result_time =
            (self.time as f64 * factor + sec_remainder * USECS_PER_SEC as f64).round();
        if result_time > std::i64::MAX as f64 || result_time < std::i64::MIN as f64 {
            return Err(DateTimeError::overflow());
        }

        result.time = result_time as i64;

        Ok(result)
    }

    /// `Interval` divides float64.
    #[inline]
    pub fn div_f64(self, factor: f64) -> Result<Self, DateTimeError> {
        let orig_month = self.month;
        let orig_day = self.day;
        if factor == 0.0 {
            return Err(DateTimeError::invalid(format!(
                "interval: {:?} divide zero",
                self
            )));
        }
        let month = (self.month as f64 / factor) as i32;
        let day = (self.day as f64 / factor) as i32;

        let mut result = Interval::from_mdt(month, day, 0);
        // Fractional months full days into days.  See comment in interval_mul().

        let mut month_remainder_days =
            (orig_month as f64 / factor - result.month as f64) * DAYS_PER_MONTH as f64;
        month_remainder_days = timestamp_round(month_remainder_days);
        let mut sec_remainder = (orig_day as f64 / factor - result.day as f64
            + month_remainder_days
            - month_remainder_days as i32 as f64)
            * SECS_PER_DAY as f64;
        sec_remainder = timestamp_round(sec_remainder);
        if sec_remainder.abs() >= SECS_PER_DAY as f64 {
            result.day += (sec_remainder / SECS_PER_DAY as f64) as i32;
            sec_remainder -= (((sec_remainder / SECS_PER_DAY as f64) as i32) * SECS_PER_DAY) as f64;
        }

        // cascade units down
        result.day += month_remainder_days as i32;
        result.time =
            (self.time as f64 / factor + sec_remainder * USECS_PER_SEC as f64).round() as i64;
        Ok(result)
    }

    /// Constructs `Interval`from `PgTime`.
    #[inline]
    pub(crate) fn from_pgtime(tm: &PgTime, fsec: i64) -> Result<Self, DateTimeError> {
        let total_months = tm.year as f64 * MONTHS_PER_YEAR as f64 + tm.mon as f64;
        if total_months > std::i32::MAX as f64 || total_months < std::i32::MIN as f64 {
            return Err(DateTimeError::overflow());
        }

        let month = total_months as i32;
        let day = tm.mday;
        let time =
            (((((tm.hour * 60) + tm.min) as i64 * 60) + tm.sec as i64) * USECS_PER_SEC) + fsec;
        Ok(Self::from_mdt(month, day, time))
    }

    /// Converts an `Interval` data type to a `PgTime` structure.
    #[inline]
    pub(crate) fn to_pgtime(&self, tm: &mut PgTime, fsec: &mut i64) -> Result<(), DateTimeError> {
        tm.year = self.month / MONTHS_PER_YEAR;
        tm.mon = self.month % MONTHS_PER_YEAR;
        tm.mday = self.day;
        let mut time = self.time;

        let mut tfrac = time / USECS_PER_HOUR;
        time -= tfrac * USECS_PER_HOUR;
        if tfrac < std::i32::MIN as i64 || tfrac > std::i32::MAX as i64 {
            return Err(DateTimeError::overflow());
        }
        tm.hour = tfrac as i32;
        tfrac = time / USECS_PER_MINUTE;
        time -= tfrac * USECS_PER_MINUTE;
        tm.min = tfrac as i32;
        tfrac = time / USECS_PER_SEC;
        *fsec = time - (tfrac * USECS_PER_SEC);
        tm.sec = tfrac as i32;
        Ok(())
    }

    /// Adjusts `Interval` for specified precision, in both year to second range and sub-second precision.
    pub(crate) fn adjust_by_typmod(&mut self, typmod: i32) -> Result<(), DateTimeError> {
        // Unspecified range and precision? Then not necessary to adjust. Setting
        // typmod to -1 is the convention for all data types.
        if typmod >= 0 {
            let range = interval_range(typmod);
            let precision = interval_precision(typmod);

            // Our interpretation of intervals with a limited set of fields is
            // that fields to the right of the last one specified are zeroed out,
            // but those to the left of it remain valid.  Thus for example there
            // is no operational difference between INTERVAL YEAR TO MONTH and
            // INTERVAL MONTH.  In some cases we could meaningfully enforce that
            // higher-order fields are zero; for example INTERVAL DAY could reject
            // nonzero "month" field.  However that seems a bit pointless when we
            // can't do it consistently.  (We cannot enforce a range limit on the
            // highest expected field, since we do not have any equivalent of
            // SQL's <interval leading field precision>.)  If we ever decide to
            // revisit this, interval_transform will likely require adjusting.
            //
            // Note: before PG 8.4 we interpreted a limited set of fields as
            // actually causing a "modulo" operation on a given value, potentially
            // losing high-order as well as low-order information.  But there is
            // no support for such behavior in the standard, and it seems fairly
            // undesirable on data consistency grounds anyway.  Now we only
            // perform truncation or rounding of low-order fields.
            if range == INTERVAL_FULL_RANGE {
                // Do nothing.
            } else if range == TokenType::YEAR.mask() {
                self.month = (self.month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
                self.day = 0;
                self.time = 0;
            }
            // YEAR TO MONTH.
            else if range == TokenType::MONTH.mask()
                || range == (TokenType::YEAR.mask() | TokenType::MONTH.mask())
            {
                self.day = 0;
                self.time = 0;
            } else if range == TokenType::DAY.mask() {
                self.time = 0;
            } else if range == TokenType::HOUR.mask() {
                self.time = (self.time / USECS_PER_HOUR) * USECS_PER_HOUR;
            } else if range == TokenType::MINUTE.mask() {
                self.time = (self.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
            } else if range == TokenType::SECOND.mask() {
                // fractional-second rounding will be dealt with below.
            }
            // DAY TO HOUR.
            else if range == (TokenType::DAY.mask() | TokenType::HOUR.mask()) {
                self.time = (self.time / USECS_PER_HOUR) * USECS_PER_HOUR;
            }
            /* DAY TO MINUTE */
            else if range
                == (TokenType::DAY.mask() | TokenType::HOUR.mask() | TokenType::MINUTE.mask())
            {
                self.time = (self.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
            }
            // DAY TO SECOND
            else if range
                == (TokenType::DAY.mask()
                    | TokenType::HOUR.mask()
                    | TokenType::MINUTE.mask()
                    | TokenType::SECOND.mask())
            {
                // fractional-second rounding will be dealt with below
            }
            // HOUR TO MINUTE.
            else if range == (TokenType::HOUR.mask() | TokenType::MINUTE.mask()) {
                self.time = (self.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
            }
            // HOUR TO SECOND or MINUTE TO SECOND.
            else if range
                == (TokenType::HOUR.mask() | TokenType::MINUTE.mask() | TokenType::SECOND.mask())
                || range == (TokenType::MINUTE.mask() | TokenType::SECOND.mask())
            {
                // fractional-second rounding will be dealt with below.
            } else {
                return Err(DateTimeError::invalid(format!(
                    "range: {:?} is valid",
                    range
                )));
            }

            // Need to adjust sub-second precision?.
            if precision != INTERVAL_FULL_PRECISION {
                if precision < 0 || precision > MAX_INTERVAL_PRECISION as i32 {
                    return Err(DateTimeError::overflow());
                }

                if self.time >= 0 {
                    self.time = ((self.time + INTERVAL_OFFSETS[precision as usize])
                        / INTERVAL_SCALES[precision as usize])
                        * INTERVAL_SCALES[precision as usize];
                } else {
                    self.time = -(((-self.time + INTERVAL_OFFSETS[precision as usize])
                        / INTERVAL_SCALES[precision as usize])
                        * INTERVAL_SCALES[precision as usize]);
                }
            }
        }

        Ok(())
    }

    /// Parses `Internal` from string.
    #[allow(clippy::uninit_assumed_init)]
    pub fn try_from_str(
        s: &str,
        type_mod: i32,
        format_type: IntervalStyle,
    ) -> Result<Self, DateTimeError> {
        let s = s.as_bytes();
        let mut fields: [&[u8]; MAX_DATE_FIELDS] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut work_buf: [u8; MAX_DATE_LEN] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut f_types: [TokenField; MAX_DATE_FIELDS] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let mut d_type = TokenField::Number;
        let mut fsec = 0;
        let mut tm = PgTime::new();

        let range = if type_mod >= 0 {
            interval_range(type_mod)
        } else {
            INTERVAL_FULL_RANGE
        };

        let nf = parse_date_time(s, &mut work_buf, &mut fields, &mut f_types, MAX_DATE_FIELDS)?;
        let err = decode_interval(
            &fields[0..nf],
            &f_types[0..nf],
            range,
            &mut d_type,
            &mut tm,
            &mut fsec,
            format_type,
        );
        if let Err(e) = err {
            if e.is_invalid() {
                decode_iso8601_interval(s, &mut d_type, &mut tm, &mut fsec)?;
            } else {
                return Err(e);
            }
        }

        let mut result = match d_type {
            TokenField::Delta => Interval::from_pgtime(&tm, fsec)?,
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "date: {:?} is invalid",
                    d_type
                )))
            }
        };
        result.adjust_by_typmod(type_mod)?;

        Ok(result)
    }

    /// Converts `Interval` to string.
    #[inline]
    pub fn format(self, interval_format: IntervalStyle) -> Result<String, DateTimeError> {
        //interval_to_string(self, interval_format)
        let mut buf = Vec::with_capacity(128);
        unsafe { buf.set_len(128) };
        let mut tm: PgTime = PgTime::new();
        let mut fsec = 0;

        self.to_pgtime(&mut tm, &mut fsec)?;
        let len = format_interval(buf.as_mut_slice(), &tm, fsec, interval_format)?;
        unsafe {
            buf.set_len(len);
            Ok(String::from_utf8_unchecked(buf))
        }
    }
}

/// Interpret time structure as a delta time and convert to string.
///
/// Support "traditional Postgres" and ISO-8601 styles.
/// Actually, afaik ISO does not address time interval formatting,
/// but this looks similar to the spec for absolute date/time.
/// - thomas 1998-04-30
///
/// Actually, afaik, ISO 8601 does specify formats for "time
/// intervals...[of the]...format with time-unit designators", which
/// are pretty ugly.  The format looks something like
///   P1Y1M1DT1H1M1.12345S
/// but useful for exchanging data with computers instead of humans.
/// - ron 2003-07-14
///
/// And ISO's SQL 2008 standard specifies standards for
/// "year-month literal"s (that look like '2-3') and
/// "day-time literal"s (that look like ('4 5:6:7')
#[allow(clippy::cognitive_complexity)]
fn format_interval(
    s: &mut [u8],
    tm: &PgTime,
    mut fsec: i64,
    style: IntervalStyle,
) -> Result<usize, DateTimeError> {
    let mut year = tm.year;
    let mut mon = tm.mon;
    let mut mday = tm.mday;
    let mut hour = tm.hour;
    let mut min = tm.min;
    let mut sec = tm.sec;
    let mut is_before = false;
    let mut is_zero = true;

    let mut offset = 0;
    // The sign of year and month are guaranteed to match, since they are
    // stored internally as "month". But we'll need to check for is_before and
    // is_zero when determining the signs of day and hour/minute/seconds
    // fields.
    match style {
        // SQL Standard interval format.
        IntervalStyle::SQLStandard => {
            let has_negative =
                year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
            let has_positive =
                year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
            let has_year_month = year != 0 || mon != 0;
            let has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
            let has_day = mday != 0;
            let sql_standard_value =
                !(has_negative && has_positive || has_year_month && has_day_time);
            // SQL Standard wants only 1 "<sign>" preceding the whole
            // interval. but can't do that if mixed signs.
            if has_negative && sql_standard_value {
                offset = set_one_byte(s, offset, b'-');
                year = -year;
                mon = -mon;
                mday = -mday;
                hour = -hour;
                min = -min;
                sec = -sec;
                fsec = -fsec;
            }

            if !has_negative && !has_positive {
                offset = set_one_byte(s, offset, b'0');
                Ok(offset)
            } else if !sql_standard_value {
                // For non sql-standard interval values, force outputting
                // the signs to avoid ambiguities with intervals with
                // mixed sign components.

                let year_sign = if year < 0 || mon < 0 { b'-' } else { b'+' };
                let day_sign = if mday < 0 { b'-' } else { b'+' };
                let sec_sign = if hour < 0 || min < 0 || sec < 0 || fsec < 0 {
                    b'-'
                } else {
                    b'+'
                };
                offset = set_one_byte(s, offset, year_sign);
                offset += pg_ltostr(&mut s[offset..], year.abs());
                offset = set_one_byte(s, offset, b'-');
                offset += pg_ltostr(&mut s[offset..], mon.abs());
                offset = set_one_byte(s, offset, b' ');
                offset = set_one_byte(s, offset, day_sign);
                offset += pg_ltostr(&mut s[offset..], mday.abs());
                offset = set_one_byte(s, offset, b' ');
                offset = set_one_byte(s, offset, sec_sign);
                offset += pg_ltostr(&mut s[offset..], hour.abs());
                offset = set_one_byte(s, offset, b':');

                offset += pg_ltostr_zeropad(&mut s[offset..], min.abs(), 2);
                offset = set_one_byte(s, offset, b':');
                offset += append_seconds(
                    &mut s[offset..],
                    sec,
                    fsec,
                    MAX_INTERVAL_PRECISION as i32,
                    true,
                );
                Ok(offset)
            } else if has_year_month {
                offset += pg_ltostr(&mut s[offset..], year);
                offset = set_one_byte(s, offset, b'-');
                offset += pg_ltostr(&mut s[offset..], mon);
                Ok(offset)
            } else if has_day {
                offset += pg_ltostr(&mut s[offset..], mday);
                offset = set_one_byte(s, offset, b' ');
                offset += pg_ltostr(&mut s[offset..], hour);
                offset = set_one_byte(s, offset, b':');
                offset += pg_ltostr_zeropad(&mut s[offset..], min, 2);
                offset = set_one_byte(s, offset, b':');
                offset += append_seconds(
                    &mut s[offset..],
                    sec,
                    fsec,
                    MAX_INTERVAL_PRECISION as i32,
                    true,
                );
                Ok(offset)
            } else {
                offset += pg_ltostr(&mut s[offset..], hour);
                offset = set_one_byte(s, offset, b':');
                offset += pg_ltostr_zeropad(&mut s[offset..], min, 2);
                offset = set_one_byte(s, offset, b':');
                offset += append_seconds(
                    &mut s[offset..],
                    sec,
                    fsec,
                    MAX_INTERVAL_PRECISION as i32,
                    true,
                );
                Ok(offset)
            }
        }
        // ISO 8601 "time-intervals by duration only"
        IntervalStyle::ISO8601 => {
            //special-case zero to avoid printing nothing.
            if year == 0 && mon == 0 && mday == 0 && hour == 0 && min == 0 && sec == 0 && fsec == 0
            {
                offset = copy_slice(s, offset, b"PT0S");
                return Ok(offset);
            }
            offset = set_one_byte(s, offset, b'P');
            offset += add_iso8601_int_part(&mut s[offset..], year, b'Y')?;
            offset += add_iso8601_int_part(&mut s[offset..], mon, b'M')?;
            offset += add_iso8601_int_part(&mut s[offset..], mday, b'D')?;
            if hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                offset = set_one_byte(s, offset, b'T');
            }
            offset += add_iso8601_int_part(&mut s[offset..], hour, b'H')?;
            offset += add_iso8601_int_part(&mut s[offset..], min, b'M')?;
            if sec != 0 || fsec != 0 {
                if sec < 0 || fsec < 0 {
                    offset = set_one_byte(s, offset, b'-');
                }
                offset += append_seconds(
                    &mut s[offset..],
                    sec,
                    fsec,
                    MAX_INTERVAL_PRECISION as i32,
                    false,
                );
                offset = set_one_byte(s, offset, b'S');
                Ok(offset)
            } else {
                Ok(offset)
            }
        }
        // Compatible with postgresql < 8.4 when DateStyle = 'iso'.
        IntervalStyle::Postgres => {
            offset += add_postgres_int_part(
                &mut s[offset..],
                year,
                b"year",
                &mut is_zero,
                &mut is_before,
            )?;

            // Ideally we should spell out "month" like we do for "year" and
            // "day".  However, for backward compatibility, we can't easily
            // fix this.  bjm 2011-05-24
            offset +=
                add_postgres_int_part(&mut s[offset..], mon, b"mon", &mut is_zero, &mut is_before)?;
            offset += add_postgres_int_part(
                &mut s[offset..],
                mday,
                b"day",
                &mut is_zero,
                &mut is_before,
            )?;
            if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;
                if !is_zero {
                    offset = set_one_byte(s, offset, b' ');
                }
                if minus {
                    offset = set_one_byte(s, offset, b'-');
                } else if is_before {
                    offset = set_one_byte(s, offset, b'+');
                }

                offset += pg_ltostr_zeropad(&mut s[offset..], hour.abs(), 2);
                offset = set_one_byte(s, offset, b':');
                offset += pg_ltostr_zeropad(&mut s[offset..], min.abs(), 2);
                offset = set_one_byte(s, offset, b':');
                offset += append_seconds(
                    &mut s[offset..],
                    sec,
                    fsec,
                    MAX_INTERVAL_PRECISION as i32,
                    true,
                );
                Ok(offset)
            } else {
                Ok(offset)
            }
        }
        // Compatible with postgresql < 8.4 when DateStyle != 'iso'.
        _ => {
            offset = set_one_byte(s, offset, b'@');
            offset += add_verbose_int_part(
                &mut s[offset..],
                year,
                b"year",
                &mut is_zero,
                &mut is_before,
            )?;
            offset +=
                add_verbose_int_part(&mut s[offset..], mon, b"mon", &mut is_zero, &mut is_before)?;
            offset +=
                add_verbose_int_part(&mut s[offset..], mday, b"day", &mut is_zero, &mut is_before)?;
            offset += add_verbose_int_part(
                &mut s[offset..],
                hour,
                b"hour",
                &mut is_zero,
                &mut is_before,
            )?;
            offset +=
                add_verbose_int_part(&mut s[offset..], min, b"min", &mut is_zero, &mut is_before)?;
            if sec != 0 || fsec != 0 {
                offset = set_one_byte(s, offset, b' ');
                if sec < 0 || (sec == 0 && fsec < 0) {
                    if is_zero {
                        is_before = true;
                    } else if !is_before {
                        offset = set_one_byte(s, offset, b'-');
                    }
                } else if is_before {
                    offset = set_one_byte(s, offset, b'-');
                }
                offset += append_seconds(
                    &mut s[offset..],
                    sec,
                    fsec,
                    MAX_INTERVAL_PRECISION as i32,
                    false,
                );
                offset = copy_slice(s, offset, b" sec");
                if sec.abs() != 1 || fsec != 0 {
                    offset = set_one_byte(s, offset, b's');
                }

                is_zero = false;
            }
            // Identically zero? then put in a unitless zero.
            if is_zero {
                offset = copy_slice(s, offset, b" 0");
            }

            if is_before {
                offset = copy_slice(s, offset, b" ago");
            }
            Ok(offset)
        }
    }
}

/// Helper functions to avoid duplicated code in EncodeInterval.
/// Append an ISO-8601-style interval field, but only if value isn't zero.
#[inline]
fn add_iso8601_int_part(s: &mut [u8], value: i32, units: u8) -> Result<usize, DateTimeError> {
    if value == 0 {
        return Ok(0);
    }

    let mut offset = 0;
    offset += pg_ltostr(&mut s[offset..], value);
    offset = set_one_byte(s, offset, units);
    Ok(offset)
}

/// Append a postgres-style interval field, but only if value isn't zero.
#[inline]
fn add_postgres_int_part(
    s: &mut [u8],
    value: i32,
    units: &[u8],
    is_zero: &mut bool,
    is_before: &mut bool,
) -> Result<usize, DateTimeError> {
    let mut offset = 0;
    if value == 0 {
        return Ok(offset);
    }
    if !*is_zero {
        offset = set_one_byte(s, offset, b' ');
    }

    if *is_before && value > 0 {
        offset = set_one_byte(s, offset, b'+');
    }
    offset += pg_ltostr(&mut s[offset..], value);
    offset = set_one_byte(s, offset, b' ');
    offset = copy_slice(s, offset, units);

    if value != 1 {
        offset = set_one_byte(s, offset, b's');
    }

    // Each nonzero field sets is_before for (only) the next one.  This is a
    // tad bizarre but it's how it worked before...
    *is_before = value < 0;
    *is_zero = false;
    Ok(offset)
}

/// Append a verbose-style interval field, but only if value isn't zero.
#[inline]
fn add_verbose_int_part(
    s: &mut [u8],
    value: i32,
    units: &[u8],
    is_zero: &mut bool,
    is_before: &mut bool,
) -> Result<usize, DateTimeError> {
    if value == 0 {
        return Ok(0);
    }

    let mut offset = 0;
    offset = set_one_byte(s, offset, b' ');
    // first nonzero value sets is_before.
    if *is_zero {
        *is_before = value < 0;
        offset += pg_ltostr(&mut s[offset..], value.abs());
    } else if *is_before {
        offset += pg_ltostr(&mut s[offset..], -value);
    } else {
        offset += pg_ltostr(&mut s[offset..], value);
    };
    offset = set_one_byte(s, offset, b' ');
    offset = copy_slice(s, offset, units);
    if value != 1 {
        offset = set_one_byte(s, offset, b's');
    }
    *is_zero = false;
    Ok(offset as usize)
}

/// Converts `Time` to `Interval`.
impl From<Time> for Interval {
    #[inline]
    fn from(value: Time) -> Self {
        Interval::from_mdt(0, 0, value.value())
    }
}

impl From<Interval> for i128 {
    #[inline]
    fn from(span: Interval) -> Self {
        let mut v = span.time() as i128;
        v += span.day() as i128 * USECS_PER_DAY as i128;
        v += span.month() as i128 * DAYS_PER_MONTH as i128 * USECS_PER_DAY as i128;
        v
    }
}

///Used for no overflow checked.
impl Add<Interval> for Interval {
    type Output = Interval;

    #[inline]
    fn add(self, rhs: Interval) -> Self::Output {
        self.add_interval(rhs).unwrap()
    }
}

impl PartialOrd for Interval {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let left: i128 = From::from(*self);
        let right: i128 = From::from(*other);
        left.partial_cmp(&right)
    }
}

impl Ord for Interval {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let left: i128 = From::from(*self);
        let right: i128 = From::from(*other);
        left.cmp(&right)
    }
}

impl DateTime for Interval {
    /// Extracts specified field from `Interval`.
    #[inline]
    fn date_part(&self, ty: FieldType, unit: DateUnit) -> Result<Option<f64>, DateTimeError> {
        match ty {
            FieldType::Unit => {
                let mut tm: PgTime = PgTime::new();
                let mut fsec = 0;
                self.to_pgtime(&mut tm, &mut fsec)?;
                match unit {
                    DateUnit::MicroSec => Ok(Some(tm.sec as f64 * 1_000_000.0 + fsec as f64)),
                    DateUnit::MilliSec => Ok(Some(tm.sec as f64 * 1000.0 + fsec as f64 / 1000.0)),
                    DateUnit::Second => Ok(Some(tm.sec as f64 + fsec as f64 / 1_000_000.0)),
                    DateUnit::Minute => Ok(Some(tm.min as f64)),
                    DateUnit::Hour => Ok(Some(tm.hour as f64)),
                    DateUnit::Day => Ok(Some(tm.mday as f64)),
                    DateUnit::Month => Ok(Some(tm.mon as f64)),
                    DateUnit::Quarter => Ok(Some(((tm.mon / 3) + 1) as f64)),
                    DateUnit::Year => Ok(Some(tm.year as f64)),
                    DateUnit::Decade => Ok(Some((tm.year / 10) as f64)),
                    DateUnit::Century => Ok(Some((tm.year / 100) as f64)),
                    DateUnit::Millennium => Ok(Some((tm.year / 1000) as f64)),
                    _ => Err(DateTimeError::invalid(format!(
                        "unit: {:?} is invalid",
                        unit
                    ))),
                }
            }
            FieldType::Epoch => {
                if unit == DateUnit::Epoch {
                    let mut result = self.time as f64 / 1_000_000.0;
                    result += (DAYS_PER_YEAR as f64 * SECS_PER_DAY as f64)
                        * (self.month / MONTHS_PER_YEAR) as f64;
                    result += (DAYS_PER_MONTH as f64 * SECS_PER_DAY as f64)
                        * (self.month % MONTHS_PER_YEAR) as f64;
                    result += SECS_PER_DAY as f64 * self.day as f64;
                    Ok(Some(result))
                } else {
                    Err(DateTimeError::invalid(format!(
                        "token ty: {:?} is invalid",
                        ty
                    )))
                }
            }
        }
    }

    /// Checks whether `Interval` is finite.
    #[inline]
    fn is_finite(&self) -> bool {
        true
    }

    /// Truncates `Interval` to specified units.
    #[inline]
    fn truncate(&self, ty: FieldType, unit: DateUnit) -> Result<Self, DateTimeError> {
        match ty {
            FieldType::Unit => {
                let mut tm = PgTime::new();
                let mut fsec = 0;
                self.to_pgtime(&mut tm, &mut fsec)?;
                match unit {
                    DateUnit::Millennium => tm.truncate_interval_millennium(&mut fsec),
                    DateUnit::Century => tm.truncate_interval_century(&mut fsec),
                    DateUnit::Decade => tm.truncate_interval_decade(&mut fsec),
                    DateUnit::Year => tm.truncate_interval_year(&mut fsec),
                    DateUnit::Quarter => tm.truncate_interval_quarter(&mut fsec),
                    DateUnit::Month => tm.truncate_interval_month(&mut fsec),
                    DateUnit::Day => tm.truncate_day(&mut fsec),
                    DateUnit::Hour => tm.truncate_hour(&mut fsec),
                    DateUnit::Minute => (&mut tm).truncate_minute(&mut fsec),
                    DateUnit::Second => (&mut tm).truncate_sec(&mut fsec),
                    DateUnit::MilliSec => (&mut tm).truncate_milli_sec(&mut fsec),
                    DateUnit::MicroSec => (&mut tm).truncate_micro_sec(&mut fsec),
                    _ => {
                        return Err(DateTimeError::invalid(format!(
                            "date unit: {:?} is invalid",
                            unit
                        )))
                    }
                }

                Interval::from_pgtime(&tm, fsec)
            }
            _ => Err(DateTimeError::invalid(format!("type: {:?} is invalid", ty))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DateTimeError;
    use crate::{
        Date, DateOrder, DateStyle, DateTime, DateUnit, FieldType, Interval, IntervalStyle,
        Timestamp,
    };

    #[test]
    fn test_interval_from_str() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_str("P1987-07-08-T", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P-1987-07-08T-06:40:50.02", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str(
            "P198999999997-07-08T06:40:50.02",
            -1,
            IntervalStyle::ISO8601,
        );
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987/07/08T06:40:50.02r", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987-07/08T06:40:50.02r", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987-07-08Y06:40:50.02", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("P19870708T06:40:50.02r", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("P1987Y", -1, IntervalStyle::ISO8601)?;
        assert_eq!(interval.format(IntervalStyle::SQLStandard)?, "1987-0");

        let interval = Interval::try_from_str("P19870708T06:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 7 mons 8 days 06:40:50.02");

        let interval = Interval::try_from_str("@  -8 month 1 millennium 9 century 8 decade 7 year 7 day 6 hour 40 minute 50 second 20 millisecond",
                                              -1, IntervalStyle::SQLStandard)?;
        let s = interval.format(IntervalStyle::SQLStandard)?;
        assert_eq!(s, "-1987-8 -7 -6:40:50.02");

        let interval = Interval::try_from_str("@  -1 millennium 9 century 8 decade 7 year 8 month 7 day 6 hour 40 minute 50 second 20 millisecond",
                                              -1, IntervalStyle::SQLStandard)?;
        let s = interval.format(IntervalStyle::SQLStandard)?;
        assert_eq!(s, "-13-8 -7 -6:40:50.02");
        let interval =
            Interval::try_from_str("@  -7 year 8 month", -1, IntervalStyle::SQLStandard)?;
        assert_eq!(interval.format(IntervalStyle::SQLStandard)?, "-7-8");

        let interval = Interval::try_from_str("@  7 year 8 month", -1, IntervalStyle::ISO8601)?;
        assert_eq!(interval.format(IntervalStyle::SQLStandard)?, "7-8");

        let interval = Interval::try_from_str("@  7 day", -1, IntervalStyle::ISO8601)?;
        assert_eq!(interval.format(IntervalStyle::SQLStandard)?, "7 0:00:00");

        let interval = Interval::try_from_str(
            "@  6 hour 40 minute 50 second 20 millisecond",
            -1,
            IntervalStyle::ISO8601,
        )?;

        let s = interval.format(IntervalStyle::SQLStandard)?;
        assert_eq!(s, "6:40:50.02");

        let interval = Interval::try_from_str("@ 78.789.45 years", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@ 78.789_45 years", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@ J20045", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@ @", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("invalid", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("6:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 6 hours 40 mins 50.02 secs");

        let interval =
            Interval::try_from_str("+1987-8 +7 +6:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 8 mons 7 days 6 hours 40 mins 50.02 secs");

        let interval =
            Interval::try_from_str("-1987-8 -7 -6:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(
            s,
            "@ 1987 years 8 mons 7 days 6 hours 40 mins 50.02 secs ago"
        );

        let interval =
            Interval::try_from_str("@ 1 millennium 9 century", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1900 years");

        let interval =
            Interval::try_from_str("@ 1 millennium 9 century ago", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1900 years ago");

        let interval = Interval::try_from_str("@ 1.1 millennium 9.1 century 8.1 decade 7 year 8 month 1 week 7 day 6 hour 40 minute 50 second 20 millisecond",
                                              -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 2098 years 8 mons 14 days 6 hours 40 mins 50.02 secs");

        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8 month 7 day 6 hour 40 minute 50 second 20 millisecond",
                                          -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 8 mons 7 days 6 hours 40 mins 50.02 secs");

        let s = interval.format(IntervalStyle::SQLStandard)?;
        assert_eq!(s, "+1987-8 +7 +6:40:50.02");

        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 06:40:50.02");

        let s = interval.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P1987Y8M7DT6H40M50.02S");

        let interval = Interval::try_from_str("@ -1 millennium -9 century 8 decade 7 year 8 month 7 day 6 hour 40 minute 50 second 20 millisecond",
                                              -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(
            s,
            "@ 1812 years 4 mons -7 days -6 hours -40 mins -50.02 secs ago"
        );

        let s = interval.format(IntervalStyle::SQLStandard)?;
        assert_eq!(s, "-1812-4 +7 +6:40:50.02");

        let interval = Interval::try_from_str(
            "@ 1 millennium 9 century 9 century ago",
            -1,
            IntervalStyle::ISO8601,
        );
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@ 1 yue", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("infinity", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987Y8M1W7DT6H40M50.02S", -1, IntervalStyle::ISO8601)?;

        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 14 days 06:40:50.02");

        let interval = Interval::try_from_str("P1987-8-7T06:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 06:40:50.02");

        let interval = Interval::try_from_str("P1987T06:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 06:40:50.02");

        let interval = Interval::try_from_str("P1987-8T06:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 06:40:50.02");

        let interval = Interval::try_from_str("P1987-8-7", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days");

        let interval = Interval::try_from_str("P1987", -1, IntervalStyle::ISO8601)?;

        assert_eq!(interval.format(IntervalStyle::Postgres)?, "1987 years");

        let interval = Interval::try_from_str("P1987-8", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons");

        let interval = Interval::try_from_str("P1987-8-7T06:40:50.02", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 06:40:50.02");

        let interval =
            Interval::try_from_str("P1987-8-7T064020.34567", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 06:40:20.34567");

        let interval = Interval::try_from_str("P1987-8-7T23.3", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 23:18:00");

        let interval =
            Interval::try_from_str("P1987-8-7T064020.34567", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 06:40:20.34567");

        let interval = Interval::try_from_str("P19870807.23456", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "1987 years 8 mons 7 days 05:37:45.984142");

        let interval = Interval::try_from_str("P807.23456", -1, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::Postgres)?;
        assert_eq!(s, "807 years 2 mons");

        let interval = Interval::try_from_str("P", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());
        let interval = Interval::try_from_str("P1987-n8-7T06:40:50.02", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987Y8M1W7DT6H40M50.02Y", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987Y8M1N7DT6H40M50.02S", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987Y8M17D4567.8T6H40M50.02S", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval =
            Interval::try_from_str("P1987Y8M17DT6H40M50.02S5678.9", -1, IntervalStyle::ISO8601);
        assert!(interval.is_err());
        Ok(())
    }

    #[test]
    fn test_interval_convert() -> Result<(), DateTimeError> {
        let time = Time::try_from_hms(12, 34, 23.456788)?;
        let interval1: Interval = From::from(time);
        let interval2: Interval = Interval::try_from_ymwd_hms(0, 0, 0, 0, 12, 34, 23.456788)?;
        assert_eq!(interval1, interval2);

        let interval: Interval = Interval::try_from_ymwd_hms(2020, 12, 23, 12, 36, 34, 23.456788)?;
        let time1: Time = From::from(interval);
        let time2 = time;
        assert_eq!(time1, time2);

        let interval: Interval = Interval::try_from_ymwd_hms(2020, 12, 23, 12, -2, 34, 23.456788)?;
        let time1: Time = From::from(interval);
        assert_eq!(time1.format(DateStyle::SQL)?, "22:34:23.456788");

        Ok(())
    }

    #[test]
    fn test_mask() -> Result<(), DateTimeError> {
        let type_mod = interval_type_mod(6, TokenType::YEAR.mask() | TokenType::HOUR.mask());
        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8.1 month 7 day 6 hour 40 minute 50 second 20 millisecond 567 microsecond",
                                              type_mod, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let type_mod = interval_type_mod(6, TokenType::MINUTE.mask() | TokenType::HOUR.mask());
        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8.1 month 7 day 6 hour 40 minute 50 second 20 millisecond 567 microsecond",
                                              type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 8 mons 10 days 6 hours 40 mins");

        let type_mod = interval_type_mod(6, TokenType::MINUTE.mask() | TokenType::HOUR.mask());
        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8.1 month 7 day -6 hour 40 minute 50 second 20 millisecond 567 microsecond",
                                              type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 8 mons 10 days -5 hours -19 mins");

        let type_mod = interval_type_mod(6, TokenType::DAY.mask() | TokenType::HOUR.mask());
        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8.1 month 7 day 6 hour 40 minute 50 second 20 millisecond 567 microsecond",
                                              type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 8 mons 10 days 6 hours");

        let type_mod = interval_type_mod(6, TokenType::MONTH.mask());
        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8.1 month 7 day 6 hour 40 minute 50 second 20 millisecond 567 microsecond",
                                              type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 8 mons");

        let type_mod = interval_type_mod(6, TokenType::YEAR.mask());
        let interval = Interval::try_from_str("@ 1 millennium 9 century 8 decade 7 year 8.1 month 7 day 6 hour 40 minute 50 second 20 millisecond 567 microsecond",
                                              type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_Y);
        let interval = Interval::try_from_str("@ 1987.34", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_YM);
        let interval = Interval::try_from_str("@ 1987-7", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 1987 years 7 mons");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_D);
        let interval = Interval::try_from_str("@ 19", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 19 days");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_DH);
        let interval = Interval::try_from_str("@ 10", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10 hours");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_H);
        let interval = Interval::try_from_str("@ 10", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10 hours");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_MIN);
        let interval = Interval::try_from_str("@ 10", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10 mins");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_S);
        let interval = Interval::try_from_str("@ 10", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10 secs");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_MS);
        let interval = Interval::try_from_str("@ 10", type_mod, IntervalStyle::ISO8601)?;

        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10 secs");

        let type_mod = interval_type_mod(6, INTERVAL_FULL_RANGE);
        let interval = Interval::try_from_str("@ 10", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10 secs");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_HMS);
        let interval = Interval::try_from_str("@ 10.2345", type_mod, IntervalStyle::ISO8601)?;
        let s = interval.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 10.2345 secs");

        let type_mod = interval_type_mod(6, INTERVAL_MASK_YM);
        let interval = Interval::try_from_str("@ 10-h", type_mod, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@ 10-13", type_mod, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        let interval = Interval::try_from_str("@ 10-13", type_mod, IntervalStyle::ISO8601);
        assert!(interval.is_err());
        let s = "@ 102222222222222222222-12";
        let interval = Interval::try_from_str(s, type_mod, IntervalStyle::ISO8601);
        assert!(interval.is_err());

        Ok(())
    }

    #[test]
    fn test_interval_range() -> Result<(), DateTimeError> {
        let time = Interval::try_from_str("-178000000 year", -1, IntervalStyle::ISO8601)?;
        let s = time.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 178000000 years ago");

        let time = Interval::try_from_str("178000000 year", -1, IntervalStyle::ISO8601)?;
        let s = time.format(IntervalStyle::PostgresVerbose)?;
        assert_eq!(s, "@ 178000000 years");

        let time = Interval::try_from_str("-179000000 year", -1, IntervalStyle::ISO8601);
        assert!(time.is_err());

        let time = Interval::try_from_str("-179000000 year", -1, IntervalStyle::ISO8601);
        assert!(time.is_err());
        Ok(())
    }

    #[test]
    fn test_try_from_ymwd_hms() -> Result<(), DateTimeError> {
        let span = Interval::try_from_ymwd_hms(1, 13, 3, 29, 34, 56, std::f64::INFINITY);
        assert!(span.is_err());
        let span = Interval::try_from_ymwd_hms(1, 13, 3, 29, 34, 56, std::f64::NAN);
        assert!(span.is_err());
        let span = Interval::try_from_ymwd_hms(1, 13, 3, 29, 34, 56, 67.8)?;
        let s = span.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2Y1M50DT34H57M7.8S");

        let span = Interval::try_from_ymwd_hms(
            std::i32::MAX,
            std::i32::MAX,
            std::i32::MAX,
            std::i32::MAX,
            std::i32::MAX,
            std::i32::MAX,
            std::f64::MAX,
        )?;
        let s = span.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P178956969Y7M-8DT-378772746H-53M-54.775808S");
        Ok(())
    }

    #[test]
    fn test_interval_date_part() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_ymwd_hms(2001, 2, 0, 16, 20, 38, 40.4567890)?;
        let ret = interval.date_part(FieldType::Epoch, DateUnit::Century);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(2001, 2, 0, 16, 20, 38, 40.4567890)?;
        let ret = interval.date_part(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret, Some(20.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Millennium)?;
        assert_eq!(ret, Some(2.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Decade)?;
        assert_eq!(ret, Some(200.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Day)?;
        assert_eq!(ret, Some(16.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Dow);
        assert!(ret.is_err());

        let ret = interval.date_part(FieldType::Unit, DateUnit::Doy);
        assert!(ret.is_err());

        let ret = interval.date_part(FieldType::Unit, DateUnit::IsoDow);
        assert!(ret.is_err());

        let ret = interval.date_part(FieldType::Unit, DateUnit::IsoYear);
        assert!(ret.is_err());

        let ret = interval.date_part(FieldType::Epoch, DateUnit::Epoch)?;
        assert_eq!(ret, Some(63153398320.45679));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Hour)?;
        assert_eq!(ret, Some(20.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Minute)?;
        assert_eq!(ret, Some(38.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Second)?;
        assert_eq!(ret, Some(40.456789));

        let ret = interval.date_part(FieldType::Unit, DateUnit::MilliSec)?;
        assert_eq!(ret, Some(40456.789));

        let ret = interval.date_part(FieldType::Unit, DateUnit::MicroSec)?;
        assert_eq!(ret, Some(40456789.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Month)?;
        assert_eq!(ret, Some(2.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Quarter)?;
        assert_eq!(ret, Some(1.0));

        let ret = interval.date_part(FieldType::Unit, DateUnit::Week);
        assert!(ret.is_err());

        let ret = interval.date_part(FieldType::Unit, DateUnit::Year)?;
        assert_eq!(ret, Some(2001.0));

        Ok(())
    }

    #[test]
    fn test_interval_tuncate() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_ymwd_hms(2116, 2, 0, 16, 20, 38, 40.4567890)?;
        let ret = interval.truncate(FieldType::Epoch, DateUnit::MicroSec);
        assert!(ret.is_err());
        let ret = interval.truncate(FieldType::Unit, DateUnit::Epoch);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(2116, 2, 0, 16, 20, 38, 40.4567890)?;
        let ret = interval.truncate(FieldType::Unit, DateUnit::Millennium)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2000Y");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Century)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2100Y");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Decade)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2110Y");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Year)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2116Y");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Quarter)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2116Y");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Month)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2116Y2M");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Week);
        assert!(ret.is_err());

        let ret = interval.truncate(FieldType::Unit, DateUnit::Day)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2116Y2M16D");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Hour)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2116Y2M16DT20H");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Minute)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P2116Y2M16DT20H38M");

        let ret = interval.truncate(FieldType::Unit, DateUnit::Second)?;
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116Y2M16DT20H38M40S");

        let ret = interval.truncate(FieldType::Unit, DateUnit::MilliSec)?;
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116Y2M16DT20H38M40.456S");

        let ret = interval.truncate(FieldType::Unit, DateUnit::MicroSec)?;
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116Y2M16DT20H38M40.456789S");
        Ok(())
    }

    #[test]
    fn test_justify() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_ymwd_hms(2116, 2, 0, 80, 80, 38, 40.4567890)?;
        let ret = interval.justify_days();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116Y4M20DT80H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, 2, 0, -2, 20, 38, 40.4567890)?;
        let ret = interval.justify_days();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P1M28DT20H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, -2, 0, 4, 12, 38, 40.4567890)?;
        let ret = interval.justify_days();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P-1M-26DT12H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(2116, 2, 0, 80, 80, 38, 40.4567890)?;
        let ret = interval.justify_hours();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116Y2M83DT8H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, 2, 0, 80, -2, 38, 40.4567890)?;
        let ret = interval.justify_hours();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2M79DT22H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, 2, 0, -80, 48, 38, 40.4567890)?;
        let ret = interval.justify_hours();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2M-77DT-23H-21M-19.543211S");

        let interval = Interval::try_from_ymwd_hms(2116, 2, 0, 80, 80, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116Y4M23DT8H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, 2, 0, -2, 12, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P1M28DT12H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, 2, 0, 0, -8, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P1M29DT16H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(0, -1, 0, 23, 8, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P-6DT-15H-21M-19.543211S");

        let interval = Interval::try_from_ymwd_hms(0, -1, 0, 0, 8, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P-29DT-15H-21M-19.543211S");

        let interval = Interval::try_from_ymwd_hms(2116, 0, 0, 1, -8, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116YT16H38M40.456789S");

        let interval = Interval::try_from_ymwd_hms(2116, 0, 0, -1, 28, 38, 40.4567890)?;
        let ret = interval.justify_interval();
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2116YT4H38M40.456789S");

        Ok(())
    }

    #[test]
    fn test_finite() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_ymwd_hms(2116, 2, 0, 80, 80, 38, 40.4567890)?;
        assert!(!interval.is_infinite());
        assert!(interval.is_finite());
        Ok(())
    }

    #[test]
    fn test_add_operator() -> Result<(), DateTimeError> {
        let timestamp = Timestamp::new(-211813488000000000);
        let time = Time::from(-23);
        let timestamp = timestamp.add_time(time);
        assert!(timestamp.is_err());

        let timestamp = Timestamp::try_from_ymd_hms(2011, 12, 09, 1, 15, 5.456)?;
        let time = Time::try_from_hms(1, 20, 30.23)?;
        let timestamp = timestamp.add_time(time)?;
        let s = timestamp.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2011-12-09 02:35:35.686");

        let date = Date::try_from_ymd(2020, 1, 20)?;
        let date2 = Date::try_from_ymd(2020, 1, 31)?;
        let date1 = date.add_days(11)?;
        assert_eq!(date1, date2);

        let date2 = Date::try_from_ymd(2020, 1, 31)?;
        let date1 = date.add_days(11)?;
        assert_eq!(date1, date2);

        let date_infinite = Date::new(std::i32::MIN);
        let date1 = date_infinite.add_days(11)?;
        let date2 = Date::new(std::i32::MIN);
        assert_eq!(date1, date2);

        let date_infinite = Date::new(std::i32::MAX);
        let date1 = date_infinite.add_days(11)?;
        let date2 = Date::new(std::i32::MAX);
        assert_eq!(date1, date2);

        let date = Date::try_from_ymd(5874897, 12, 31)?;
        let ret = date.add_days(1);
        assert!(ret.is_err());

        let date = Date::try_from_ymd(2010, 10, 29)?;
        let interval = Interval::try_from_ymwd_hms(1, 1, 1, 2, 25, 15, 5.456)?;
        let timestamp2 = Timestamp::try_from_ymd_hms(2011, 12, 09, 1, 15, 5.456)?;
        let timestamp1 = date.add_interval(interval)?;
        assert_eq!(timestamp1, timestamp2);
        let s = timestamp1.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2011-12-09 01:15:05.456");

        let date = Date::new(std::i32::MIN);
        let ret = date.add_interval(interval)?;
        let s = ret.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "-infinity");

        let date = Date::try_from_ymd(5874897, 10, 29)?;
        let interval = Interval::try_from_ymwd_hms(1, 1, 1, 2, 25, 15, 5.456)?;
        let ret = date.add_interval(interval);
        assert!(ret.is_err());

        let date = Date::try_from_ymd(2010, 10, 29)?;
        let time = Time::try_from_hms(1, 25, 34.56)?;
        let timestamp2 = Timestamp::try_from_ymd_hms(2010, 10, 29, 1, 25, 34.56)?;
        let timestamp1 = date.add_time(time)?;
        assert_eq!(timestamp1, timestamp2);
        let s = timestamp1.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "2010-10-29 01:25:34.56");

        let date = Date::new(std::i32::MIN);
        let time = Time::try_from_hms(1, 25, 34.56)?;
        let timestamp1 = date.add_time(time)?;
        let s = timestamp1.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "-infinity");

        let date = Date::new(std::i32::MAX);
        let time = Time::try_from_hms(1, 25, 34.56)?;
        let timestamp1 = date.add_time(time)?;
        let s = timestamp1.format(DateStyle::ISO, DateOrder::YMD)?;
        assert_eq!(s, "infinity");

        let interval = Interval::try_from_ymwd_hms(1, 1, 1, 2, 25, 15, 5.456)?;
        let span = Interval::try_from_ymwd_hms(2, 40, 6, 31, 25, 15, 5.456)?;
        let result = interval.add_interval(span)?;
        assert_eq!(
            result.format(IntervalStyle::ISO8601)?,
            "P6Y5M82DT50H30M10.912S"
        );

        let span = Interval::try_from_ymwd_hms(0, std::i32::MAX, 6, 31, 25, 15, 5.456)?;
        let result = interval.add_interval(span);
        assert!(result.is_err());

        let span = Interval::try_from_ymwd_hms(0, 10, 0, std::i32::MAX, 25, 15, 5.456)?;
        let result = interval.add_interval(span);
        assert!(result.is_err());

        let span = Interval::try_from_ymwd_hms(
            0,
            10,
            0,
            0,
            0,
            0,
            std::i64::MAX as f64 / USECS_PER_SEC as f64 - 1.0,
        )?;
        let result = interval.add_interval(span);
        assert!(result.is_err());

        let timestamp = Timestamp::try_from_ymd_hms(2011, 12, 09, 1, 15, 5.456)?;
        let result = timestamp.add_interval(interval)?;
        assert_eq!(
            result.format(DateStyle::ISO, DateOrder::YMD)?,
            "2013-01-19 02:30:10.912"
        );

        let time = Time::try_from_hms(1, 25, 34.56)?;
        let result = time.add_interval(interval);
        assert_eq!(result.format(DateStyle::ISO)?, "02:40:40.016");

        let time = Time::from(-30000000);
        let interval = Interval::try_from_str("@ 1 day", -1, IntervalStyle::Postgres)?;
        let result = time.add_interval(interval);
        assert_eq!(result.format(DateStyle::ISO)?, "23:59:30");

        Ok(())
    }

    #[test]
    fn test_negative() -> Result<(), DateTimeError> {
        let span = Interval::try_from_ymwd_hms(12, 23, 6, 31, 25, 15, 5.456)?;
        let result = span.negate()?;
        assert_eq!(
            result.format(IntervalStyle::ISO8601)?,
            "P-13Y-11M-73DT-25H-15M-5.456S"
        );

        let span = Interval::from_mdt(12, 34, std::i64::MIN);
        let ret = span.negate();
        assert!(ret.is_err());

        let span = Interval::from_mdt(12, 34, std::i64::MIN);
        let ret = span.negate();
        assert!(ret.is_err());

        let span = Interval::from_mdt(12, std::i32::MIN, 23);
        let ret = span.negate();
        assert!(ret.is_err());

        let span = Interval::from_mdt(std::i32::MIN, 23, 23);
        let ret = span.negate();
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_sub_operator() -> Result<(), DateTimeError> {
        let date1 = Date::try_from_ymd(2020, 10, 1)?;
        let date2 = Date::try_from_ymd(2000, 2, 28)?;
        let ret = date1.sub_date(date2)?;
        assert_eq!(ret, 7521);
        let ret = date2.sub_date(date1)?;
        assert_eq!(ret, -7521);

        let date1 = Date::new(std::i32::MIN);
        let date2 = Date::try_from_ymd(2000, 2, 28)?;
        let ret = date1.sub_date(date2);
        assert!(ret.is_err());
        let ret = date2.sub_date(date1);
        assert!(ret.is_err());

        let date1 = Date::try_from_ymd(2020, 10, 1)?;
        let ret = date1.sub_days(2)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "2020-09-29");

        let date1 = Date::new(std::i32::MIN);
        let ret = date1.sub_days(2)?;
        assert_eq!(ret.format(DateStyle::ISO, DateOrder::YMD)?, "-infinity");

        let date1 = Date::new(std::i32::MIN + 1);
        let ret = date1.sub_days(2);
        assert!(ret.is_err());

        let date1 = Date::try_from_ymd(2020, 10, 1)?;
        let ret = date1.sub_days(2451545 + 2020 * 365);
        assert!(ret.is_err());

        let date = Date::try_from_ymd(2020, 10, 1)?;
        let interval = Interval::try_from_ymwd_hms(1, 2, 0, 34, 10, 20, 34.678)?;
        let timestamp = date.sub_interval(interval)?;
        assert_eq!(
            timestamp.format(DateStyle::ISO, DateOrder::YMD)?,
            "2019-06-27 13:39:25.322"
        );

        let time1 = Time::try_from_hms(10, 20, 45.567)?;
        let time2 = Time::try_from_hms(4, 50, 54.56)?;
        let ret = time1.sub_time(time2);
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "PT5H29M51.007S");

        let time = Time::try_from_hms(10, 20, 45.567)?;
        let interval = Interval::try_from_ymwd_hms(20, 13, 2, 29, 30, 67, 67.78)?;
        let ret = time.sub_interval(interval);
        assert_eq!(ret.format(DateStyle::ISO)?, "03:12:37.787");

        let timestamp = Timestamp::try_from_ymd_hms(2010, 7, 14, 23, 14, 34.678)?;
        let interval = Interval::try_from_ymwd_hms(20, 13, 2, 29, 30, 67, 67.78)?;
        let ret = timestamp.sub_interval(interval)?;
        assert_eq!(
            ret.format(DateStyle::ISO, DateOrder::YMD)?,
            "1989-05-01 16:06:26.898"
        );

        let interval1 = Interval::try_from_ymwd_hms(20, 13, 2, 29, 30, 67, 67.78)?;
        let interval2 = Interval::try_from_ymwd_hms(10, 26, 4, 23, 45, 34, 100.78)?;
        let ret = interval1.sub_interval(interval2)?;
        assert_eq!(
            ret.format(IntervalStyle::ISO8601)?,
            "P8Y11M-8DT-14H-27M-33S"
        );

        let interval1 = Interval::try_from_ymwd_hms(1, std::i32::MIN + 1, 1, 1, 1, 1, 67.78)?;
        let ret = interval1.sub_interval(interval2);
        assert!(ret.is_err());

        let interval1 = Interval::try_from_ymwd_hms(1, 1, 1, std::i32::MIN + 10, 1, 1, 67.78)?;
        let ret = interval1.sub_interval(interval2);
        assert!(ret.is_err());

        let interval1 = Interval::try_from_ymwd_hms(
            1,
            1,
            1,
            std::i32::MIN + 10,
            0,
            0,
            std::i64::MIN as f64 / USECS_PER_SEC as f64,
        )?;
        let ret = interval1.sub_interval(interval2);
        assert!(ret.is_err());

        let timestamp1 = Timestamp::try_from_ymd_hms(2010, 7, 14, 23, 14, 34.678)?;
        let timestamp2 = Timestamp::try_from_ymd_hms(2008, 3, 31, 2, 14, 16.678)?;
        let ret = timestamp1.sub_timestamp(timestamp2)?;
        assert_eq!(ret.format(IntervalStyle::ISO8601)?, "P835DT21H18S");
        let timestamp1 = Timestamp::new(std::i64::MIN);
        let ret = timestamp1.sub_timestamp(timestamp2);
        assert!(ret.is_err());
        let timestamp1 = Timestamp::new(std::i64::MAX);
        let ret = timestamp1.sub_timestamp(timestamp2);
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_mul_operation() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_ymwd_hms(20, 13, 2, 29, 30, 67, 67.78)?;
        let ret = interval.mul_f64(8.0)?;
        assert_eq!(
            ret.format(IntervalStyle::ISO8601)?,
            "P168Y8M344DT249H5M2.24S"
        );

        let interval = Interval::try_from_ymwd_hms(20, 13, 2, 29, 30, 67, 67.78)?;
        let ret = interval.mul_f64(800.0)?;
        assert_eq!(
            ret.format(IntervalStyle::ISO8601)?,
            "P16866Y8M34400DT24908H23M44S"
        );

        let interval = Interval::try_from_ymwd_hms(0, 13, 2, 29, 30, 67, 67.78)?;
        let ret = interval.mul_f64(0.88)?;
        assert_eq!(
            ret.format(IntervalStyle::ISO8601)?,
            "P11M51DT28H21M33.2464S"
        );

        let interval = Interval::try_from_ymwd_hms(0, 0, 0, 0, 12, 67, 67.78)?;
        let ret = interval.mul_f64(std::f64::MAX);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(0, std::i32::MAX, 2, 29, 30, 67, 67.78)?;
        let ret = interval.mul_f64(std::f64::INFINITY);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(0, std::i32::MAX, 2, 29, 30, 67, 67.78)?;
        let ret = interval.mul_f64(8.0);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(0, std::i32::MIN, 2, 29, 30, 67, 67.78)?;
        let ret = interval.mul_f64(8.0);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(0, 0, 0, std::i32::MAX, 30, 67, 67.78)?;
        let ret = interval.mul_f64(8.0);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(0, 0, 0, std::i32::MIN, 30, 67, 67.78)?;
        let ret = interval.mul_f64(8.0);
        assert!(ret.is_err());

        Ok(())
    }

    #[test]
    fn test_divide_operation() -> Result<(), DateTimeError> {
        let interval = Interval::try_from_ymwd_hms(20, 13, 2, 29, 30, 67, 67.78)?;
        let ret = interval.div_f64(8.0)?;
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2Y7M24DT6H53M30.9725S");
        let ret = interval.div_f64(0.0);
        assert!(ret.is_err());

        let interval = Interval::try_from_ymwd_hms(20, 13, 0, std::i32::MAX, 30, 67, 67.78)?;
        let ret = interval.div_f64(8.0)?;
        let s = ret.format(IntervalStyle::ISO8601)?;
        assert_eq!(s, "P2Y7M268435474DT18H53M30.9725S");
        Ok(())
    }

    #[test]
    fn test_cmp_operation() -> Result<(), DateTimeError> {
        let time1 = Time::try_from_hms(23, 45, 56.78)?;
        let time2 = Time::try_from_hms(12, 56, 56.78)?;
        assert!(time1 > time2);
        assert!(time1 >= time2);
        assert!(time1 == time1);
        assert!(time1 != time2);
        assert!(time2 < time1);
        assert!(time2 <= time1);

        let time1 = Time::try_from_hms(24, 0, 0.0)?;
        let time2 = Time::try_from_hms(0, 0, 0.0)?;
        assert!(time1 > time2);
        assert!(time1 >= time2);
        assert!(time1 == time1);
        assert!(time1 != time2);
        assert!(time2 < time1);
        assert!(time2 <= time1);

        let time1 = Time::try_from_hms(24, 0, 0.0)?;
        let time2 = Time::try_from_hms(12, 1, 34.0)?;
        assert!(time1 > time2);
        assert!(time1 >= time2);
        assert!(time1 == time1);
        assert!(time1 != time2);
        assert!(time2 < time1);
        assert!(time2 <= time1);

        let date1 = Date::try_from_ymd(2019, 7, 2)?;
        let date2 = Date::try_from_ymd(2018, 8, 31)?;
        assert!(date1 > date2);
        assert!(date1 >= date2);
        assert!(date1 == date1);
        assert!(date1 != date2);
        assert!(date2 < date1);
        assert!(date2 <= date1);

        let timestamp1 = Timestamp::try_from_ymd_hms(2020, 1, 2, 1, 23, 34.78)?;
        let timestamp2 = Timestamp::try_from_ymd_hms(2019, 10, 12, 11, 43, 54.78)?;
        assert!(timestamp1 > timestamp2);
        assert!(timestamp1 >= timestamp2);
        assert!(timestamp1 == timestamp1);
        assert!(timestamp1 != timestamp2);
        assert!(timestamp2 < timestamp1);
        assert!(timestamp2 <= timestamp1);

        let interval1 = Interval::try_from_ymwd_hms(10, 1, 2, 1, 23, 56, 34.78)?;
        let interval2 = Interval::try_from_ymwd_hms(8, 10, 5, 12, 11, 43, 54.78)?;
        assert!(interval1 > interval2);
        assert!(interval1 >= interval2);
        assert!(interval1 == interval1);
        assert!(interval1 != interval2);
        assert!(interval2 < interval1);
        assert!(interval2 <= interval1);

        let interval1 = Interval::try_from_ymwd_hms(0, 11, 1, 1, 23, 56, 34.78)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 10, 2, 12, 11, 43, 54.78)?;
        assert!(interval1 > interval2);
        assert!(interval1 >= interval2);
        assert!(interval1 == interval1);
        assert!(interval1 != interval2);
        assert!(interval2 < interval1);
        assert!(interval2 <= interval1);

        let interval1 = Interval::try_from_ymwd_hms(0, 0, 3, 1, 23, 56, 34.78)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 0, 2, 6, 11, 43, 54.78)?;
        assert!(interval1 > interval2);

        let interval1 = Interval::try_from_ymwd_hms(0, 0, 0, 7, 22, 56, 34.78)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 0, 0, 6, 23, 43, 54.78)?;
        assert!(interval1 > interval2);

        let interval1 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 23, 56, 34.78)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 22, 59, 54.78)?;
        assert!(interval1 > interval2);

        let interval1 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 0, 56, 34.78)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 0, 47, 54.78)?;
        assert!(interval1 > interval2);

        let interval1 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 0, 0, 56.78)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 0, 0, 54.78)?;
        assert!(interval1 > interval2);

        let interval1 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 0, 0, 56.781)?;
        let interval2 = Interval::try_from_ymwd_hms(0, 0, 0, 0, 0, 0, 56.78)?;
        assert!(interval1 > interval2);

        let ret = interval1.partial_cmp(&interval2);
        assert_eq!(ret, Some(Ordering::Greater));

        let ret = interval1.cmp(&interval2);
        assert_eq!(ret, Ordering::Greater);
        Ok(())
    }
}
