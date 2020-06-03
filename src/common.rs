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

//! Common structures, constants and functions.

use crate::error::DateTimeError;
use crate::timestamp::is_valid_timestamp;
use crate::token::*;

#[inline]
pub fn copy_slice(dst: &mut [u8], offset: usize, src: &[u8]) -> usize {
    debug_assert!(dst.len() - offset >= src.len());
    let end = offset + src.len();
    let d = &mut dst[offset..end];
    d.copy_from_slice(src);
    end
}

#[inline]
pub fn set_one_byte(dst: &mut [u8], offset: usize, src: u8) -> usize {
    debug_assert!(dst.len() - offset >= 1);
    dst[offset] = src;
    offset + 1
}

/// Calendar time to Julian date conversions.
/// Julian date is commonly used in astronomical applications,
/// since it is numerically accurate and computationally simple.
/// The algorithms here will accurately convert between Julian day
/// and calendar date for all non-negative Julian days
/// (i.e. from Nov 24, -4713 on).
///
/// These routines will be used by other date/time packages
/// - thomas 97/02/25
///
/// Rewritten to eliminate overflow problems. This now allows the
/// routines to work correctly for all Julian day counts from
/// 0 to 2147483647 (Nov 24, -4713 to Jun 3, 5874898) assuming
/// a 32-bit integer. Longer types should also work to the limits
/// of their precision.
#[inline]
pub fn date2julian(y: i32, m: i32, d: i32) -> i32 {
    let (y, m) = if m > 2 {
        (y + 4800, m + 1)
    } else {
        (y + 4799, m + 13)
    };

    let century = y / 100;

    let mut julian = y * 365 - 32167;
    julian += y / 4 - century + century / 4;
    julian += 7834 * m / 256 + d;

    julian
}

/// Converts julian to date.
#[inline]
pub fn julian2date(jd: i32) -> (i32, i32, i32) {
    let mut julian = jd as u32 + 32044;
    let mut quad = julian / 146_097;
    let extra = (julian - quad * 146_097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146_097;
    quad = julian / 1461;
    julian -= quad * 1461;

    let mut y: i32 = (julian * 4 / 1461) as i32;
    julian = if y != 0 {
        (julian + 305) % 365 + 123
    } else {
        (julian + 306) % 366 + 123
    };
    y += (quad * 4) as i32;
    let year = y - 4800;
    quad = julian * 2141 / 65_536;

    let day = julian - 7834 * quad / 256;
    let month = (quad + 10) % MONTHS_PER_YEAR as u32 + 1;
    (year, month as i32, day as i32)
}

#[derive(Debug)]
pub struct PgTime {
    pub sec: i32,
    pub min: i32,
    pub hour: i32,
    pub mday: i32,
    /// origin 0, not 1.
    pub mon: i32,
    /// relative to 1900.
    pub year: i32,
    /// Day of weed.
    pub wday: i32,
    /// Day pf year.
    pub yday: i32,
    /// Whether is daylight saving time.
    pub isdst: i32,
    /// Offset of GTM time.
    pub gmtoff: i64,
    pub zone: Option<&'static [u8]>,
}

impl PgTime {
    #[inline]
    pub const fn new() -> Self {
        Self {
            sec: 0,
            min: 0,
            hour: 0,
            mday: 0,
            mon: 0,
            year: 0,
            wday: 0,
            yday: 0,
            isdst: -1,
            gmtoff: 0,
            zone: None,
        }
    }

    #[inline]
    pub fn set_ymd(&mut self, y: i32, m: i32, d: i32) {
        self.year = y;
        self.mon = m;
        self.mday = d;
    }

    #[inline]
    pub fn truncate_timestamp_week(&mut self, fsec: &mut i64) {
        let woy = date_to_iso_week(self.year, self.mon, self.mday);
        // If it is week 52/53 and the month is January, then the
        // week must belong to the previous year. Also, some
        // December dates belong to the next year.
        if woy >= 52 && self.mon == 1 {
            self.year -= 1;
        }
        if woy <= 1 && self.mon == MONTHS_PER_YEAR {
            self.year -= 1;
        }
        iso_week_to_date(woy, self);
        self.hour = 0;
        self.min = 0;
        self.sec = 0;
        *fsec = 0;
    }

    #[inline]
    pub fn truncate_micro_sec(&mut self, _fsec: &mut i64) {}

    #[inline]
    pub fn truncate_milli_sec(&mut self, fsec: &mut i64) {
        *fsec = (*fsec / 1000) * 1000;
        self.truncate_micro_sec(fsec);
    }

    #[inline]
    pub fn truncate_sec(&mut self, fsec: &mut i64) {
        *fsec = 0;
        self.truncate_milli_sec(fsec);
    }

    #[inline]
    pub fn truncate_minute(&mut self, fsec: &mut i64) {
        self.sec = 0;
        self.truncate_sec(fsec);
    }

    #[inline]
    pub fn truncate_hour(&mut self, fsec: &mut i64) {
        self.min = 0;
        self.truncate_minute(fsec);
    }

    #[inline]
    pub fn truncate_day(&mut self, fsec: &mut i64) {
        self.hour = 0;
        self.truncate_hour(fsec);
    }

    #[inline]
    pub fn truncate_interval_month(&mut self, fsec: &mut i64) {
        self.mday = 0;
        self.truncate_day(fsec);
    }

    #[inline]
    pub fn truncate_timestamp_month(&mut self, fsec: &mut i64) {
        self.mday = 1;
        self.truncate_day(fsec);
    }

    #[inline]
    pub fn truncate_interval_quarter(&mut self, fsec: &mut i64) {
        self.mon = 3 * (self.mon / 3);
        self.truncate_interval_month(fsec);
    }

    #[inline]
    pub fn truncate_timestamp_quarter(&mut self, fsec: &mut i64) {
        self.mon = (3 * ((self.mon - 1) / 3)) + 1;
        self.truncate_timestamp_month(fsec);
    }

    #[inline]
    pub fn truncate_interval_year(&mut self, fsec: &mut i64) {
        self.mon = 0;
        self.truncate_interval_quarter(fsec);
    }

    #[inline]
    pub fn truncate_timestamp_year(&mut self, fsec: &mut i64) {
        self.mon = 1;
        self.truncate_timestamp_quarter(fsec);
    }

    #[inline]
    pub fn truncate_interval_decade(&mut self, fsec: &mut i64) {
        self.year = (self.year / 10) * 10;
        self.truncate_interval_year(fsec)
    }

    #[inline]
    pub fn truncate_timestamp_decade(&mut self, fsec: &mut i64) {
        // truncating to the decade? first year of the decade. must
        // not be applied if year was truncated before!
        if self.year > 0 {
            self.year = (self.year / 10) * 10;
        } else {
            self.year = -((8 - (self.year - 1)) / 10) * 10;
        }
        self.truncate_timestamp_year(fsec)
    }

    #[inline]
    pub fn truncate_interval_century(&mut self, fsec: &mut i64) {
        self.year = (self.year / 100) * 100;
        self.truncate_interval_decade(fsec);
    }

    #[inline]
    pub fn truncate_timestamp_century(&mut self, fsec: &mut i64) {
        // truncating to the century? as above: -100, 1, 101.
        if self.year > 0 {
            self.year = ((self.year + 99) / 100) * 100 - 99;
        } else {
            self.year = -((99 - (self.year - 1)) / 100) * 100 + 1;
        }
        self.truncate_timestamp_year(fsec);
    }

    #[inline]
    pub fn truncate_interval_millennium(&mut self, fsec: &mut i64) {
        self.year = (self.year / 1000) * 1000;
        self.truncate_interval_century(fsec);
    }

    #[inline]
    pub fn truncate_timestamp_millennium(&mut self, fsec: &mut i64) {
        // truncating to the millennium? what is this supposed to
        // mean? let us put the first year of the millennium... i.e.
        // -1000, 1, 1001, 2001.
        if self.year > 0 {
            self.year = ((self.year + 999) / 1000) * 1000 - 999;
        } else {
            self.year = -((999 - (self.year - 1)) / 1000) * 1000 + 1;
        }
        self.truncate_timestamp_century(fsec);
    }

    /// validate_date.
    /// Check valid year/month/day values, handle BC and DOY cases.
    /// Return 0 if okay, a DTERR code if not.
    pub fn validate_date(
        &mut self,
        f_mask: i32,
        is_julian: bool,
        is2digits: bool,
        bc: bool,
    ) -> Result<(), DateTimeError> {
        if (f_mask & TokenType::YEAR.mask()) != 0 {
            if is_julian {
                // tm_year is correct and should not be touched.
            } else if bc {
                // there is no year zero in AD/BC notation.
                if self.year <= 0 {
                    return Err(DateTimeError::overflow());
                }
                // internally, we represent 1 BC as year zero, 2 BC as -1, etc.
                self.year = -(self.year - 1);
            } else if is2digits {
                // process 1 or 2-digit input as 1970-2069 AD, allow '0' and '00'
                if self.year < 0 {
                    // just paranoia
                    return Err(DateTimeError::overflow());
                }
                if self.year < 70 {
                    self.year += 2000;
                } else if self.year < 100 {
                    self.year += 1900;
                }
            } else {
                // there is no year zero in AD/BC notation.
                if self.year <= 0 {
                    return Err(DateTimeError::overflow());
                }
            }
        }

        // now that we have correct year, decode DOY.
        if (f_mask & TokenType::DOY.mask()) != 0 {
            let (y, m, d) = julian2date(date2julian(self.year, 1, 1) + self.yday - 1);
            self.set_ymd(y, m, d);
        }

        // check for valid month.
        if (f_mask & TokenType::MONTH.mask()) != 0 && (self.mon < 1 || self.mon > MONTHS_PER_YEAR) {
            return Err(DateTimeError::overflow());
        }

        // minimal check for valid day.
        if (f_mask & TokenType::DAY.mask()) != 0 && (self.mday < 1 || self.mday > 31) {
            return Err(DateTimeError::overflow());
        }

        // Check for valid day of month, now that we know for sure the month
        // and year.  Note we don't use MD_FIELD_OVERFLOW here, since it seems
        // unlikely that "Feb 29" is a YMD-order error.
        if (f_mask & DTK_DATE_M) == DTK_DATE_M && self.mday > days_of_month(self.year, self.mon) {
            return Err(DateTimeError::overflow());
        }

        Ok(())
    }

    /// Convert a tm structure to a timestamp data type.
    /// Note that year is _not_ 1900-based, but is an explicit full value.
    /// Also, month is one-based, _not_ zero-based.
    pub fn to_timestamp(&self, fsec: i64, tz: Option<i32>) -> Result<i64, DateTimeError> {
        // Prevent overflow in Julian-day routines.
        if !is_valid_julian(self.year, self.mon, self.mday) {
            return Err(DateTimeError::invalid(format!(
                "date: ({}, {}, {}) is not valid julian date",
                self.year, self.mon, self.mday
            )));
        }

        let date = date2julian(self.year, self.mon, self.mday) - date2julian(2000, 1, 1);
        let time = time2t(self.hour, self.min, self.sec, fsec);

        // Check for major overflow
        let result = match (date as i64).checked_mul(USECS_PER_DAY) {
            Some(v) => v,
            None => return Err(DateTimeError::overflow()),
        };

        let mut result = match result.checked_add(time) {
            Some(v) => v,
            None => return Err(DateTimeError::overflow()),
        };

        if let Some(z) = tz {
            result = datetime2local(result, -z);
        }

        // final range check catches just-out-of-range timestamps.
        if !is_valid_timestamp(result) {
            return Err(DateTimeError::invalid(format!(
                "timestamp: {:?} is not valid",
                result
            )));
        }

        Ok(result)
    }
}

#[inline]
fn time2t(hour: i32, min: i32, sec: i32, fsec: i64) -> i64 {
    (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) as i64 + sec as i64)
        * USECS_PER_SEC as i64)
        + fsec
}

#[inline]
fn datetime2local(datetime: i64, tz: i32) -> i64 {
    datetime - tz as i64 * USECS_PER_SEC
}

/// Converts Julian date to day-of-week (0..6 == Sun..Sat)
///
/// Note: various places use the locution j2day(date - 1) to produce a
/// result according to the convention 0..6 = Mon..Sun.  This is a bit of
/// a crock, but will work as long as the computation here is just a modulo.
#[inline]
pub fn julian_to_week_day(date: i32) -> i32 {
    let mut date = date + 1;
    date %= 7;
    // Cope if division truncates towards zero, as it probably does.
    if date < 0 {
        date += 7;
    }
    date
}

/// Converts date to iso_week.
#[inline]
pub fn date_to_iso_week(year: i32, mon: i32, mday: i32) -> i32 {
    //current day.
    let dayn = date2julian(year, mon, mday);

    // fourth day of current year.
    let mut day4 = date2julian(year, 1, 4);

    //ay0 == offset to first day of week (Monday).
    let mut day0 = julian_to_week_day(day4 - 1);

    // We need the first week containing a Thursday, otherwise this day falls
    // into the previous year for purposes of counting weeks
    if dayn < day4 - day0 {
        day4 = date2julian(year - 1, 1, 4);

        // day0 == offset to first day of week (Monday)
        day0 = julian_to_week_day(day4 - 1);
    }

    let mut result = (dayn - (day4 - day0)) / 7 + 1;

    // Sometimes the last few days in a year will fall into the first week of
    // the next year, so check for this.
    if result >= 52 {
        day4 = date2julian(year + 1, 1, 4);

        // day0 == offset to first day of week (Monday).
        day0 = julian_to_week_day(day4 - 1);

        if dayn >= day4 - day0 {
            result = (dayn - (day4 - day0)) / 7 + 1;
        }
    }

    result
}

/// Converts ISO week to julian date.
/// Return the Julian day which corresponds to the first day (Monday) of the given ISO 8601 year and week.
/// Julian days are used to convert between ISO week dates and Gregorian dates.
#[inline]
fn iso_week_to_julian(year: i32, week: i32) -> i32 {
    // Fourth day of current year.
    let day4 = date2julian(year, 1, 4);

    // day0 == offset to first day of week (Monday).
    let day0 = julian_to_week_day(day4 - 1);

    ((week - 1) * 7) + (day4 - day0)
}

/// Convert ISO week of year number to date.
/// The year field must be specified with the ISO year!
/// karel 2000/08/07
#[inline]
fn iso_week_to_date(woy: i32, tm: &mut PgTime) {
    let week_julian = iso_week_to_julian(tm.year, woy);
    let (y, m, d) = julian2date(week_julian);
    tm.set_ymd(y, m, d);
}

///Converts date to ISO year.
#[inline]
pub fn date_to_iso_year(year: i32, mon: i32, mday: i32) -> i32 {
    let mut year = year;
    // current day.
    let dayn = date2julian(year, mon, mday);

    // fourth day of current year.
    let mut day4 = date2julian(year, 1, 4);

    // day0 == offset to first day of week (Monday)
    let mut day0 = julian_to_week_day(day4 - 1);

    // We need the first week containing a Thursday, otherwise this day falls
    // into the previous year for purposes of counting weeks
    if dayn < day4 - day0 {
        day4 = date2julian(year - 1, 1, 4);

        // day0 == offset to first day of week (Monday)
        day0 = julian_to_week_day(day4 - 1);

        year -= 1;
    }

    let result = (dayn - (day4 - day0)) / 7 + 1;

    // Sometimes the last few days in a year will fall into the first week of
    // the next year, so check for this.
    if result >= 52 {
        day4 = date2julian(year + 1, 1, 4);

        // day0 == offset to first day of week (Monday)
        day0 = julian_to_week_day(day4 - 1);

        if dayn >= day4 - day0 {
            year += 1;
        }
    }

    year
}

#[inline]
fn year_is_leap(y: i32) -> bool {
    y % 4 == 0 && ((y % 100) != 0 || (y % 400) == 0)
}

#[inline]
pub fn days_of_month(y: i32, m: i32) -> i32 {
    const DAY_TABLE: [[i32; 13]; 2] = [
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    ];

    DAY_TABLE[year_is_leap(y) as usize][m as usize - 1]
}

//
// Julian date support.
//
// date2j() and j2date() nominally handle the Julian date range 0..INT_MAX,
// or 4714-11-24 BC to 5874898-06-03 AD.  In practice, date2j() will work and
// give correct negative Julian dates for dates before 4714-11-24 BC as well.
//  We rely on it to do so back to 4714-11-01 BC.  Allowing at least one day's
// slop is necessary so that timestamp rotation doesn't produce dates that
// would be rejected on input.  For example, '4714-11-24 00:00 GMT BC' is a
// legal timestamptz value, but in zones east of Greenwich it would print as
// sometime in the afternoon of 4714-11-23 BC; if we couldn't process such a
// date we'd have a dump/reload failure.  So the idea is for IS_VALID_JULIAN
// to accept a slightly wider range of dates than we really support, and
// then we apply the exact checks in IS_VALID_DATE or IS_VALID_TIMESTAMP,
// after timezone rotation if any.  To save a few cycles, we can make
// IS_VALID_JULIAN check only to the month boundary, since its exact cutoffs
// are not very critical in this scheme.
//
// It is correct that JULIAN_MINYEAR is -4713, not -4714; it is defined to
// allow easy comparison to tm_year values, in which we follow the convention
// that tm_year <= 0 represents abs(tm_year)+1 BC.

const JULIAN_MIN_YEAR: i32 = -4713;
const JULIAN_MIN_MONTH: i32 = 11;
const JULIAN_MAX_YEAR: i32 = 5_874_898;
const JULIAN_MAX_MONTH: i32 = 6;

// Julian-date equivalents of Day 0 in Unix and Postgres reckoning
pub const POSTGRES_EPOCH_JDATE: i32 = 2_451_545; // == date2j(2000, 1, 1)

#[inline]
pub fn is_valid_julian(y: i32, m: i32, _d: i32) -> bool {
    (y > JULIAN_MIN_YEAR || (y == JULIAN_MIN_YEAR && ((m) >= JULIAN_MIN_MONTH)))
        && (y < JULIAN_MAX_YEAR || (y == JULIAN_MAX_YEAR && (m < JULIAN_MAX_MONTH)))
}

#[inline]
pub fn time_modulo(t: i64, unit: i64) -> (i64, i64) {
    let q = t / unit;
    if q != 0 {
        let left = t - q * unit;
        (left, q)
    } else {
        (t, q)
    }
}

#[inline]
pub fn timestamp_round(t: f64) -> f64 {
    // Round off to MAX_TIMESTAMP_PRECISION decimal places.
    // Note: this is also used for rounding off intervals.
    const TS_PREC_INV: f64 = 1_000_000.0;

    (t * TS_PREC_INV).round() / TS_PREC_INV
}

#[inline]
pub fn get_month_name(month: i32) -> &'static [u8] {
    const MONTHS: [&[u8]; 13] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec", b"Unknow",
    ];

    MONTHS[month as usize - 1]
}

#[inline]
pub const fn get_wday_name(wday: i32) -> &'static [u8] {
    const DAYS: [&[u8]; 8] = [
        b"Sun", b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat", b"unknow",
    ];

    DAYS[wday as usize]
}

// Assorted constants for datetime-related calculations
pub const DAYS_PER_YEAR: f32 = 365.25; //assumes leap year every four years.
pub const MONTHS_PER_YEAR: i32 = 12;

// DAYS_PER_MONTH is very imprecise.  The more accurate value is
// 365.2425/12 = 30.436875, or '30 days 10:29:06'.  Right now we only
// return an integral number of days, but someday perhaps we should
// also return a 'time' value to be used as well.  ISO 8601 suggests
//30 days.
pub const DAYS_PER_MONTH: i32 = 30; //assumes exactly 30 days per month.
pub const HOURS_PER_DAY: i32 = 24; // assume no daylight savings time changes.

// This doesn't adjust for uneven daylight savings time intervals or leap
// seconds, and it crudely estimates leap years.  A more accurate value
// for days per years is 365.2422.
#[allow(dead_code)]
pub const SECS_PER_YEAR: i64 = 36525 * 864; // avoid floating-point computation.
pub const SECS_PER_DAY: i32 = 86400;
pub const SECS_PER_HOUR: i32 = 3600;
pub const SECS_PER_MINUTE: i32 = 60;
pub const MINS_PER_HOUR: i32 = 60;

pub const USECS_PER_DAY: i64 = 86_400_000_000i64;
pub const USECS_PER_HOUR: i64 = 3_600_000_000i64;
pub const USECS_PER_MINUTE: i64 = 60_000_000i64;
pub const USECS_PER_SEC: i64 = 1_000_000i64;

#[allow(dead_code)]
pub const MAX_TZDISP_HOUR: i32 = 15;
pub const MAX_DATE_LEN: usize = 128;
pub const MAX_TIME_PRECISION: i32 = 6;

// Limits on the "precision" option (typmod) for these data types.
pub const MAX_TIMESTAMP_PRECISION: usize = 6;
pub const MAX_INTERVAL_PRECISION: usize = 6;

/// Converts 'value' into a decimal string representation stored at 'str'.
///
/// Returns the ending address of the string result (the last character written
/// plus 1).  Note that no NUL terminator is written.
///
/// The intended use-case for this function is to build strings that contain
/// multiple individual numbers, for example:
///
/// str = pg_ltostr(str, a);
/// *str++ = ' ';
/// str = pg_ltostr(str, b);
/// *str = '\0';
///
/// Note: Caller must ensure that 'str' points to enough memory to hold the
/// result.
#[inline]
pub fn pg_ltostr(s: &mut [u8], value: i32) -> usize {
    let mut start = 0;
    // Handle negative numbers in a special way.  We can't just write a '-'
    // prefix and reverse the sign as that would overflow for INT32_MIN.
    let mut new_start = 0;
    let mut value = value;
    if value < 0 {
        s[0] = b'-';
        start = 1;
        // Mark the position we must reverse the string from.
        // Compute the result string backwards.
        let mut value = value;
        new_start = start;
        loop {
            let old_val = value;
            value /= 10;
            let remainder = old_val - value * 10;
            // As above, we expect remainder to be negative.
            s[new_start] = (b'0' as i8 - remainder as i8) as u8;
            if value == 0 {
                break;
            }
            new_start += 1;
        }
    } else {
        // Compute the result string backwards. */
        loop {
            let old_val = value;
            value /= 10;
            let remainder = old_val - value * 10;
            s[new_start] = b'0' + remainder as u8;
            if value == 0 {
                break;
            }
            new_start += 1;
        }
    }

    // Remember the end+1 and back up 'str' to the last character.
    let end = new_start + 1;
    // Reverse string.
    while start < new_start {
        s.swap(start, new_start);
        new_start -= 1;
        start += 1;
    }
    end
}

/// Converts 'value' into a decimal string representation stored at 'str'.
/// 'minwidth' specifies the minimum width of the result; any extra space
///  is filled up by prefixing the number with zeros.
///
/// Returns the ending address of the string result (the last character written
/// plus 1).  Note that no NUL terminator is written.
///
/// The intended use-case for this function is to build strings that contain
/// multiple individual numbers, for example:
///
/// str = pg_ltostr_zeropad(str, hours, 2);
/// *str++ = ':';
/// str = pg_ltostr_zeropad(str, mins, 2);
/// *str++ = ':';
/// str = pg_ltostr_zeropad(str, secs, 2);
/// *str = '\0';
///
/// Note: Caller must ensure that 'str' points to enough memory to hold the
/// result.
#[inline]
pub fn pg_ltostr_zeropad(s: &mut [u8], value: i32, min_width: usize) -> usize {
    let mut start = 0;
    let end = min_width;
    let mut num = value;
    let mut min_width = min_width;

    debug_assert!(min_width > 0);
    // Handle negative numbers in a special way.  We can't just write a '-'
    // prefix and reverse the sign as that would overflow for INT32_MIN.
    if num < 0 {
        s[0] = b'-';
        start += 1;
        min_width -= 1;

        // Build the number starting at the last digit.  Here remainder will
        // be a negative number, so we must reverse the sign before adding '0'
        // in order to get the correct ASCII digit.
        while min_width > 0 {
            min_width -= 1;
            let old_val = num;
            num /= 10;
            let remainder = old_val - num * 10;
            s[start + min_width] = b'0' - remainder as u8;
        }
    } else {
        // Build the number starting at the last digit */
        while min_width > 0 {
            min_width -= 1;
            let oldval = num;
            num /= 10;
            let remainder = oldval - num * 10;
            s[min_width] = b'0' + remainder as u8;
        }
    }

    // If minwidth was not high enough to fit the number then num won't have
    // been divided down to zero.  We punt the problem to pg_ltostr(), which
    // will generate a correct answer in the minimum valid width.
    if num != 0 {
        return pg_ltostr(s, value);
    }

    // Otherwise, return last output character + 1
    end as usize
}

/// Append seconds and fractional seconds (if any) at *cp.
///
/// precision is the max number of fraction digits, fillzeros says to
/// pad to two integral-seconds digits.
///
/// Returns a pointer to the new end of string.  No NUL terminator is put
/// there; callers are responsible for NUL terminating str themselves.
/// Note that any sign is stripped from the input seconds values.
#[inline]
pub fn append_seconds(
    s: &mut [u8],
    sec: i32,
    fsec: i64,
    precision: i32,
    fill_zeros: bool,
) -> usize {
    debug_assert!(precision >= 0);

    let mut offset = if fill_zeros {
        pg_ltostr_zeropad(s, sec.abs(), 2)
    } else {
        pg_ltostr(s, sec.abs())
    };

    // fsec_t is just an int32.
    if fsec != 0 {
        let mut value = fsec.abs();
        let mut got_nonzero = false;
        s[offset] = b'.';
        offset += 1;
        // Append the fractional seconds part.  Note that we don't want any
        // trailing zeros here, so since we're building the number in reverse
        // we'll skip appending zeros until we've output a non-zero digit.
        let mut precision = precision;
        let mut valid_num = 0;
        while precision > 0 {
            precision -= 1;
            let old_val = value;
            value /= 10;
            let remainder = old_val - value * 10;
            // check if we got a non-zero
            if remainder != 0 {
                got_nonzero = true;
            }

            if got_nonzero {
                s[offset + precision as usize] = (b'0' + remainder as u8) as u8;
                valid_num += 1;
            }
        }

        // If we still have a non-zero value then precision must have not been
        // enough to print the number.  We punt the problem to pg_ltostr(),
        // which will generate a correct answer in the minimum valid width.
        if value != 0 {
            return pg_ltostr(&mut s[offset..], fsec.abs() as i32);
        }
        offset += valid_num;
        offset
    } else {
        offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_ltostr() -> Result<(), DateTimeError> {
        let value = -123456;
        let mut s: [u8; 20] = [0; 20];
        let ret = pg_ltostr(&mut s, value);
        assert_eq!(&s[0..ret], b"-123456");
        Ok(())
    }
}
