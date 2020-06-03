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

//! Time definition.

use crate::common::*;
use crate::error::DateTimeError;
use crate::interval::Interval;
use crate::parse::{decode_time_only, parse_date_time, timestamp2time};
use crate::token::*;
use crate::{DateOrder, DateStyle, DateTime, DateUnit, FieldType, Timestamp};
use std::convert::TryFrom;
use std::mem::MaybeUninit;

const TIME_SCALES: [i64; MAX_TIME_PRECISION as usize + 1] =
    [1_000_000, 100_000, 10_000, 1000, 100, 10, 1];

const TIME_OFFSETS: [i64; MAX_TIME_PRECISION as usize + 1] = [500_000, 50_000, 5000, 500, 50, 5, 0];

/// Time of day (no date).
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Time(i64);

impl Time {
    /// Construct a `Time` value.
    #[inline]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Gets value of `Time`.
    #[inline]
    pub(crate) const fn value(self) -> i64 {
        self.0
    }

    /// Constructs `Time` use hour, minute and second. second can indicate milliseconds an microseconds
    /// THe min unit is microseconds.
    #[inline]
    pub fn try_from_hms(h: i32, m: i32, s: f64) -> Result<Time, DateTimeError> {
        // This should match the checks in DecodeTimeOnly
        // test for > 24:00:00
        if h < 0
            || m < 0
            || m > MINS_PER_HOUR - 1
            || s < 0.0
            || s > SECS_PER_MINUTE as f64
            || h > HOURS_PER_DAY
            || (h == HOURS_PER_DAY && (m > 0 || s > 0.0))
        {
            return Err(DateTimeError::overflow());
        }

        // This should match tm2time
        let time = (((h * MINS_PER_HOUR + m) * SECS_PER_MINUTE) as i64 * USECS_PER_SEC)
            + (s * USECS_PER_SEC as f64).round() as i64;
        Ok(Time::from(time))
    }

    /// Constructs `Time` from pgtime.
    #[inline]
    fn from_pgtime(tm: &PgTime, f_sec: i64) -> Self {
        let secs = ((tm.hour * MINS_PER_HOUR + tm.min) * SECS_PER_MINUTE) as i64;
        let usecs = (secs + tm.sec as i64) * USECS_PER_SEC;
        Time::from(usecs + f_sec)
    }

    /// Force the precision of the time value to a specified value.
    /// Uses *exactly* the same code as in AdjustTimestampForTypemod()
    /// but we make a separate copy because those types do not
    /// have a fundamental tie together but rather a coincidence of
    /// implementation. - thomas.
    #[inline]
    fn adjust_by_typmod(self, typmod: i32) -> Self {
        if typmod >= 0 && typmod <= MAX_TIME_PRECISION {
            let ret = if self.value() >= 0 {
                ((self.value() + TIME_OFFSETS[typmod as usize]) / TIME_SCALES[typmod as usize])
                    * TIME_SCALES[typmod as usize]
            } else {
                -((((-self.value()) + TIME_OFFSETS[typmod as usize])
                    / TIME_SCALES[typmod as usize])
                    * TIME_SCALES[typmod as usize])
            };

            Time::from(ret)
        } else {
            self
        }
    }

    /// Parses `Time` string. type_mod should be 0 ~ 6, else will have no affect.
    #[allow(clippy::uninit_assumed_init)]
    pub fn try_from_str(
        s: &str,
        type_mod: i32,
        date_order: DateOrder,
    ) -> Result<Time, DateTimeError> {
        let s = s.as_bytes();
        let mut fields: [&[u8]; MAX_DATE_FIELDS] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut work_buf: [u8; MAX_DATE_LEN] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut f_types: [TokenField; MAX_DATE_FIELDS] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let mut tm = PgTime::new();

        let field_num =
            parse_date_time(s, &mut work_buf, &mut fields, &mut f_types, MAX_DATE_FIELDS)?;
        let mut tz = Some(0);
        let fsec = decode_time_only(
            &fields[0..field_num],
            &mut f_types[0..field_num],
            &mut tm,
            &mut tz,
            date_order,
        )?;

        let time = Time::from_pgtime(&tm, fsec).adjust_by_typmod(type_mod);
        Ok(time)
    }

    /// Converts `Time` to string.
    #[inline]
    pub fn format(self, date_style: DateStyle) -> Result<String, DateTimeError> {
        let mut buf = Vec::with_capacity(128);
        unsafe { buf.set_len(128) };
        let mut tm: PgTime = PgTime::new();
        let mut fsec = 0;
        self.to_pgtime(&mut tm, &mut fsec);
        let len = format_time(&tm, buf.as_mut_slice(), fsec, false, 0, date_style);
        unsafe {
            buf.set_len(len);
            Ok(String::from_utf8_unchecked(buf))
        }
    }

    /// `Time` Adds `Interval`.
    #[inline]
    pub fn add_interval(self, span: Interval) -> Time {
        let mut result = self.value().wrapping_add(span.time());
        result -= result / USECS_PER_DAY * USECS_PER_DAY;
        if result < 0 {
            result += USECS_PER_DAY;
        }
        Time::from(result)
    }

    /// `Time` subtracts `Time`
    #[inline]
    pub fn sub_time(self, time: Time) -> Interval {
        let time = self.0.wrapping_sub(time.0);
        Interval::from_mdt(0, 0, time)
    }

    /// `Time` subtracts `Interval`.
    #[inline]
    pub fn sub_interval(self, span: Interval) -> Time {
        let mut result = self.value().wrapping_sub(span.time());
        result -= result / USECS_PER_DAY * USECS_PER_DAY;
        if result < 0 {
            result += USECS_PER_DAY;
        }
        Time::from(result)
    }

    /// Converts `Time` to `PgTime`
    #[inline]
    pub(crate) fn to_pgtime(self, tm: &mut PgTime, fsec: &mut i64) {
        let mut time = self.value();
        tm.hour = (time / USECS_PER_HOUR) as i32;
        time -= tm.hour as i64 * USECS_PER_HOUR;
        tm.min = (time / USECS_PER_MINUTE) as i32;
        time -= tm.min as i64 * USECS_PER_MINUTE;
        tm.sec = (time / USECS_PER_SEC) as i32;
        time -= tm.sec as i64 * USECS_PER_SEC;
        *fsec = time;
    }
}

/// Encode time fields only.
/// tm and fsec are the value to encode, print_tz determines whether to include
/// a time zone (the difference between time and timetz types), tz is the
/// numeric time zone offset, style is the date style, str is where to write the
/// output.
#[inline]
fn format_time(
    tm: &PgTime,
    str: &mut [u8],
    fsec: i64,
    _print_tz: bool,
    _tz: i32,
    _style: DateStyle,
) -> usize {
    let mut offset = pg_ltostr_zeropad(str, tm.hour, 2);
    str[offset] = b':';
    offset += 1;
    offset += pg_ltostr_zeropad(&mut str[offset..], tm.min, 2);
    str[offset] = b':';
    offset += 1;
    offset += append_seconds(&mut str[offset..], tm.sec, fsec, MAX_TIME_PRECISION, true);
    /*    if print_tz {
        offset += encode_timezone(&mut str[offset..], tz, style);
    }*/
    offset
}

/// Converts `Time` to `i64` type.
impl From<Time> for i64 {
    #[inline]
    fn from(t: Time) -> Self {
        t.0
    }
}

/// Converts `i64` to `Date` to type.
impl From<i64> for Time {
    #[inline]
    fn from(v: i64) -> Self {
        Time(v)
    }
}

/// Converts `Timestamp` to `Time`.
impl TryFrom<Timestamp> for Option<Time> {
    type Error = DateTimeError;

    #[inline]
    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        if value.is_infinite() {
            return Ok(None);
        }

        let mut tm: PgTime = PgTime::new();
        let mut fsec = 0;
        timestamp2time(value.value(), &None, &mut tm, &mut fsec, &mut None, &None)?;
        // Could also do this with time = (timestamp / USECS_PER_DAY *
        // USECS_PER_DAY) - timestamp;
        let result = ((((tm.hour * MINS_PER_HOUR + tm.min) * SECS_PER_MINUTE) as i64
            + tm.sec as i64)
            * USECS_PER_SEC)
            + fsec;

        Ok(Some(Time::from(result)))
    }
}

/// Converts `Interval` to `Time`.
/// This is defined as producing the fractional-day portion of the interval.
/// Therefore, we can just ignore the months field.  It is not real clear
/// what to do with negative intervals, but we choose to subtract the floor,
/// so that, say, '-2 hours' becomes '22:00:00'.
impl From<Interval> for Time {
    #[inline]
    fn from(value: Interval) -> Self {
        let mut result = value.time();
        if result >= USECS_PER_DAY {
            let days = result / USECS_PER_DAY;
            result -= days * USECS_PER_DAY;
        } else if result < 0 {
            let days = (-result + USECS_PER_DAY - 1) / USECS_PER_DAY;
            result += days * USECS_PER_DAY;
        }
        Time::from(result)
    }
}

impl DateTime for Time {
    /// Extracts specified field from time type.
    #[inline]
    fn date_part(&self, ty: FieldType, unit: DateUnit) -> Result<f64, DateTimeError> {
        match ty {
            FieldType::Unit => {
                let mut tm = PgTime::new();
                let mut fsec = 0;
                self.to_pgtime(&mut tm, &mut fsec);
                match unit {
                    DateUnit::MicroSec => Ok(tm.sec as f64 * 1_000_000.0 + fsec as f64),
                    DateUnit::MilliSec => Ok(tm.sec as f64 * 1000.0 + fsec as f64 / 1000.0),
                    DateUnit::Second => Ok(tm.sec as f64 + fsec as f64 / 1_000_000.0),
                    DateUnit::Minute => Ok(tm.min as f64),
                    DateUnit::Hour => Ok(tm.hour as f64),
                    _ => Err(DateTimeError::invalid(format!(
                        "unit: {:?} is invalid for time",
                        unit
                    ))),
                }
            }
            FieldType::Epoch => {
                if unit == DateUnit::Epoch {
                    Ok(self.value() as f64 / 1_000_000.0)
                } else {
                    Err(DateTimeError::invalid(format!("type: {:?} is invalid", ty)))
                }
            }
        }
    }

    /// Checks whether `Time` is finite.
    #[inline]
    fn is_finite(&self) -> bool {
        true
    }

    /// Truncates `Time` to specified units.
    #[inline]
    fn truncate(&self, ty: FieldType, unit: DateUnit) -> Result<Self, DateTimeError> {
        let interval: Interval = From::from(*self);
        let it = interval.truncate(ty, unit)?;
        Ok(From::from(it))
    }
}

#[cfg(test)]
mod tests {
    use crate::error::DateTimeError;
    use crate::time::*;
    use crate::{DateOrder, DateStyle, DateTime, DateUnit, FieldType};

    #[test]
    fn test_time_from_str() -> Result<(), DateTimeError> {
        let time = Time::try_from_str("PST", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("azot", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("10.2 2010-10", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("dow345", -1, DateOrder::MDY);
        assert!(time.is_err());

        let time = Time::try_from_str("j-345", -1, DateOrder::MDY);
        assert!(time.is_err());

        let time = Time::try_from_str("azot45", -1, DateOrder::MDY);
        assert!(time.is_err());

        let time = Time::try_from_str("y1989m3d10.45", -1, DateOrder::MDY);
        assert!(time.is_err());

        let time = Time::try_from_str("allballs", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "00:00:00");

        let time = Time::try_from_str("2010-10-10 04:05 PM", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "16:05:00");

        let time = Time::try_from_str("040506", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:06");

        let time = Time::try_from_str("040506.456", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:06.456");

        let time: Time = Time::try_from_str("y1989m3d10h12mm30s20.4567", -1, DateOrder::MDY)?;
        assert_eq!(time.format(DateStyle::ISO)?, "12:30:20.4567".to_string());

        let time: Time = Time::try_from_str("y1989m3d10h12m30s20.4567", -1, DateOrder::MDY)?;
        assert_eq!(time.format(DateStyle::ISO)?, "12:30:20.4567".to_string());

        let time: Time = Time::try_from_str("J2451187.345", -1, DateOrder::MDY)?;
        assert_eq!(time.format(DateStyle::ISO)?, "08:16:47.999999".to_string());

        let time = Time::try_from_str("+567h", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("+567h", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("4 hours 5 minutes", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str(".2344", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("allballs", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "00:00:00");

        let time = Time::try_from_str("04:05:06.789t", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("t04:05:06.789", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:06.789");

        let time = Time::try_from_str("at 04:05:06.789", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:06.789");

        let time = Time::try_from_str("04:05:06", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:06");

        let time = Time::try_from_str("04:05", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:00");

        let time = Time::try_from_str("04:05.234", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "00:04:05.234");

        let time = Time::try_from_str("040506", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:06");

        let time = Time::try_from_str("04:05 AM", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "04:05:00");

        let time = Time::try_from_str("12:20:05 AM", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "00:20:05");

        let time = Time::try_from_str("04:05 PM", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "16:05:00");

        let ret = Time::try_from_str("04:05:06-08:00", 3, DateOrder::YMD);
        assert!(ret.is_err());

        let time = Time::try_from_str("2010-10-10 04:05 PM", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "16:05:00");

        let time = Time::try_from_str("14:04:05 AM", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("now", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("current", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("2003-04-12 04:05:06 America/New_York", 3, DateOrder::YMD);
        assert!(time.is_err());

        let time = Time::try_from_str("2003-04-12 04:05:06 shanghai", 3, DateOrder::YMD);
        assert!(time.is_err());
        Ok(())
    }

    #[test]
    fn test_time_range() -> Result<(), DateTimeError> {
        let time = Time::try_from_str("00:00:00", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "00:00:00");

        let time = Time::try_from_str("24:00:00", 3, DateOrder::YMD)?;
        assert_eq!(time.format(DateStyle::ISO)?, "24:00:00");

        let time = Time::try_from_str("24:00:01", 3, DateOrder::YMD);
        assert!(time.is_err());
        Ok(())
    }

    #[test]
    fn test_from_hms() -> Result<(), DateTimeError> {
        let time = Time::try_from_hms(23, 24, 56.67)?;
        assert_eq!(time.format(DateStyle::ISO)?, "23:24:56.67");

        let time = Time::try_from_hms(23, 24, 56.6789289)?;
        assert_eq!(time.format(DateStyle::ISO)?, "23:24:56.678929");

        let ret = Time::try_from_hms(-2, 24, 56.67);
        assert!(ret.is_err());

        let ret = Time::try_from_hms(-2, 24, 56.67);
        assert!(ret.is_err());

        let ret = Time::try_from_hms(23, -24, 56.67);
        assert!(ret.is_err());

        let ret = Time::try_from_hms(23, 24, -56.67);
        assert!(ret.is_err());
        let ret = Time::try_from_hms(25, 24, 56.67);
        assert!(ret.is_err());
        let time = Time::try_from_hms(24, 0, 0.0)?;
        assert_eq!(time.format(DateStyle::ISO)?, "24:00:00");
        let ret = Time::try_from_hms(24, 1, 0.0);
        assert!(ret.is_err());
        let ret = Time::try_from_hms(24, 0, 1.0);
        assert!(ret.is_err());

        let ret = Time::try_from_hms(23, 60, 1.0);
        assert!(ret.is_err());

        let time = Time::try_from_hms(23, 10, 60.0)?;
        assert_eq!(time.format(DateStyle::ISO)?, "23:11:00");

        let ret = Time::try_from_hms(23, 60, 60.1);
        assert!(ret.is_err());

        Ok(())
    }

    #[test]
    fn test_time_part() -> Result<(), DateTimeError> {
        let time = Time::try_from_str("20:45:34.673452", -1, DateOrder::YMD)?;
        let ret = time.date_part(FieldType::Unit, DateUnit::Hour)?;
        assert_eq!(ret, 20.0);

        let ret = time.date_part(FieldType::Unit, DateUnit::Minute)?;
        assert_eq!(ret, 45.0);

        let ret = time.date_part(FieldType::Unit, DateUnit::Second)?;
        assert_eq!(ret, 34.673452);

        let ret = time.date_part(FieldType::Unit, DateUnit::MilliSec)?;
        assert_eq!(ret, 34673.452);

        let ret = time.date_part(FieldType::Unit, DateUnit::MicroSec)?;
        assert_eq!(ret, 34673452.0);

        let ret = time.date_part(FieldType::Epoch, DateUnit::Epoch)?;
        assert_eq!(ret, 74734.673452);

        let ret = time.date_part(FieldType::Unit, DateUnit::Year);
        assert!(ret.is_err());

        let ret = time.date_part(FieldType::Epoch, DateUnit::Year);
        assert!(ret.is_err());

        Ok(())
    }

    #[test]
    fn test_time_truncate() -> Result<(), DateTimeError> {
        let time = Time::try_from_str("20:38:40.456789", 6, DateOrder::YMD)?;
        let ret = time.truncate(FieldType::Unit, DateUnit::Hour)?;
        assert_eq!(ret.format(DateStyle::ISO)?, "20:00:00");

        let ret = time.truncate(FieldType::Unit, DateUnit::Minute)?;
        assert_eq!(ret.format(DateStyle::ISO)?, "20:38:00");

        let ret = time.truncate(FieldType::Unit, DateUnit::Second)?;
        assert_eq!(ret.format(DateStyle::ISO)?, "20:38:40");

        let ret = time.truncate(FieldType::Unit, DateUnit::MilliSec)?;
        assert_eq!(ret.format(DateStyle::ISO)?, "20:38:40.456");

        let ret = time.truncate(FieldType::Unit, DateUnit::MicroSec)?;
        assert_eq!(ret.format(DateStyle::ISO)?, "20:38:40.456789");
        Ok(())
    }

    #[test]
    fn test_time_finite() -> Result<(), DateTimeError> {
        let time = Time::try_from_str("10:23:45", 6, DateOrder::YMD)?;
        assert!(!time.is_infinite());
        assert!(time.is_finite());

        Ok(())
    }

    #[test]
    fn test_adjust_by_type_mod() -> Result<(), DateTimeError> {
        let time = Time::from(-12345).adjust_by_typmod(5);
        assert_eq!(time.format(DateStyle::ISO)?, "00:00:00.01235");
        Ok(())
    }
}
