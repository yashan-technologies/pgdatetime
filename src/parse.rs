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

//! Parse functions for date time types.

use crate::common::*;
use crate::error::DateTimeError;
use crate::interval::*;
use crate::token::*;
use crate::{DateOrder, IntervalStyle};
use std::convert::TryFrom;

#[inline]
fn eat_whitespaces(s: &[u8]) -> &[u8] {
    let count = s.iter().take_while(|&i| i.is_ascii_whitespace()).count();
    &s[count..]
}

#[inline]
fn eat_digits(s: &[u8]) -> (&[u8], &[u8]) {
    let count = s.iter().take_while(|&i| i.is_ascii_digit()).count();
    (&s[0..count], &s[count..])
}

#[inline]
fn eat_alphabetics(s: &[u8]) -> (&[u8], &[u8]) {
    let count = s.iter().take_while(|&i| i.is_ascii_alphabetic()).count();
    (&s[0..count], &s[count..])
}

#[inline]
fn eat_digits_and_chars(s: &[u8], c: u8) -> (&[u8], &[u8]) {
    let count = s
        .iter()
        .take_while(|&&i| i == c || i.is_ascii_digit())
        .count();
    (&s[0..count], &s[count..])
}

#[inline]
fn eat_alphanumerics_and_chars(s: &[u8], c: u8) -> (&[u8], &[u8]) {
    let count = s
        .iter()
        .take_while(|&&i| i == c || i.is_ascii_alphanumeric())
        .count();
    (&s[0..count], &s[count..])
}

#[inline]
fn eat_special_chars<F>(s: &[u8], f: F) -> (&[u8], &[u8])
where
    F: Fn(u8) -> bool,
{
    let count = s.iter().take_while(|&&i| f(i)).count();
    (&s[0..count], &s[count..])
}

#[inline]
fn eat_time(s: &[u8]) -> (&[u8], &[u8]) {
    let count = s
        .iter()
        .take_while(|&&i| i.is_ascii_digit() || i == b':' || i == b'.')
        .count();
    (&s[0..count], &s[count..])
}

/// Splits a decimal string bytes into sign and the rest, without inspecting or validating the rest.
#[inline]
fn extract_sign(s: &[u8]) -> (i32, &[u8]) {
    match s.first() {
        Some(b'+') => (1, &s[1..]),
        Some(b'-') => (-1, &s[1..]),
        _ => (1, s),
    }
}

/// Parse date time string for digit and delim begin.
#[inline]
fn parse_date_time_for_digit_delim_begin<'a, 'b>(
    s: &'a [u8],
    current_buf: &'b mut [u8],
    f_types: &mut [TokenField],
    buf_pos: &mut usize,
    nf: usize,
) -> &'a [u8] {
    let delim = s.first().unwrap();
    f_types[nf] = if delim == &b'.' {
        TokenField::Number
    } else {
        TokenField::Date
    };
    current_buf[*buf_pos] = *delim;
    let s = &s[1..];
    *buf_pos += 1;
    match s.first() {
        Some(c) => {
            if c.is_ascii_digit() {
                let (field_str, s) = eat_digits(s);
                *buf_pos = copy_slice(current_buf, *buf_pos, field_str);
                match s.first() {
                    Some(c) => {
                        if c == delim {
                            f_types[nf] = TokenField::Date;
                            let (field_str, s) = eat_digits_and_chars(s, *delim);
                            *buf_pos = copy_slice(current_buf, *buf_pos, field_str);
                            s
                        } else {
                            s
                        }
                    }
                    None => s,
                }
            } else {
                f_types[nf] = TokenField::Date;
                let (field_str, s) = eat_alphanumerics_and_chars(s, *delim);
                let lower_str = field_str.to_ascii_lowercase();
                *buf_pos = copy_slice(current_buf, *buf_pos, lower_str.as_ref());
                s
            }
        }
        None => s,
    }
}

/// Parse date time string for digit begin.
#[inline]
fn parse_date_time_for_digit_begin<'a, 'b, 'c>(
    s: &'a [u8],
    current_buf: &'b mut [u8],
    fields: &'c mut [&'b [u8]],
    f_types: &mut [TokenField],
    nf: usize,
) -> (&'a [u8], &'b mut [u8]) {
    let (field_part0, s) = eat_digits(s);
    let mut buf_pos = copy_slice(current_buf, 0, field_part0);
    let s = match s.first() {
        Some(&b':') => {
            let (field_str, s) = eat_time(s);
            buf_pos = copy_slice(current_buf, buf_pos, field_str);
            f_types[nf] = TokenField::Time;
            s
        }
        Some(&b'-') | Some(&b'/') | Some(&b'.') => {
            parse_date_time_for_digit_delim_begin(s, current_buf, f_types, &mut buf_pos, nf)
        }

        _ => {
            f_types[nf as usize] = TokenField::Number;
            s
        }
    };

    let (field, current_buf) = current_buf.split_at_mut(buf_pos);
    fields[nf as usize] = field;
    (s, current_buf)
}

/// Parse date time string for dot begin.
#[inline]
fn parse_date_time_for_dot_begin<'a, 'b, 'c>(
    s: &'a [u8],
    current_buf: &'b mut [u8],
    fields: &'c mut [&'b [u8]],
    f_types: &mut [TokenField],
    nf: usize,
) -> (&'a [u8], &'b mut [u8]) {
    f_types[nf] = TokenField::Number;
    current_buf[0] = s[0];
    let s = &s[1..];
    let mut buf_pos = 1;
    let (field_str, s) = eat_digits(s);
    buf_pos = copy_slice(current_buf, buf_pos, field_str);
    let (filed, ret_buf) = current_buf.split_at_mut(buf_pos);
    fields[nf] = filed;
    (s, ret_buf)
}

/// Parse date time string for alpha begin.
fn parse_date_time_for_alpha_begin<'a, 'b, 'c>(
    s: &'a [u8],
    current_buf: &'b mut [u8],
    fields: &'c mut [&'b [u8]],
    f_types: &mut [TokenField],
    nf: usize,
) -> (&'a [u8], &'b mut [u8]) {
    f_types[nf] = TokenField::String;
    let (field_str, s) = eat_alphabetics(s);
    let field_str = field_str.to_ascii_lowercase();
    let mut buf_pos = copy_slice(current_buf, 0, field_str.as_ref());
    let is_date = match s.first() {
        Some(c) => {
            if c == &b'-' || c == &b'/' || c == &b'.' {
                true
            } else if c == &b'+' || c.is_ascii_digit() {
                let ret = search_date_token(field_str.as_ref());
                ret.is_none()
            } else {
                false
            }
        }
        None => false,
    };

    let s = if is_date {
        f_types[nf] = TokenField::Date;
        let (field_str, s) = eat_special_chars(s, |i| {
            i == b'+'
                || i == b'-'
                || i == b'/'
                || i == b'_'
                || i == b'.'
                || i == b':'
                || i.is_ascii_alphanumeric()
        });
        buf_pos = copy_slice(current_buf, buf_pos, field_str);
        s
    } else {
        s
    };

    let (filed, ret_buf) = current_buf.split_at_mut(buf_pos);
    fields[nf] = filed;
    (s, ret_buf)
}

/// Parse date time string for sign begin.
#[inline]
fn parse_date_time_for_sign_begin<'a, 'b, 'c>(
    s: &'a [u8],
    current_buf: &'b mut [u8],
    fields: &'c mut [&'b [u8]],
    f_types: &mut [TokenField],
    nf: usize,
) -> Result<(&'a [u8], &'b mut [u8]), DateTimeError> {
    current_buf[0] = s[0];
    let s = &s[1..];
    let s = eat_whitespaces(s);
    let mut buf_pos = 1;
    let s = match s.first() {
        Some(c) => {
            if c.is_ascii_digit() {
                f_types[nf] = TokenField::Tz;
                let (field_str, s) = eat_special_chars(s, |i| {
                    i.is_ascii_digit() || i == b':' || i == b'.' || i == b'-'
                });
                buf_pos = copy_slice(current_buf, buf_pos, field_str);
                s
            } else if c.is_ascii_alphabetic() {
                f_types[nf] = TokenField::Special;
                let (field_str, s) = eat_alphabetics(s);
                let save_buf_pos = buf_pos;
                buf_pos = copy_slice(current_buf, buf_pos, field_str);
                let buf = &mut current_buf[save_buf_pos..save_buf_pos + field_str.len()];
                buf.make_ascii_lowercase();
                s
            } else {
                return Err(DateTimeError::invalid(format!(
                    "date time expect digit or alpha after sign, but it is :{:?}",
                    c
                )));
            }
        }
        None => s,
    };

    let (filed, ret_buf) = current_buf.split_at_mut(buf_pos);
    fields[nf] = filed;
    Ok((s, ret_buf))
}

/// Parse date_time.
/// Break string into tokens based on a date/time context.
/// Returns 0 if successful, DTERR code if bogus input detected.
///
/// time_str - the input string
/// max_fields - dimensions of the above two arrays
///
/// The fields extracted from the input are stored as separate,
/// null-terminated strings in the workspace at workbuf. Any text is
/// converted to lower case.
///
/// Several field types are assigned:
/// DTK_NUMBER - digits and (possibly) a decimal point
/// DTK_DATE - digits and two delimiters, or digits and text
/// DTK_TIME - digits, colon delimiters, and possibly a decimal point
/// DTK_STRING - text (no digits or punctuation)
/// DTK_SPECIAL - leading "+" or "-" followed by text
/// DTK_TZ - leading "+" or "-" followed by digits (also eats ':', '.', '-')
///
/// Note that some field types can hold unexpected items:
/// DTK_NUMBER can hold date fields (yy.ddd)
/// DTK_STRING can hold months (January) and time zones (PST)
/// DTK_DATE can hold time zone names (America/New_York, GMT-8)
///
pub fn parse_date_time<'a, 'b>(
    time_str: &[u8],
    worker_buf: &'a mut [u8],
    fields: &'b mut [&'a [u8]],
    f_types: &mut [TokenField],
    max_fields: usize,
) -> Result<usize, DateTimeError>
where
    'a: 'b,
{
    let mut field_num: usize = 0;

    if time_str.is_empty() {
        return Err(DateTimeError::empty(
            "date time string is empty".to_string(),
        ));
    }

    let mut current_str = time_str;
    let mut current_buf = worker_buf;

    loop {
        current_str = eat_whitespaces(current_str);
        let first_char = current_str.first();
        match first_char {
            Some(char) => {
                if field_num >= max_fields {
                    return Err(DateTimeError::invalid(format!(
                        "field number: {:?} exceed max field number: {:?} at field: {:?}",
                        field_num, max_fields, time_str
                    )));
                }

                if char.is_ascii_digit() {
                    let (ret_str, ret_buf) = parse_date_time_for_digit_begin(
                        current_str,
                        current_buf,
                        fields,
                        f_types,
                        field_num,
                    );
                    current_str = ret_str;
                    current_buf = ret_buf;
                } else if char == &b'.' {
                    let (ret_str, ret_buf) = parse_date_time_for_dot_begin(
                        current_str,
                        current_buf,
                        fields,
                        f_types,
                        field_num,
                    );
                    current_str = ret_str;
                    current_buf = ret_buf;
                } else if char.is_ascii_alphabetic() {
                    let (ret_str, ret_buf) = parse_date_time_for_alpha_begin(
                        current_str,
                        current_buf,
                        fields,
                        f_types,
                        field_num,
                    );
                    current_str = ret_str;
                    current_buf = ret_buf;
                } else if *char == b'+' || *char == b'-' {
                    let (ret_str, ret_buf) = parse_date_time_for_sign_begin(
                        current_str,
                        current_buf,
                        fields,
                        f_types,
                        field_num,
                    )?;
                    current_str = ret_str;
                    current_buf = ret_buf;
                } else if char.is_ascii_punctuation() {
                    let (_field_str, s) =
                        eat_special_chars(current_str, |i| i.is_ascii_punctuation());
                    current_str = s;
                    continue;
                } else {
                    return Err(DateTimeError::invalid(format!(
                        "char: {:?} is not expected at str: {:?}",
                        char, time_str
                    )));
                }
            }
            None => break,
        }
        field_num += 1;
    }

    Ok(field_num)
}

#[inline]
fn str2i32(s: &[u8]) -> Result<(i32, &[u8]), DateTimeError> {
    let s = eat_whitespaces(s);
    if s.is_empty() {
        return Ok((0, s));
    }

    let (sign, s) = extract_sign(s);
    let (integral, s) = eat_digits(s);

    if integral.len() > 10 {
        return Err(DateTimeError::overflow());
    }

    let val = integral
        .iter()
        .fold(0_i64, |product, &i| product * 10 + (i - b'0') as i64);
    let val = val * sign as i64;

    if val >= i32::min_value() as i64 && val <= i32::max_value() as i64 {
        Ok((val as i32, s))
    } else {
        Err(DateTimeError::overflow())
    }
}

#[inline]
fn str2d(s: &[u8]) -> Result<(f64, &[u8]), DateTimeError> {
    let s = eat_whitespaces(s);
    if s.is_empty() {
        return Ok((0.0, s));
    }

    let (sign, s) = extract_sign(s);
    let (integral, s) = eat_digits(s);
    if integral.len() > 10 {
        return Err(DateTimeError::overflow());
    }

    let (fractional, s) = match s.first() {
        Some(b'.') => {
            let (fractional, s) = eat_digits(&s[1..]);
            (fractional, s)
        }
        _ => (b"".as_ref(), s),
    };

    if fractional.len() > 7 {
        return Err(DateTimeError::overflow());
    }

    let int = integral
        .iter()
        .fold(0, |val, &i| val * 10 + (i - b'0') as i64);
    let val = if !fractional.is_empty() {
        let float = fractional
            .iter()
            .fold(int, |val, &i| val * 10 + (i - b'0') as i64);
        let div = 10_i64.pow(fractional.len() as u32);
        (float * sign as i64) as f64 / div as f64
    } else {
        (int * sign as i64) as f64
    };

    Ok((val, s))
}

/// decode_timezone
/// Interpret string as a numeric timezone.
///
/// Return 0 if okay (and set *tzp), a DTERR code if not okay.
fn decode_timezone(_input: &[u8], _tz: &mut Option<i32>) -> Result<(), DateTimeError> {
    Err(DateTimeError::invalid(
        "timezone is not supported at current".to_string(),
    ))
    /* // Leading character must be "+" or "-" *
    match input.first() {
        Some(b'+') => {}
        Some(b'-') => {}
        _ => {
            return Err(DateTimeError::invalid(format!(
                "timezone should begin with '+' or '+', current field: {:?}",
                input
            )))
        }
    }

    let (s, hr) = str2i32(&input[1..])?;
    let (s, hr, min, sec) = match s.first() {
        Some(&b':') => {
            let (s, min) = str2i32(&s[1..])?;
            match s.first() {
                Some(&b':') => {
                    let (s, sec) = str2i32(&s[1..])?;
                    (s, hr, min, sec)
                }
                _ => (s, hr, min, 0),
            }
        }
        _ => {
            if input.len() > 3 {
                // We could, but don't, support a run-together hhmmss format
                (s, hr / 100, hr % 100, 0)
            } else {
                (s, hr, 0, 0)
            }
        }
    };

    if !s.is_empty() {
        return Err(DateTimeError::invalid(format!(
            "field: {:?} is invalid timezone",
            input
        )));
    }

    // Range-check the values
    if hr < 0
        || hr > MAX_TZDISP_HOUR
        || min < 0
        || min >= MINS_PER_HOUR
        || sec < 0
        || sec >= SECS_PER_MINUTE
    {
        return Err(DateTimeError::overflow());
    }

    match tz {
        Some(_z) => {
            return Err(DateTimeError::invalid(
                "timezone is not supported at current".to_string(),
            ))
        }
        None => {}
    }
    Ok(())*/
}

/// decode_number_field
/// Interpret numeric string as a concatenated date or time field.
/// Use the context of previously decoded fields to help with
/// the interpretation.
fn decode_number_field(
    str: &[u8],
    f_mask: i32,
    t_mask: &mut i32,
    tm: &mut PgTime,
    fsec: &mut i64,
    is2digits: &mut bool,
) -> Result<TokenField, DateTimeError> {
    let mut len = str.len();
    //Have a decimal point? Then this is a date or something with a seconds field.
    let count = str.iter().take_while(|&i| i != &b'.').count();
    if str.len() != count {
        /*
         * Can we use ParseFractionalSecond here?  Not clear whether trailing
         * junk should be rejected ...
         */
        let s = &str[count..];
        // Can we use ParseFractionalSecond here?  Not clear whether trailing
        // junk should be rejected.
        let (v, _left) = str2d(s)?;
        let v = (v * 1_000_000_f64).round() as i64;
        *fsec = v;
        len = count;
    }
    // No decimal point and no complete date yet.
    else if (f_mask & DTK_DATE_M) != DTK_DATE_M && len >= 6 {
        *t_mask = DTK_DATE_M;
        // Start from end and consider first 2 as Day, next 2 as Month,
        // and the rest as Year.
        let (v, _) = str2i32(&str[len - 2..])?;
        tm.mday = v;
        let (v, _) = str2i32(&str[len - 4..len - 2])?;
        tm.mon = v;
        let (v, _) = str2i32(&str[0..len - 4])?;
        tm.year = v;
        if (len - 4) == 2 {
            *is2digits = true;
        }

        return Ok(TokenField::Date);
    }

    // not all time fields are specified.
    if (f_mask & DTK_TIME_M) != DTK_TIME_M {
        // hhmmss.
        if len == 6 {
            *t_mask = DTK_TIME_M;
            let (v, _) = str2i32(&str[4..])?;
            tm.sec = v;
            let (v, _) = str2i32(&str[2..4])?;
            tm.min = v;
            let (v, _) = str2i32(&str[0..2])?;
            tm.hour = v;

            return Ok(TokenField::Time);
        }
        // hhmm.
        else if len == 4 {
            *t_mask = DTK_TIME_M;
            tm.sec = 0;
            let (v, _) = str2i32(&str[2..])?;
            tm.min = v;
            let (v, _) = str2i32(&str[0..2])?;
            tm.hour = v;

            return Ok(TokenField::Time);
        }
    }

    Err(DateTimeError::invalid(format!(
        "field: {:?} is invalid number field",
        str
    )))
}

thread_local!(static DATE_CACHE: RefCell<[Option<&'static DateToken>; MAX_DATE_FIELDS]> = RefCell::new([None; MAX_DATE_FIELDS]));

/// decode_special
///  Decode text string using lookup table.
///
/// Recognizes the keywords listed in datetktbl.
/// Note: at one time this would also recognize timezone abbreviations,
/// but no more; use DecodeTimezoneAbbrev for that.
///
/// Given string must be lowercased already.
///
/// Implement a cache lookup since it is likely that dates
/// will be related in format.
#[inline]
fn decode_special(field: usize, low_token: &[u8]) -> (TokenType, i32) {
    let tp = {
        let tp = DATE_CACHE.with(|tokens| tokens.borrow_mut()[field]);
        let key = if low_token.len() > MAX_TOKEN_LEN {
            &low_token[0..MAX_TOKEN_LEN]
        } else {
            low_token
        };

        if tp.is_none() || key != tp.unwrap().token {
            search_date_token(key)
        } else {
            tp
        }
    };

    match tp {
        None => (TokenType::UnknownField, 0),
        Some(t) => {
            DATE_CACHE.with(|tokens| tokens.borrow_mut()[field] = tp);
            (t.ty, t.value)
        }
    }
}

/// Fetch a fractional-second value with suitable error checking.
#[inline]
fn parse_fractional_second(s: &[u8]) -> Result<i64, DateTimeError> {
    debug_assert!(s[0] == b'.');
    let (frac, s) = str2d(s)?;
    // check for parse failure.
    match s.first() {
        None => {}
        Some(_) => {
            return Err(DateTimeError::invalid(format!(
                "after double, it also have str: {:?}",
                s
            )));
        }
    }
    let fsec = (frac * 1_000_000.0).round() as i64;
    Ok(fsec)
}

/// decode_number.
/// Interpret plain numeric field as a date value in context.
/// Return 0 if okay, a DTERR code if not.
#[allow(clippy::too_many_arguments)]
fn decode_number(
    str: &[u8],
    have_text_month: bool,
    f_mask: i32,
    t_mask: &mut i32,
    tm: &mut PgTime,
    fsec: &mut i64,
    is2digits: &mut bool,
    date_order: DateOrder,
) -> Result<(), DateTimeError> {
    *t_mask = 0;
    let (val, s) = str2i32(str)?;
    if s == str {
        return Err(DateTimeError::invalid(format!(
            "str: {:?} expect have digit",
            s
        )));
    }

    match s.first() {
        Some(&b'.') => {
            if str.len() - s.len() > 2 {
                let _ = decode_number_field(str, f_mask | DTK_DATE_M, t_mask, tm, fsec, is2digits);
                return Ok(());
            }
            *fsec = parse_fractional_second(s)?;
        }
        Some(c) => {
            return Err(DateTimeError::invalid(format!(
                "char: {:?} is not expect after digit",
                c
            )));
        }
        None => {}
    };

    // Special case for day of year
    if str.len() == 3 && (f_mask & DTK_DATE_M) == TokenType::YEAR.mask() && val >= 1 && val <= 366 {
        *t_mask = TokenType::DOY.mask() | TokenType::MONTH.mask() | TokenType::DAY.mask();
        tm.yday = val;
        // tm_mon and tm_mday can't actually be set yet.
        return Ok(());
    }

    // Switch based on what we have so far.
    match f_mask & DTK_DATE_M {
        0 => {
            // Nothing so far; make a decision about what we think the input
            // is.  There used to be lots of heuristics here, but the
            // consensus now is to be paranoid.  It *must* be either
            // YYYY-MM-DD (with a more-than-two-digit year field), or the
            // field order defined by DateOrder.

            if str.len() >= 3 || date_order == DateOrder::YMD {
                *t_mask = TokenType::YEAR.mask();
                tm.year = val;
            } else if date_order == DateOrder::DMY {
                *t_mask = TokenType::DAY.mask();
                tm.mday = val;
            } else {
                *t_mask = TokenType::MONTH.mask();
                tm.mon = val;
            }
        }
        DTK_YEAR_M => {
            // Must be at second field of YY-MM-DD
            *t_mask = TokenType::MONTH.mask();
            tm.mon = val;
        }
        DTK_MONTH_M => {
            if have_text_month {
                // We are at the first numeric field of a date that included a
                // textual month name.  We want to support the variants
                // MON-DD-YYYY, DD-MON-YYYY, and YYYY-MON-DD as unambiguous
                // inputs.  We will also accept MON-DD-YY or DD-MON-YY in
                // either DMY or MDY modes, as well as YY-MON-DD in YMD mode.

                if str.len() >= 3 || date_order == DateOrder::YMD {
                    *t_mask = TokenType::YEAR.mask();
                    tm.year = val;
                } else {
                    *t_mask = TokenType::DAY.mask();
                    tm.mday = val;
                }
            } else {
                // Must be at second field of MM-DD-YY.
                *t_mask = TokenType::DAY.mask();
                tm.mday = val;
            }
        }

        DTK_YEAR_MONTH_M => {
            if have_text_month {
                // Need to accept DD-MON-YYYY even in YMD mode.
                if str.len() >= 3 && *is2digits {
                    // Guess that first numeric field is day was wrong.
                    *t_mask = TokenType::DAY.mask(); // YEAR is already set .
                    tm.mday = tm.year;
                    tm.year = val;
                    *is2digits = false;
                } else {
                    *t_mask = TokenType::DAY.mask();
                    tm.mday = val;
                }
            } else {
                // Must be at third field of YY-MM-DD.
                *t_mask = TokenType::DAY.mask();
                tm.mday = val;
            }
        }

        DTK_DAY_M => {
            // Must be at second field of DD-MM-YY.
            *t_mask = TokenType::MONTH.mask();
            tm.mon = val;
        }

        DTK_MONTH_DAY_M => {
            // Must be at third field of DD-MM-YY or MM-DD-YY.
            *t_mask = TokenType::YEAR.mask();
            tm.year = val;
        }

        DTK_DATE_M => {
            // we have all the date, so it must be a time field.
            let _ = decode_number_field(str, f_mask, t_mask, tm, fsec, is2digits)?;
            return Ok(());
        }

        _ => {
            // Anything else is bogus input.
            return Err(DateTimeError::invalid(format!(
                "field mask: {:x} is not expect",
                f_mask & DTK_DATE_M
            )));
        }
    }

    // When processing a year field, mark it for adjustment if it's only one
    // or two digits.
    if *t_mask == TokenType::YEAR.mask() {
        *is2digits = str.len() <= 2;
    }

    Ok(())
}

/// decode date.
/// Decode date string which includes delimiters.
/// Return 0 if okay, a DTERR code if not.
///
/// str: field to be parsed
/// f_mask: bit_mask for field types already seen
/// t_mask: receives bit_mask for fields found here
/// is2digits: set to TRUE if we find 2-digit year
/// tm: field values are stored into appropriate members of this struct
fn decode_date(
    str: &[u8],
    f_mask: i32,
    t_mask: &mut i32,
    is2digits: &mut bool,
    tm: &mut PgTime,
    date_order: DateOrder,
) -> Result<(), DateTimeError> {
    let mut have_text_month = false;
    let mut f_mask = f_mask;
    let mut nf = 0;
    let mut fields: [Option<&[u8]>; MAX_DATE_FIELDS] = [None; MAX_DATE_FIELDS];
    let mut d_mask = 0;
    let mut fsec = 0;
    *t_mask = 0;
    let mut curr_s = str;

    while !curr_s.is_empty() && nf < MAX_DATE_FIELDS {
        // Skip field separators
        let (_, s) = eat_special_chars(curr_s, |i| !i.is_ascii_alphanumeric());
        curr_s = match s.first() {
            // End of string after separator.
            None => {
                return Err(DateTimeError::invalid(
                    "date end with special char".to_string(),
                ));
            }
            Some(c) => {
                if c.is_ascii_digit() {
                    let (f, p) = eat_digits(s);
                    fields[nf] = Some(f);
                    p
                } else if c.is_ascii_alphabetic() {
                    let (f, p) = eat_alphabetics(s);
                    fields[nf] = Some(f);
                    p
                } else {
                    continue;
                }
            }
        };
        nf += 1;
    }

    let ret_fields = &mut fields[0..nf];
    // look first for text fields, since that will be unambiguous month.
    for (i, field) in ret_fields.iter_mut().enumerate() {
        if let Some(f) = field {
            if let Some(c) = f.first() {
                if c.is_ascii_alphabetic() {
                    let (ty, val) = decode_special(i, f);
                    if ty == TokenType::IgnoreDtf {
                        continue;
                    }
                    let d_mask = ty.mask();
                    match ty {
                        TokenType::MONTH => {
                            tm.mon = val;
                            have_text_month = true;
                        }
                        _ => {
                            return Err(DateTimeError::invalid(format!(
                                "{:?} is not month in date",
                                f
                            )));
                        }
                    }

                    if f_mask & d_mask != 0 {
                        return Err(DateTimeError::invalid(format!(
                            "month appear more than one times, current month: {:?}",
                            f
                        )));
                    }

                    f_mask |= d_mask;
                    *t_mask |= d_mask;

                    // mark this field as being completed
                    *field = None;
                }
            }
        }
    }

    for field in ret_fields.iter() {
        match field {
            None => continue,
            Some(f) => {
                if f.is_empty() {
                    return Err(DateTimeError::invalid("date field is empty".to_string()));
                }

                decode_number(
                    f,
                    have_text_month,
                    f_mask,
                    &mut d_mask,
                    tm,
                    &mut fsec,
                    is2digits,
                    date_order,
                )?;
                if f_mask & d_mask != 0 {
                    return Err(DateTimeError::invalid(format!(
                        "date mask: {:x} appear more than one times",
                        d_mask
                    )));
                }

                f_mask |= d_mask;
                *t_mask |= d_mask;
            }
        }
    }

    if (f_mask & !(TokenType::DOY.mask() | TokenType::TZ.mask())) != DTK_DATE_M {
        return Err(DateTimeError::invalid(format!(
            "field mask: {:x} is invalid",
            f_mask
        )));
    }

    Ok(())
}

/// decode_time.
/// Decode time string which includes delimiters.
/// Return 0 if okay, a DTERR code if not.
///
/// Only check the lower limit on hours, since this same code can be
/// used to represent time spans.
fn decode_time(
    field: &[u8],
    _f_mask: i32,
    range: i32,
    t_mask: &mut i32,
    tm: &mut PgTime,
    f_sec: &mut i64,
) -> Result<(), DateTimeError> {
    *t_mask = DTK_TIME_M;
    let (v, s) = str2i32(field)?;
    tm.hour = v;
    match s.first() {
        Some(b':') => {}
        _ => {
            return Err(DateTimeError::invalid(format!(
                "field: {:?} is invalid time",
                field
            )));
        }
    }

    let (v, s) = str2i32(&s[1..])?;
    tm.min = v;

    match s.first() {
        None => {
            tm.sec = 0;
            *f_sec = 0;
            // If it's a MINUTE TO SECOND interval, take 2 fields as being mm:ss.
            if range == INTERVAL_MASK_MS {
                tm.sec = tm.min;
                tm.min = tm.hour;
                tm.hour = 0;
            }
        }
        Some(b'.') => {
            // always assume mm:ss.sss is MINUTE TO SECOND.
            *f_sec = parse_fractional_second(s)?;
            tm.sec = tm.min;
            tm.min = tm.hour;
            tm.hour = 0;
        }
        Some(b':') => {
            let (v, s) = str2i32(&s[1..])?;
            tm.sec = v;
            match s.first() {
                None => {
                    *f_sec = 0;
                }
                Some(b'.') => {
                    *f_sec = parse_fractional_second(s)?;
                }
                Some(c) => {
                    return Err(DateTimeError::invalid(format!(
                        "field:{:?} is invalid time for have unexpected char: {:?}",
                        field, c
                    )));
                }
            }
        }
        Some(_) => {
            return Err(DateTimeError::invalid(format!(
                "field: {:?} is invalid at time",
                field
            )));
        }
    }

    //Do a sanity check.
    if tm.hour < 0
        || tm.min < 0
        || tm.min > MINS_PER_HOUR - 1
        || tm.sec < 0
        || tm.sec > SECS_PER_MINUTE
        || *f_sec < 0
        || *f_sec > USECS_PER_SEC
    {
        Err(DateTimeError::invalid(format!("time: {:?} is invalid", tm)))
    } else {
        Ok(())
    }
}

#[inline]
fn date2time(jd: i64, hour: &mut i32, min: &mut i32, sec: &mut i32, fsec: &mut i64) {
    let mut time = jd;
    *hour = (time / USECS_PER_HOUR) as i32;
    time -= (*hour as i64) * USECS_PER_HOUR;
    *min = (time / USECS_PER_MINUTE) as i32;
    time -= (*min as i64) * USECS_PER_MINUTE;
    *sec = (time / USECS_PER_SEC) as i32;
    *fsec = time - ((*sec as i64) * USECS_PER_SEC);
}

/// Timestamp modulo.
/// We assume that int64 follows the C99 semantics for division (negative
/// quotients truncate towards zero).
#[inline]
fn timestamp_modulo(t: i64, per_unit: i64) -> (i64, i64) {
    let q = t / per_unit;
    let v = if q != 0 { t - q * per_unit } else { t };
    (v, q)
}

///  Convert timestamp data type to POSIX time structure.
/// Note that year is _not_ 1900-based, but is an explicit full value.
/// Also, month is one-based, _not_ zero-based.
/// Returns:
/// 0 on success
/// Error on out of range
pub fn timestamp2time(
    dt: i64,
    tz: &Option<&mut i32>,
    tm: &mut PgTime,
    fsec: &mut i64,
    tzn: &mut Option<&[u8]>,
    _at_time_zone: &Option<&mut PgTimezone>,
) -> Result<(), DateTimeError> {
    let (mut time, mut date) = timestamp_modulo(dt, USECS_PER_DAY);
    if time < 0 {
        time += USECS_PER_DAY;
        date -= 1;
    }

    // add offset to go from J2000 back to standard Julian date.
    date += POSTGRES_EPOCH_JDATE as i64;
    // Julian day routine does not work for negative Julian days.
    if date < 0 || date > std::i32::MAX as i64 {
        return Err(DateTimeError::overflow());
    }

    let (y, m, d) = julian2date(date as i32);
    tm.set_ymd(y, m, d);
    date2time(time, &mut tm.hour, &mut tm.min, &mut tm.sec, fsec);

    // Done if no TZ conversion wanted
    match tz {
        None => {
            tm.isdst = -1;
            tm.gmtoff = 0;
            tm.zone = None;
            *tzn = None;
        }
        Some(_) => {}
    }

    Ok(())
}

/// Interpret string as a timezone abbreviation, if possible.
///
/// Returns an abbreviation type (TZ, DTZ, or DYNTZ), or UNKNOWN_FIELD if
/// string is not any known abbreviation.  On success, set *offset and *tz to
/// represent the UTC offset (for TZ or DTZ) or underlying zone (for DYNTZ).
/// Note that full timezone names (such as America/New_York) are not handled
/// here, mostly for historical reasons.
///
/// Given string must be lowercased already.
///
/// Implement a cache lookup since it is likely that dates
/// will be related in format.
///
const PG_TIME_ZONE: PgTimezone = PgTimezone::new();

#[inline]
fn decode_timezone_abbrev(
    _field: usize,
    _low_token: &[u8],
    _offset: &mut i32,
) -> (TokenType, &'static PgTimezone) {
    // todo
    (TokenType::UnknownField, &PG_TIME_ZONE)
}

/// Load a timezone from file or from cache.
/// Does not verify that the timezone is acceptable!
///
/// "GMT" is always interpreted as the tzparse() definition, without attempting
/// to load a definition from the filesystem.  This has a number of benefits:
/// 1. It's guaranteed to succeed, so we don't have the failure mode wherein
/// the bootstrap default timezone setting doesn't work (as could happen if
/// the OS attempts to supply a leap-second-aware version of "GMT").
/// 2. Because we aren't accessing the filesystem, we can safely initialize
/// the "GMT" zone definition before my_exec_path is known.
/// 3. It's quick enough that we don't waste much time when the bootstrap
/// default timezone setting is later overridden from postgresql.conf.
#[inline]
fn pg_time_zone_set(_s: &[u8]) -> Result<Option<PgTimezone>, DateTimeError> {
    //todo
    Ok(None)
}

#[inline]
fn check_mask_set(f_mask: i32, t_mask: i32) -> Result<(), DateTimeError> {
    if t_mask & f_mask != 0 {
        Err(DateTimeError::invalid(format!(
            "time mask: {:x} have been set at field mask: {:x}",
            t_mask, f_mask
        )))
    } else {
        Ok(())
    }
}

#[inline]
fn check_mask_not_empty(f_mask: i32) -> Result<(), DateTimeError> {
    if f_mask == 0 {
        Err(DateTimeError::invalid(
            "field mask of interval is empty".to_string(),
        ))
    } else {
        Ok(())
    }
}

/// Context for decode date time.
struct DecodeDateTimeCtx {
    d_type: TokenField,
    f_sec: i64,
    f_mask: i32,
    t_mask: i32,
    p_type: TokenField,
    // "prefix type" for ISO h04mm05s06 format.
    is_julian: bool,
    is2digits: bool,
    bc: bool,
    mer: i32,
    have_text_month: bool,
}

impl DecodeDateTimeCtx {
    #[inline]
    fn new() -> Self {
        Self {
            d_type: TokenField::Date,
            f_sec: 0,
            f_mask: 0,
            t_mask: 0,
            p_type: TokenField::Number,
            is_julian: false,
            is2digits: false,
            bc: false,
            mer: HR24,
            have_text_month: false,
        }
    }

    /// decode date.
    /// Decode date string which includes delimiters.
    /// Return 0 if okay, a DTERR code if not.
    ///
    /// str: field to be parsed
    /// f_mask: bit_mask for field types already seen
    /// t_mask: receives bit_mask for fields found here
    /// is2digits: set to TRUE if we find 2-digit year
    /// tm: field values are stored into appropriate members of this struct
    fn decode_date(
        &mut self,
        str: &[u8],
        tm: &mut PgTime,
        date_order: DateOrder,
    ) -> Result<(), DateTimeError> {
        let mut have_text_month = false;
        let mut f_mask = self.f_mask;
        let mut nf = 0;
        let mut fields: [Option<&[u8]>; MAX_DATE_FIELDS] = [None; MAX_DATE_FIELDS];
        let mut d_mask = 0;
        let mut fsec = 0;
        self.t_mask = 0;
        let mut curr_s = str;

        while !curr_s.is_empty() && nf < MAX_DATE_FIELDS {
            // Skip field separators
            let (_, s) = eat_special_chars(curr_s, |i| !i.is_ascii_alphanumeric());
            curr_s = match s.first() {
                // End of string after separator.
                None => {
                    return Err(DateTimeError::invalid(
                        "date end with special char".to_string(),
                    ));
                }
                Some(c) => {
                    if c.is_ascii_digit() {
                        let (f, p) = eat_digits(s);
                        fields[nf] = Some(f);
                        p
                    } else if c.is_ascii_alphabetic() {
                        let (f, p) = eat_alphabetics(s);
                        fields[nf] = Some(f);
                        p
                    } else {
                        continue;
                    }
                }
            };
            nf += 1;
        }

        let ret_fields = &mut fields[0..nf];
        // look first for text fields, since that will be unambiguous month.
        for (i, field) in ret_fields.iter_mut().enumerate() {
            if let Some(f) = field {
                if let Some(c) = f.first() {
                    if c.is_ascii_alphabetic() {
                        let (ty, val) = decode_special(i, f);
                        if ty == TokenType::IgnoreDtf {
                            continue;
                        }
                        let d_mask = ty.mask();
                        match ty {
                            TokenType::MONTH => {
                                tm.mon = val;
                                have_text_month = true;
                            }
                            _ => {
                                return Err(DateTimeError::invalid(format!(
                                    "{:?} is not month in date",
                                    f
                                )));
                            }
                        }

                        if f_mask & d_mask != 0 {
                            return Err(DateTimeError::invalid(format!(
                                "month appear more than one times, current month: {:?}",
                                f
                            )));
                        }

                        f_mask |= d_mask;
                        self.t_mask |= d_mask;

                        // mark this field as being completed
                        *field = None;
                    }
                }
            }
        }

        for field in ret_fields.iter() {
            match field {
                None => continue,
                Some(f) => {
                    if f.is_empty() {
                        return Err(DateTimeError::invalid("date field is empty".to_string()));
                    }

                    decode_number(
                        f,
                        have_text_month,
                        f_mask,
                        &mut d_mask,
                        tm,
                        &mut fsec,
                        &mut self.is2digits,
                        date_order,
                    )?;
                    if f_mask & d_mask != 0 {
                        return Err(DateTimeError::invalid(format!(
                            "date mask: {:x} appear more than one times",
                            d_mask
                        )));
                    }

                    f_mask |= d_mask;
                    self.t_mask |= d_mask;
                }
            }
        }

        if (f_mask & !(TokenType::DOY.mask() | TokenType::TZ.mask())) != DTK_DATE_M {
            return Err(DateTimeError::invalid(format!(
                "field mask: {:x} is invalid",
                f_mask
            )));
        }

        Ok(())
    }

    fn decode_date_token(
        &mut self,
        field: &[u8],
        tm: &mut PgTime,
        tz: &mut Option<i32>,
        date_order: DateOrder,
    ) -> Result<(), DateTimeError> {
        // Integral julian day with attached time zone? All other
        // forms with JD will be separated into distinct fields, so we
        // handle just this case here.
        if self.p_type == TokenField::Julian {
            let (val, s) = str2i32(field)?;
            let (y, m, d) = julian2date(val);
            tm.set_ymd(y, m, d);
            self.is_julian = true;
            // Get the time zone from the end of the string
            decode_timezone(s, tz)?;
        /*            self.t_mask = DTK_DATE_M | DTK_TIME_M | DTK_TIME_ZONE_M;
        self.p_type = 0;*/
        }
        // Already have a date? Then this might be a time zone name
        // with embedded punctuation (e.g. "America/New_York") or a
        // run-together time with trailing time zone (e.g. hhmmss-zz).
        // - thomas 2001-12-25
        //
        // We consider it a time zone if we already have month & day.
        // This is to allow the form "mmm dd hhmmss tz year", which
        // we've historically accepted.
        else if self.p_type != TokenField::Number
            || (self.f_mask & (TokenType::MONTH.mask() | TokenType::DAY.mask()))
                == (TokenType::MONTH.mask() | TokenType::DAY.mask())
        {
            if let Some(c) = field.first() {
                if c.is_ascii_digit() || self.p_type != TokenField::Number {
                    if self.p_type != TokenField::Number {
                        // Sanity check; should not fail this test
                        if self.p_type != TokenField::Time {
                            return Err(DateTimeError::invalid(format!(
                                "prefix type: {:?} is not time",
                                self.p_type
                            )));
                        } else {
                            self.p_type = TokenField::Number;
                        }

                        // Starts with a digit but we already have a time
                        // field? Then we are in trouble with a date and time
                        // already.
                        if (self.f_mask & DTK_TIME_M) == DTK_TIME_M {
                            return Err(DateTimeError::invalid(format!(
                                "time have been set as field mask: {:x}",
                                self.f_mask
                            )));
                        }

                        let count = field.iter().take_while(|&i| i != &b'-').count();
                        if count == field.len() {
                            return Err(DateTimeError::invalid(
                                "can not find '-' flag".to_string(),
                            ));
                        }

                        // Get the time zone from the end of the string.
                        decode_timezone(&field[1..], tz)?;
                        /*                        let _ = decode_number_field(
                            &field[0..count],
                            self.f_mask,
                            &mut self.t_mask,
                            tm,
                            &mut self.f_sec,
                            &mut self.is2digits,
                        )?;
                        self.t_mask |= date_token_mask(TZ);*/
                    }
                } else {
                    //Todo get local time zone
                    return Err(DateTimeError::invalid(
                        "unsupport time zone now".to_string(),
                    ));
                }
            }
        } else {
            self.decode_date(field, tm, date_order)?;
        };

        Ok(())
    }

    #[allow(clippy::if_same_then_else)]
    fn decode_number_token(
        &mut self,
        str: &[u8],
        tm: &mut PgTime,
        _tz: &mut Option<i32>,
        date_order: DateOrder,
    ) -> Result<(), DateTimeError> {
        // Was this an "ISO date" with embedded field labels? An
        // example is "y2001m02d04" - thomas 2001-02-04
        if self.p_type != TokenField::Number {
            let (val, s) = str2i32(str)?;
            // only a few kinds are allowed to have an embedded decimal.
            match s.first() {
                Some(b'.') => match self.p_type {
                    TokenField::Julian | TokenField::Time | TokenField::Second => {}
                    _ => {
                        return Err(DateTimeError::invalid(format!(
                            "prefix type: {:?} do not expect '.' at field: {:?}",
                            self.p_type, str
                        )));
                    }
                },
                Some(_) => {
                    return Err(DateTimeError::invalid(format!(
                        "field: {:?} and prefix type: {:?} is invalid",
                        str, self.p_type
                    )));
                }
                None => {}
            };

            match self.p_type {
                TokenField::Year => {
                    tm.year = val;
                    self.t_mask = TokenType::YEAR.mask();
                }
                TokenField::Month => {
                    //already have a month and hour? then assume minutes.
                    if (self.f_mask & TokenType::MONTH.mask()) != 0
                        && (self.f_mask & TokenType::HOUR.mask()) != 0
                    {
                        tm.min = val;
                        self.t_mask = TokenType::MINUTE.mask();
                    } else {
                        tm.mon = val;
                        self.t_mask = TokenType::MONTH.mask();
                    }
                }
                TokenField::Day => {
                    tm.mday = val;
                    self.t_mask = TokenType::DAY.mask();
                }
                TokenField::Hour => {
                    tm.hour = val;
                    self.t_mask = TokenType::HOUR.mask();
                }
                TokenField::Minute => {
                    tm.min = val;
                    self.t_mask = TokenType::MINUTE.mask();
                }
                TokenField::Second => {
                    tm.sec = val;
                    self.t_mask = TokenType::SECOND.mask();
                    if let Some(b'.') = s.first() {
                        self.f_sec = parse_fractional_second(s)?;
                        self.t_mask = DTK_ALL_SECS_M;
                    }
                }

                /*            DTK_TZ => {
                    *t_mask = date_token_mask(TZ);
                    decode_timezone(str, tz)?;
                }*/
                TokenField::Julian => {
                    // Previous field was a label for "julian date".
                    if val < 0 {
                        return Err(DateTimeError::overflow());
                    }
                    self.t_mask = DTK_DATE_M;
                    let (y, m, d) = julian2date(val);
                    tm.set_ymd(y, m, d);
                    self.is_julian = true;

                    // fractional Julian Day?.
                    if let Some(b'.') = s.first() {
                        let (time, _s) = str2d(s)?;
                        let mut time = time;
                        time *= USECS_PER_DAY as f64;
                        date2time(
                            time as i64,
                            &mut tm.hour,
                            &mut tm.min,
                            &mut tm.sec,
                            &mut self.f_sec,
                        );
                        self.t_mask |= DTK_TIME_M;
                    }
                }

                TokenField::Time => {
                    // previous field was "t" for ISO time.
                    let _ = decode_number_field(
                        str,
                        self.f_mask | DTK_DATE_M,
                        &mut self.t_mask,
                        tm,
                        &mut self.f_sec,
                        &mut self.is2digits,
                    )?;
                    if self.t_mask != DTK_TIME_M {
                        return Err(DateTimeError::invalid(format!(
                            "prexfix time expects field: {:?} is not time mask at field: {:?}",
                            self.p_type, str
                        )));
                    }
                }

                _ => {
                    return Err(DateTimeError::invalid(format!(
                        "prefix type: {:?} is not expected at field: {:?}",
                        self.p_type, str
                    )));
                }
            };

            self.p_type = TokenField::Number;
            self.d_type = TokenField::Date;
        } else {
            let count = str.iter().take_while(|&i| i != &b'.').count();
            // Embedded decimal and no date yet? Only do this if
            // we're not looking at something like YYYYMMDDHHMMSS.mm
            // count != str.len() mean found.
            if count != str.len() && ((self.f_mask & DTK_DATE_M) == 0) && str.len() <= 14 {
                self.decode_date(str, tm, date_order)?;
            }
            // embedded decimal and several digits before.
            else if count != str.len() && count > 2 {
                // Interpret as a concatenated date or time Set the
                // type field to allow decoding other fields later.
                // Example: 20011223 or 040506
                let _ = decode_number_field(
                    str,
                    self.f_mask,
                    &mut self.t_mask,
                    tm,
                    &mut self.f_sec,
                    &mut self.is2digits,
                )?;
            }
            // Is this a YMD or HMS specification, or a year number?
            // YMD and HMS are required to be six digits or more, so
            // if it is 5 digits, it is a year.  If it is six or more
            // more digits, we assume it is YMD or HMS unless no date
            // and no time values have been specified.  This forces 6+
            // digit years to be at the end of the string, or to use
            // the ISO date specification.
            else if str.len() >= 6
                && ((self.f_mask & DTK_DATE_M == 0) || (self.f_mask & DTK_TIME_M == 0))
            {
                let _ = decode_number_field(
                    str,
                    self.f_mask,
                    &mut self.t_mask,
                    tm,
                    &mut self.f_sec,
                    &mut self.is2digits,
                )?;
            }
            // otherwise it is a single date/time field.
            else {
                decode_number(
                    str,
                    false,
                    self.f_mask,
                    &mut self.t_mask,
                    tm,
                    &mut self.f_sec,
                    &mut self.is2digits,
                    date_order,
                )?;
            }
        }

        Ok(())
    }

    fn decode_string_and_special_token(
        &mut self,
        field_id: usize,
        ftypes: &[TokenField],
        field: &[u8],
        tm: &mut PgTime,
        tz: &mut Option<i32>,
    ) -> Result<(bool, Option<PgTimezone>), DateTimeError> {
        let nf = ftypes.len();
        let mut named_tz = None;
        let mut val = 0;
        // timezone abbrevs take precedence over built-in tokens.
        let (t, _valtz) = decode_timezone_abbrev(field_id, field, &mut val);
        let mut ty = t;
        if ty == TokenType::UnknownField {
            let (t, v) = decode_special(field_id, field);
            ty = t;
            val = v;
        }
        if ty == TokenType::IgnoreDtf {
            return Ok((true, None)); // after return this funtion, need to return.
        }

        self.t_mask = ty.mask();
        match ty {
            TokenType::RESERV => {
                match TokenField::try_from(val)? {
                    TokenField::Current
                    | TokenField::Now
                    | TokenField::Yesterday
                    | TokenField::Today
                    | TokenField::Tomorrow => {
                        // For this date need current transaction time and
                        // session time zone, so do not implement at this crate.
                        return Err(DateTimeError::invalid(format!(
                            "{:?} is not supported at current",
                            val
                        )));
                    }

                    TokenField::Zulu => {
                        self.t_mask = DTK_TIME_M | TokenType::TZ.mask();
                        self.d_type = TokenField::Date;
                        tm.hour = 0;
                        tm.min = 0;
                        tm.sec = 0;
                        match tz {
                            Some(z) => *z = 0,
                            None => {}
                        }
                    }

                    _ => self.d_type = TokenField::try_from(val)?,
                }
            }

            TokenType::MONTH => {
                // already have a (numeric) month? then see if we can
                // substitute.
                if (self.f_mask & TokenType::MONTH.mask() != 0)
                    && !(self.have_text_month)
                    && (self.f_mask & TokenType::DAY.mask() == 0)
                    && tm.mon >= 1
                    && tm.mon <= 31
                {
                    tm.mday = tm.mon;
                    self.t_mask = TokenType::DAY.mask();
                }
                self.have_text_month = true;
                tm.mon = val;
            }

            /*  DTZMOD => {
                // daylight savings time modifier (solves "MET DST"
                // syntax)
                *t_mask |= date_token_mask(DTZ);
                tm.tm_isdst = 1;
                match tz {
                    None => return Err(DateTimeError::invalid("timezone is null".to_string())),
                    Some(z) => *z = *z - val,
                }
            }

            DTZ => {
                // set mask for TZ here _or_ check for DTZ later when
                // getting default timezone
                *t_mask |= date_token_mask(TZ);
                tm.tm_isdst = 1;
                match tz {
                    None => return Err(DateTimeError::invalid("timezone is null".to_string())),
                    Some(z) => *z = -val,
                }
            }
            TZ => {
                tm.tm_isdst = 0;
                match tz {
                    Some(z) => *z = -val,
                    None => return Err(DateTimeError::invalid("timezone is null".to_string())),
                }
            }
            DYNTZ => {
                *t_mask |= date_token_mask(TZ);
                // we'll determine the actual offset later.
                //todo
                //abbrevTz = valtz;
                //abbrev = str;
            }*/
            TokenType::AMPM => {
                self.mer = val;
            }
            TokenType::ADBC => {
                self.bc = val == BC;
            }
            TokenType::DOW => {
                tm.wday = val;
            }
            TokenType::UNITS => {
                self.t_mask = 0;
                self.p_type = TokenField::try_from(val)?;
            }

            TokenType::ISOTIME => {
                // This is a filler field "t" indicating that the next
                // field is time. Try to verify that this is sensible.
                self.t_mask = 0;

                // No preceding date? Then quit.
                if (self.f_mask & DTK_DATE_M) != DTK_DATE_M {
                    return Err(DateTimeError::invalid(format!(
                        "field mask: {:x} do not have time mask",
                        self.f_mask
                    )));
                }

                // We will need one of the following fields:
                //	DTK_NUMBER should be hhmmss.fff
                //	DTK_TIME should be hh:mm:ss.fff
                //	DTK_DATE should be hhmmss-zz
                if field_id >= nf - 1
                    || (ftypes[field_id + 1] != TokenField::Number
                        && ftypes[field_id + 1] != TokenField::Time
                        && ftypes[field_id + 1] != TokenField::Date)
                {
                    return Err(DateTimeError::invalid(format!(
                        "the {:?} field's type: {:?} is unexpected",
                        field_id,
                        ftypes[field_id + 1]
                    )));
                }
                self.p_type = TokenField::try_from(val)?;
            }

            TokenType::UnknownField => {
                // Before giving up and declaring error, check to see
                // if it is an all-alpha timezone nam
                named_tz = pg_time_zone_set(field)?;
                match named_tz {
                    Some(ref _tz) => {}
                    None => return Err(DateTimeError::invalid("timezone is null".to_string())),
                }
                self.t_mask = TokenType::TZ.mask();
            }
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "type: {:?} is unexpected",
                    ty
                )));
            }
        }
        Ok((false, named_tz))
    }
}

pub fn decode_date_time(
    fields: &[&[u8]],
    f_types: &[TokenField],
    tm: &mut PgTime,
    tz: &mut Option<i32>,
    date_order: DateOrder,
) -> Result<(i32, TokenField, i64), DateTimeError> {
    let mut ctx = DecodeDateTimeCtx::new();
    match tz {
        Some(z) => *z = 0,
        None => {}
    }
    ctx.f_sec = 0;
    for (i, f_type) in f_types.iter().enumerate() {
        match f_type {
            TokenField::Date => {
                ctx.decode_date_token(fields[i], tm, tz, date_order)?;
            }
            TokenField::Time => {
                // This might be an ISO time following a "t" field.
                if ctx.p_type != TokenField::Number {
                    // Sanity check; should not fail this test.
                    if ctx.p_type != TokenField::Time {
                        return Err(DateTimeError::invalid(format!(
                            "prefix type: {:?} is not time",
                            ctx.p_type
                        )));
                    }
                    ctx.p_type = TokenField::Number;
                }

                decode_time(
                    fields[i],
                    ctx.f_mask,
                    INTERVAL_FULL_RANGE,
                    &mut ctx.t_mask,
                    tm,
                    &mut ctx.f_sec,
                )?;

                // Check upper limit on hours; other limits checked in decode_time.
                // test for > 24:00:00
                if tm.hour > HOURS_PER_DAY
                    || (tm.hour == HOURS_PER_DAY && (tm.min > 0 || tm.sec > 0 || ctx.f_sec > 0))
                {
                    return Err(DateTimeError::overflow());
                }
            }
            /*            FieldType::DTKTz => {
                decode_timezone(fields[i], tz)?;
                t_mask = date_token_mask(TZ);
            }*/
            TokenField::Number => {
                ctx.decode_number_token(fields[i], tm, tz, date_order)?;
            }
            TokenField::String | TokenField::Special => {
                let (continue_flag, _) =
                    ctx.decode_string_and_special_token(i, f_types, fields[i], tm, tz)?;
                if continue_flag {
                    continue;
                }
            }
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "unsupported field type: {:?}",
                    f_type
                )));
            }
        }

        check_mask_set(ctx.f_mask, ctx.t_mask)?;
        ctx.f_mask |= ctx.t_mask;
    }

    // do final checking/adjustment of Y/M/D fields.
    tm.validate_date(ctx.f_mask, ctx.is_julian, ctx.is2digits, ctx.bc)?;
    // handle AM/PM.
    if ctx.mer != HR24 && tm.hour > HOURS_PER_DAY / 2 {
        return Err(DateTimeError::overflow());
    }
    if ctx.mer == AM && tm.hour == HOURS_PER_DAY / 2 {
        tm.hour = 0;
    } else if ctx.mer == PM && tm.hour != HOURS_PER_DAY / 2 {
        tm.hour += HOURS_PER_DAY / 2;
    }

    /* do additional checking for full date specs... */
    if ctx.d_type == TokenField::Date && (ctx.f_mask & DTK_DATE_M) != DTK_DATE_M {
        if (ctx.f_mask & DTK_TIME_M) == DTK_TIME_M {
            return Ok((1, ctx.d_type, ctx.f_sec));
        }
        return Err(DateTimeError::invalid(format!(
            "date type's field mask: {:x} is invalid",
            ctx.f_mask
        )));
        // todo add time zone process.
    }
    Ok((0, ctx.d_type, ctx.f_sec))
}

/// Context for decode time only.
struct DecodeTimeOnlyCtx {
    d_type: TokenField,
    f_sec: i64,
    f_mask: i32,
    t_mask: i32,
    p_type: TokenField,
    // "prefix type" for ISO h04mm05s06 format.
    is_julian: bool,
    is2digits: bool,
    bc: bool,
    mer: i32,
}

impl DecodeTimeOnlyCtx {
    #[inline]
    fn new() -> Self {
        Self {
            d_type: TokenField::Date,
            f_sec: 0,
            f_mask: 0,
            t_mask: 0,
            p_type: TokenField::Number,
            is_julian: false,
            is2digits: false,
            bc: false,
            mer: HR24,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_date_token(
        &mut self,
        field_id: usize,
        f_types: &mut [TokenField],
        field: &[u8],
        tm: &mut PgTime,
        tz: &mut Option<i32>,
        date_order: DateOrder,
    ) -> Result<(), DateTimeError> {
        // Time zone not allowed? Then should not accept dates or time
        // zones no matter what else!
        if tz.is_none() {
            return Err(DateTimeError::invalid(
                "only time should have timezone".to_string(),
            ));
        }

        let field_num = f_types.len();
        // Under limited circumstances, we will accept a date.
        if field_id == 0
            && field_num >= 2
            && (f_types[field_num - 1] == TokenField::Date || f_types[1] == TokenField::Time)
        {
            decode_date(
                field,
                self.f_mask,
                &mut self.t_mask,
                &mut self.is2digits,
                tm,
                date_order,
            )?;
        }
        // Otherwise, this is a time and/or time zone.
        else {
            match field.first() {
                Some(_c) => {
                    /*if c.is_ascii_digit() {
                        // Starts with a digit but we already have a time
                        // field? Then we are in trouble with time already.
                        if (f_mask & DTK_TIME_M) == DTK_TIME_M {
                            return Err(DateTimeError::invalid(format!(
                                "time mask have been set at field mask: {:x}",
                                f_mask
                            )));
                        }

                        // Should not get here and fail. Sanity check only.
                        let count = field.iter().take_while(|&i| i == &b'-').count();
                        if count == field.len() {
                            return Err(DateTimeError::invalid(
                                "can not find '-' after digit".to_string(),
                            ));
                        }
                        let s = &field[count..];

                        // Get the time zone from the end of the string.
                        decode_timezone(s, tz)?;

                        // Then read the rest of the field as a concatenated time.

                        let ret = decode_number_field(
                            &field[0..count],
                            f_mask | DTK_DATE_M,
                            t_mask,
                            tm,
                            fsec,
                            is2digits,
                        )?;
                        *(&mut f_types[field_id]) = TryFrom::try_from(ret)?;
                        *t_mask |= date_token_mask(TZ);
                    } else {*/
                    // todo timezone
                    return Err(DateTimeError::invalid(
                        "timezone is not supported at current".to_string(),
                    ));
                    //}
                }
                None => {
                    // todo timezone
                    return Err(DateTimeError::invalid(
                        "timezone is not supported at current".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn decode_number_token(
        &mut self,
        field_id: usize,
        f_types: &mut [TokenField],
        field: &[u8],
        tm: &mut PgTime,
        tz: &mut Option<i32>,
        date_order: DateOrder,
    ) -> Result<(), DateTimeError> {
        // Was this an "ISO time" with embedded field labels? An
        // example is "h04m05s06" - thomas 2001-02-04
        if self.p_type != TokenField::Number {
            // Only accept a date under limited circumstances.
            match self.p_type {
                TokenField::Julian | TokenField::Year | TokenField::Month | TokenField::Day => {
                    if tz.is_none() {
                        return Err(DateTimeError::invalid(format!(
                            "prefix type: {:?} is not expected at time only field: {:?}",
                            self.p_type, field
                        )));
                    }
                }
                _ => {}
            }

            let (val, s) = str2i32(field)?;
            // only a few kinds are allowed to have an embedded decimal.
            match s.first() {
                Some(b'.') => match self.p_type {
                    TokenField::Julian | TokenField::Time | TokenField::Second => {}
                    _ => {
                        return Err(DateTimeError::invalid(format!(
                            "prefix type: {:?} do not expect '.'",
                            self.p_type
                        )));
                    }
                },
                Some(_) => {
                    return Err(DateTimeError::invalid(format!(
                        "field: {:?} is invalid",
                        field
                    )));
                }
                None => {}
            };

            match self.p_type {
                TokenField::Year => {
                    tm.year = val;
                    self.t_mask = TokenType::YEAR.mask();
                }
                TokenField::Month => {
                    //already have a month and hour? then assume minutes.
                    if (self.f_mask & TokenType::MONTH.mask()) != 0
                        && (self.f_mask & TokenType::HOUR.mask()) != 0
                    {
                        tm.min = val;
                        self.t_mask = TokenType::MINUTE.mask();
                    } else {
                        tm.mon = val;
                        self.t_mask = TokenType::MONTH.mask();
                    }
                }
                TokenField::Day => {
                    tm.mday = val;
                    self.t_mask = TokenType::DAY.mask();
                }
                TokenField::Hour => {
                    tm.hour = val;
                    self.t_mask = TokenType::HOUR.mask();
                }
                TokenField::Minute => {
                    tm.min = val;
                    self.t_mask = TokenType::MINUTE.mask();
                }
                TokenField::Second => {
                    tm.sec = val;
                    self.t_mask = TokenType::SECOND.mask();
                    if let Some(b'.') = s.first() {
                        self.f_sec = parse_fractional_second(s)?;
                        self.t_mask = DTK_ALL_SECS_M;
                    }
                }

                TokenField::Tz => {
                    self.t_mask = TokenType::TZ.mask();
                    decode_timezone(field, tz)?;
                }

                TokenField::Julian => {
                    // Previous field was a label for "julian date".
                    if val < 0 {
                        return Err(DateTimeError::overflow());
                    }
                    self.t_mask = DTK_DATE_M;
                    let (y, m, d) = julian2date(val);
                    tm.set_ymd(y, m, d);
                    self.is_julian = true;

                    // fractional Julian Day?.
                    if let Some(b'.') = s.first() {
                        let (time, _s) = str2d(s)?;
                        let mut time = time;
                        time *= USECS_PER_DAY as f64;
                        date2time(
                            time as i64,
                            &mut tm.hour,
                            &mut tm.min,
                            &mut tm.sec,
                            &mut self.f_sec,
                        );
                        self.t_mask |= DTK_TIME_M;
                    }
                }

                TokenField::Time => {
                    // previous field was "t" for ISO time.
                    let ret = decode_number_field(
                        field,
                        self.f_mask | DTK_DATE_M,
                        &mut self.t_mask,
                        tm,
                        &mut self.f_sec,
                        &mut self.is2digits,
                    )?;
                    if self.t_mask != DTK_TIME_M {
                        return Err(DateTimeError::invalid(format!(
                            "field: {:?} is invalid for time mask is not set at field mask: {:x}",
                            self.t_mask, self.f_mask
                        )));
                    }

                    f_types[field_id] = ret;
                }

                _ => {
                    return Err(DateTimeError::invalid(format!(
                        "prefix type: {:?} is invalid",
                        self.p_type
                    )));
                }
            };

            self.p_type = TokenField::Number;
            self.d_type = TokenField::Date;
        } else {
            let count = field.iter().take_while(|&i| i != &b'.').count();
            let field_num = f_types.len();
            //Under limited circumstances, we will accept a date.
            if count != field.len() {
                if field_id == 0 && field_num >= 2 && f_types[field_num - 1] == TokenField::Date {
                    decode_date(
                        field,
                        self.f_mask,
                        &mut self.t_mask,
                        &mut self.is2digits,
                        tm,
                        date_order,
                    )?;
                }
                // embedded decimal and several digits before.
                else if count != field.len() && count > 2 {
                    // Interpret as a concatenated date or time Set the
                    // type field to allow decoding other fields later.
                    // Example: 20011223 or 040506
                    let ret = decode_number_field(
                        field,
                        self.f_mask | DTK_DATE_M,
                        &mut self.t_mask,
                        tm,
                        &mut self.f_sec,
                        &mut self.is2digits,
                    )?;
                    f_types[field_id] = ret as TokenField;
                } else {
                    return Err(DateTimeError::invalid(format!(
                        "field: {:?} is invalid",
                        field
                    )));
                }
            } else if field.len() > 4 {
                let ret = decode_number_field(
                    field,
                    self.f_mask | DTK_DATE_M,
                    &mut self.t_mask,
                    tm,
                    &mut self.f_sec,
                    &mut self.is2digits,
                )?;
                f_types[field_id] = ret;
            }
            // otherwise it is a single date/time field.
            else {
                decode_number(
                    field,
                    false,
                    self.f_mask | DTK_DATE_M,
                    &mut self.t_mask,
                    tm,
                    &mut self.f_sec,
                    &mut self.is2digits,
                    date_order,
                )?;
            }
        }

        Ok(())
    }

    fn decode_string_and_special_token(
        &mut self,
        field_id: usize,
        f_types: &mut [TokenField],
        str: &[u8],
        tm: &mut PgTime,
        _tz: &mut Option<i32>,
    ) -> Result<(bool, Option<PgTimezone>), DateTimeError> {
        let field_num = f_types.len();
        let mut named_tz = None;
        let mut val = 0;
        // timezone abbrevs take precedence over built-in tokens.
        let (t, _valtz) = decode_timezone_abbrev(field_id, str, &mut val);
        let mut ty = t;
        if ty == TokenType::UnknownField {
            let (t, v) = decode_special(field_id, str);
            ty = t;
            val = v;
        }
        if ty == TokenType::IgnoreDtf {
            return Ok((true, None)); // after return this funtion, need to return.
        }

        self.t_mask = ty.mask();
        match ty {
            TokenType::RESERV => {
                match TokenField::try_from(val)? {
                    TokenField::Current | TokenField::Now => {
                        // For this date need current transaction time and
                        // session time zone, so do not implement at this crate.
                        return Err(DateTimeError::invalid(format!(
                            "type: {:?} is not support as time",
                            val
                        )));
                    }

                    TokenField::Zulu => {
                        self.t_mask = DTK_TIME_M | TokenType::TZ.mask();
                        self.d_type = TokenField::Time;
                        tm.hour = 0;
                        tm.min = 0;
                        tm.sec = 0;
                        tm.isdst = 0;
                    }

                    _ => {
                        return Err(DateTimeError::invalid(format!(
                            "type: {:?} is not support at time",
                            val
                        )));
                    }
                }
            }

            // timezone todo
            TokenType::AMPM => {
                self.mer = val;
            }
            TokenType::ADBC => {
                self.bc = val == BC;
            }
            TokenType::UNITS => {
                self.t_mask = 0;
                self.p_type = TokenField::try_from(val)?;
            }

            TokenType::ISOTIME => {
                self.t_mask = 0;

                // We will need one of the following fields:
                //	DTK_NUMBER should be hhmmss.fff
                //	DTK_TIME should be hh:mm:ss.fff
                //	DTK_DATE should be hhmmss-zz
                if field_id >= field_num - 1
                    || (f_types[field_id + 1] != TokenField::Number
                        && f_types[field_id + 1] != TokenField::Time
                        && f_types[field_id + 1] != TokenField::Date)
                {
                    return Err(DateTimeError::invalid(format!(
                        "the {:?} type :{:?} is unexpected",
                        field_id, f_types[field_id]
                    )));
                }
                self.p_type = TokenField::try_from(val)?;
            }

            TokenType::UnknownField => {
                // Before giving up and declaring error, check to see
                // if it is an all-alpha timezone nam
                named_tz = pg_time_zone_set(str)?;
                match named_tz {
                    Some(ref _tz) => {}
                    None => return Err(DateTimeError::invalid("timezone is none".to_string())),
                }
                self.t_mask = TokenType::TZ.mask();
            }
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "type: {:?} is not support",
                    val
                )));
            }
        }
        Ok((false, named_tz))
    }
}

pub fn decode_time_only(
    fields: &[&[u8]],
    f_types: &mut [TokenField],
    tm: &mut PgTime,
    tz: &mut Option<i32>,
    date_order: DateOrder,
) -> Result<i64, DateTimeError> {
    let mut ctx = DecodeTimeOnlyCtx::new();
    match tz {
        Some(z) => *z = 0,
        None => {}
    }
    ctx.f_sec = 0;
    for (i, &field) in fields.iter().enumerate() {
        let f_type = f_types[i];
        match f_type {
            TokenField::Date => {
                ctx.decode_date_token(i, f_types, field, tm, tz, date_order)?;
            }
            TokenField::Time => {
                decode_time(
                    field,
                    ctx.f_mask | DTK_DATE_M,
                    INTERVAL_FULL_RANGE,
                    &mut ctx.t_mask,
                    tm,
                    &mut ctx.f_sec,
                )?;
            }
            TokenField::Tz => {
                if tz.is_none() {
                    return Err(DateTimeError::invalid("timezone is null".to_string()));
                }
                decode_timezone(field, tz)?;
                ctx.t_mask = TokenType::TZ.mask();
            }
            TokenField::Number => {
                ctx.decode_number_token(i, f_types, field, tm, tz, date_order)?;
            }
            TokenField::String | TokenField::Special => {
                let (continue_flag, _) =
                    ctx.decode_string_and_special_token(i, f_types, field, tm, tz)?;
                if continue_flag {
                    continue;
                }
            }
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "field type: {:?} is invalid for field: {:?}",
                    f_type, field
                )));
            }
        }

        check_mask_set(ctx.f_mask, ctx.t_mask)?;
        ctx.f_mask |= ctx.t_mask;
    }

    // do final checking/adjustment of Y/M/D fields.
    tm.validate_date(ctx.f_mask, ctx.is_julian, ctx.is2digits, ctx.bc)?;
    // handle AM/PM.
    if ctx.mer != HR24 && tm.hour > HOURS_PER_DAY / 2 {
        return Err(DateTimeError::overflow());
    }
    if ctx.mer == AM && tm.hour == HOURS_PER_DAY / 2 {
        tm.hour = 0;
    } else if ctx.mer == PM && tm.hour != HOURS_PER_DAY / 2 {
        tm.hour += HOURS_PER_DAY / 2;
    }

    // This should match the checks in make_timestamp_internal
    if tm.hour < 0
        || tm.min < 0
        || tm.min > MINS_PER_HOUR - 1
        || tm.sec < 0
        || tm.sec > SECS_PER_MINUTE
        || tm.hour > HOURS_PER_DAY
        || (tm.hour == HOURS_PER_DAY && (tm.min > 0 || tm.sec > 0 || ctx.f_sec > 0))
        || ctx.f_sec < 0
        || ctx.f_sec > USECS_PER_SEC
    {
        return Err(DateTimeError::overflow());
    }

    // To do for time zone
    Ok(ctx.f_sec)
}

/// Multiply frac by scale (to produce seconds) and add to *tm & *fsec.
/// We assume the input frac is less than 1 so overflow is not an issue.
#[inline]
fn adjust_fract_seconds(
    frac: f64,
    tm: &mut PgTime,
    fsec: &mut i64,
    scale: i32,
) -> Result<(), DateTimeError> {
    if frac == 0.0 {
        return Ok(());
    }

    let mut frac = frac * scale as f64;
    let sec = frac as i32;
    let ret = tm.sec.checked_add(sec);
    match ret {
        Some(s) => tm.sec = s,
        None => return Err(DateTimeError::overflow()),
    }
    frac -= sec as f64;
    *fsec += (frac * 1_000_000_f64).round() as i64;
    Ok(())
}

/// As above, but initial scale produces days.
#[inline]
fn adjust_fract_days(
    frac: f64,
    tm: &mut PgTime,
    fsec: &mut i64,
    scale: i32,
) -> Result<(), DateTimeError> {
    if frac == 0.0 {
        return Ok(());
    }

    let mut frac = frac * scale as f64;
    let extra_days = frac as i32;
    let ret = tm.mday.checked_add(extra_days);
    match ret {
        Some(d) => tm.mday = d,
        None => return Err(DateTimeError::overflow()),
    }
    frac -= extra_days as f64;
    adjust_fract_seconds(frac, tm, fsec, SECS_PER_DAY)?;
    Ok(())
}

fn decode_interval_number_field(
    field: &[u8],
    tm: &mut PgTime,
    ty: &mut i32,
    range: i32,
    f_val: &mut f64,
    f_sec: &mut i64,
    t_mask: &mut i32,
) -> Result<(), DateTimeError> {
    if *ty == TokenType::IgnoreDtf as i32 {
        // typmod to decide what rightmost field is.
        match range {
            INTERVAL_MASK_Y => {
                *ty = TokenField::Year.value();
            }
            INTERVAL_MASK_M | INTERVAL_MASK_YM => {
                *ty = TokenField::Month.value();
            }
            INTERVAL_MASK_D => {
                *ty = TokenField::Day.value();
            }
            INTERVAL_MASK_H | INTERVAL_MASK_DH => {
                *ty = TokenField::Hour.value();
            }
            INTERVAL_MASK_MIN | INTERVAL_MASK_HM | INTERVAL_MASK_DHM => {
                *ty = TokenField::Minute.value();
            }
            INTERVAL_MASK_S | INTERVAL_MASK_MS | INTERVAL_MASK_HMS | INTERVAL_MASK_DHMS => {
                *ty = TokenField::Second.value();
            }
            _ => {
                *ty = TokenField::Second.value();
            }
        }
    }

    let (mut val, s) = str2i32(field)?;
    match s.first() {
        Some(b'-') => {
            // SQL "years-months" syntax.

            let (mut val2, s) = str2i32(&s[1..])?;
            if val2 < 0 || val2 >= MONTHS_PER_YEAR {
                return Err(DateTimeError::overflow());
            }
            if !s.is_empty() {
                return Err(DateTimeError::invalid(
                    "after '-' and digit, str is not expect empty".to_string(),
                ));
            }
            *ty = TokenField::Month.value();

            if field[0] == b'-' {
                val2 = -val2
            }
            if (val as f64 * MONTHS_PER_YEAR as f64 + val2 as f64) > std::i32::MAX as f64
                || (val as f64 * MONTHS_PER_YEAR as f64 + val2 as f64) < std::i32::MIN as f64
            {
                return Err(DateTimeError::overflow());
            }
            val = val * MONTHS_PER_YEAR + val2;
            *f_val = 0.0;
        }
        Some(b'.') => {
            let (v, s) = str2d(s)?;
            if !s.is_empty() {
                return Err(DateTimeError::invalid(
                    "after double, str is not expect empty".to_string(),
                ));
            }
            *f_val = if field[0] == b'-' { -v } else { v };
        }
        None => *f_val = 0.0,
        _ => {
            return Err(DateTimeError::invalid(format!(
                "char: {:?} is not expected after digit",
                s.first()
            )));
        }
    }

    *t_mask = 0; // DTK_M(type)

    match TokenField::try_from(*ty)? {
        TokenField::Microsec => {
            *f_sec += (val as f64 + *f_val).round() as i64;
            *t_mask = TokenType::MICROSECOND.mask();
        }
        TokenField::Millisec => {
            // avoid overflowing the fsec field.
            tm.sec += val / 1000;
            val -= (val / 1000) * 1000;
            *f_sec += ((val as f64 + *f_val) * 1000_f64).round() as i64;
            *t_mask = TokenType::MILLISECOND.mask();
        }

        TokenField::Second => {
            tm.sec += val;
            *f_sec += (*f_val * 1_000_000_f64).round() as i64;

            // If any subseconds were specified, consider this
            // microsecond and millisecond input as well.
            if *f_val == 0.0 {
                *t_mask = TokenType::SECOND.mask();
            } else {
                *t_mask = DTK_ALL_SECS_M;
            }
        }

        TokenField::Minute => {
            tm.min += val;
            adjust_fract_seconds(*f_val, tm, f_sec, SECS_PER_MINUTE)?;
            *t_mask = TokenType::MINUTE.mask();
        }

        TokenField::Hour => {
            tm.hour += val;
            adjust_fract_seconds(*f_val, tm, f_sec, SECS_PER_HOUR)?;
            *t_mask = TokenType::HOUR.mask();
            *ty = TokenField::Day.value(); /* set for next field */
        }

        TokenField::Day => {
            tm.mday += val;
            adjust_fract_seconds(*f_val, tm, f_sec, SECS_PER_DAY)?;
            *t_mask = TokenType::DAY.mask();
        }

        TokenField::Week => {
            tm.mday += val * 7;
            adjust_fract_days(*f_val, tm, f_sec, 7)?;
            *t_mask = TokenType::WEEK.mask();
        }

        TokenField::Month => {
            tm.mon += val;
            adjust_fract_days(*f_val, tm, f_sec, DAYS_PER_MONTH)?;
            *t_mask = TokenType::MONTH.mask();
        }

        TokenField::Year => {
            tm.year += val;
            if *f_val != 0.0 {
                tm.mon += (*f_val * MONTHS_PER_YEAR as f64) as i32;
            }
            *t_mask = TokenType::YEAR.mask();
        }

        TokenField::Decade => {
            tm.year += val * 10;
            if *f_val != 0.0 {
                tm.mon += (*f_val * MONTHS_PER_YEAR as f64 * 10_f64) as i32;
            }
            *t_mask = TokenType::DECADE.mask();
        }

        TokenField::Century => {
            tm.year += val * 100;
            if *f_val != 0.0 {
                tm.mon += (*f_val * MONTHS_PER_YEAR as f64 * 100_f64) as i32;
            }
            *t_mask = TokenType::CENTURY.mask();
        }

        TokenField::Millennium => {
            tm.year += val * 1000;
            if *f_val != 0.0 {
                tm.mon += (*f_val * MONTHS_PER_YEAR as f64 * 1000_f64) as i32;
            }
            *t_mask = TokenType::MILLENNIUM.mask();
        }

        _ => {
            return Err(DateTimeError::invalid(format!(
                "type: {:?} is not expected at interval",
                *ty
            )));
        }
    }
    Ok(())
}

use crate::common::POSTGRES_EPOCH_JDATE;
use crate::timezone::PgTimezone;
use std::cell::RefCell;
thread_local!(static DELTA_CACHE: RefCell<[Option<&'static DateToken>; MAX_DATE_FIELDS]> = RefCell::new([None; MAX_DATE_FIELDS]));

/// Decode text string using lookup table.

/// This routine recognizes keywords associated with time interval units.
///
/// Given string must be lowercased already.
///
/// Implement a cache lookup since it is likely that dates
/// will be related in format.
#[inline]
fn decode_units(field: usize, low_token: &[u8]) -> (TokenType, i32) {
    let token = {
        let t: Option<&DateToken> = DELTA_CACHE.with(|tokens| tokens.borrow_mut()[field]);
        // use strncmp so that we match truncated tokens.
        let key = if low_token.len() > MAX_TOKEN_LEN {
            &low_token[0..MAX_TOKEN_LEN]
        } else {
            low_token
        };
        if t.is_none() || t.unwrap().token != key {
            search_delta_token(key)
        } else {
            t
        }
    };

    match token {
        None => (TokenType::UnknownField, 0),
        Some(t) => {
            DELTA_CACHE.with(|tokens| tokens.borrow_mut()[field] = token);
            (t.ty, t.value)
        }
    }
}

/// Interpret previously parsed fields for general time interval.
/// Returns 0 if successful, DTERR code if bogus input detected.
/// dtype, tm, fsec are output parameters.
///
/// Allow "date" field DTK_DATE since this could be just
/// an unsigned floating point number. - thomas 1997-11-16
///
/// Allow ISO-style time span, with implicit units on number of days
/// preceding an hh:mm:ss field. - thomas 1998-04-30
pub fn decode_interval(
    fields: &[&[u8]],
    f_types: &[TokenField],
    range: i32,
    d_type: &mut TokenField,
    tm: &mut PgTime,
    f_sec: &mut i64,
    interval_style: IntervalStyle,
) -> Result<(), DateTimeError> {
    let mut is_before = false;
    let mut f_mask = 0;
    let mut t_mask = 0;
    *d_type = TokenField::Delta;
    let mut ty = TokenType::IgnoreDtf as i32;
    *f_sec = 0;

    let mut fval = 0.0;
    let field_num = fields.len();
    // read through list backwards to pick up units before values.
    for i in 0..field_num {
        let field_idx = field_num - i - 1;
        let f_type = f_types[field_idx];
        let field = fields[field_idx];
        match f_type {
            TokenField::Time => {
                decode_time(field, f_mask, range, &mut t_mask, tm, f_sec)?;
                ty = TokenField::Day.value();
            }

            TokenField::Tz => {
                // Timezone means a token with a leading sign character and at
                // least one digit; there could be ':', '.', '-' embedded in
                // it as well.
                debug_assert!(field[0] == b'-' || field[0] == b'+');

                // Check for signed hh:mm or hh:mm:ss.  If so, process exactly
                // like DTK_TIME case above, plus handling the sign.
                let find = field.iter().find(|&&i| i == b':');
                match find {
                    Some(_) => {
                        decode_time(&field[1..], f_mask, range, &mut t_mask, tm, f_sec)?;

                        if field[0] == b'-' {
                            // flip the sign on all fields.
                            tm.hour = -tm.hour;
                            tm.min = -tm.min;
                            tm.sec = -tm.sec;
                            *f_sec = -(*f_sec);
                        }
                        // Set the next type to be a day, if units are not
                        // specified. This handles the case of '1 +02:03' since we
                        // are reading right to left.
                        ty = TokenField::Day.value();
                    }
                    None => {
                        decode_interval_number_field(
                            field,
                            tm,
                            &mut ty,
                            range,
                            &mut fval,
                            f_sec,
                            &mut t_mask,
                        )?;
                    }
                }
            }
            TokenField::Date | TokenField::Number => {
                decode_interval_number_field(
                    field,
                    tm,
                    &mut ty,
                    range,
                    &mut fval,
                    f_sec,
                    &mut t_mask,
                )?;
            }
            TokenField::String | TokenField::Special => {
                let (token_ty, val) = decode_units(field_idx, field);
                if token_ty == TokenType::IgnoreDtf {
                    continue;
                }

                t_mask = 0; // DTK_M(type)
                match token_ty {
                    TokenType::UNITS => {
                        ty = val;
                    }
                    TokenType::AGO => {
                        is_before = true;
                        ty = val;
                    }
                    TokenType::RESERV => {
                        t_mask = DTK_DATE_M | DTK_TIME_M;
                        *d_type = TokenField::try_from(val)?;
                    }
                    _ => {
                        return Err(DateTimeError::invalid(format!(
                            "the type: {:?} of field: {:?} is invalid",
                            ty, field
                        )));
                    }
                }
            }
            _ => {
                return Err(DateTimeError::invalid(format!(
                    "filed type: {:?} is invaid for interval",
                    f_type
                )));
            }
        }

        check_mask_set(f_mask, t_mask)?;
        f_mask |= t_mask;
    }

    // ensure that at least one time field has been found.
    check_mask_not_empty(f_mask)?;

    // ensure fractional seconds are fractional.
    if *f_sec != 0 {
        let sec = *f_sec / USECS_PER_SEC;
        *f_sec -= sec * USECS_PER_SEC;
        tm.sec += sec as i32;
    }

    // The SQL standard defines the interval literal
    //	 '-1 1:00:00'
    // to mean "negative 1 days and negative 1 hours", while Postgres
    // traditionally treats this as meaning "negative 1 days and positive
    // 1 hours".  In SQL_STANDARD intervalstyle, we apply the leading sign
    // to all fields if there are no other explicit signs.
    //
    // We leave the signs alone if there are additional explicit signs.
    // This protects us against misinterpreting postgres-style dump output,
    // since the postgres-style output code has always put an explicit sign on
    // all fields following a negative field.  But note that SQL-spec output
    // is ambiguous and can be misinterpreted on load!	(So it's best practice
    // to dump in postgres style, not SQL style.)
    let field = fields[0];
    if interval_style == IntervalStyle::SQLStandard && field[0] == b'-' {
        // Check for additional explicit sign
        let mut more_signs = false;
        for field in fields.iter().skip(1) {
            if !field.is_empty() && (field[0] == b'-' || field[0] == b'+') {
                more_signs = true;
                break;
            }
        }

        if !more_signs {
            // Rather than re-determining which field was field[0], just force
            // 'em all negative.
            if *f_sec > 0 {
                *f_sec = -(*f_sec);
            }
            if tm.sec > 0 {
                tm.sec = -tm.sec;
            }
            if tm.min > 0 {
                tm.min = -tm.min;
            }
            if tm.hour > 0 {
                tm.hour = -tm.hour;
            }
            if tm.mday > 0 {
                tm.mday = -tm.mday;
            }
            if tm.mon > 0 {
                tm.mon = -tm.mon;
            }
            if tm.year > 0 {
                tm.year = -tm.year;
            }
        }
    }

    // finally, AGO negates everything.
    if is_before {
        *f_sec = -(*f_sec);
        tm.sec = -tm.sec;
        tm.min = -tm.min;
        tm.hour = -tm.hour;
        tm.mday = -tm.mday;
        tm.mon = -tm.mon;
        tm.year = -tm.year;
    }

    Ok(())
}

/// Helper functions to avoid duplicated code in DecodeISO8601Interval.
///
/// Parse a decimal value and break it into integer and fractional parts.
/// Returns 0 or DTERR code.
#[inline]
fn parse_iso8601_number<'a, 'b, 'c>(
    s: &'a [u8],
    ipart: &'b mut i32,
    fpart: &'c mut f64,
) -> Result<&'a [u8], DateTimeError> {
    match s.first() {
        None => {
            return Err(DateTimeError::invalid(
                "str is empty for iso number".to_string(),
            ));
        }
        Some(c) => {
            if !(c.is_ascii_digit() || c == &b'c' || c == &b'.') {
                return Err(DateTimeError::invalid(format!(
                    "str: {:?} is invalid for iso number",
                    s
                )));
            }
        }
    }

    let (val, end) = str2d(s)?;
    if end.len() == s.len() {
        return Err(DateTimeError::invalid(format!("str: {:?} is not digit", s)));
    }
    // watch out for overflow */
    if val < std::i32::MIN as f64 || val > std::i32::MAX as f64 {
        return Err(DateTimeError::overflow());
    }
    // be very sure we truncate towards zero (cf dtrunc()).
    if val >= 0.0 {
        *ipart = val.floor() as i32;
    } else {
        *ipart = -val.floor() as i32;
    }
    *fpart = val - *ipart as f64;
    Ok(end)
}

/// Determine number of integral digits in a valid ISO 8601 number field
/// (we should ignore sign and any fraction part)
#[inline]
fn iso8601_integer_width(field_start: &[u8]) -> i32 {
    // We might have had a leading '-'.
    let s = if !field_start.is_empty() && field_start[0] == b'-' {
        &field_start[1..]
    } else {
        field_start
    };

    s.iter().take_while(|&&i| i >= b'0' && i <= b'9').count() as i32
}

#[allow(clippy::too_many_arguments)]
fn decode_date_part_left<'a, 'b, 'c, 'd>(
    s: &'a [u8],
    unit: u8,
    tm: &'b mut PgTime,
    datepart: &'c mut bool,
    havefield: &'c mut bool,
    val: &'d mut i32,
    fval: &'d mut f64,
    fsec: &'b mut i64,
) -> Result<(bool, &'a [u8]), DateTimeError> {
    // ISO 8601 4.4.3.3 Alternative Format, Extended.
    if *havefield {
        return Err(DateTimeError::invalid(
            "decode date part left foutn have field.".to_string(),
        ));
    }

    let mut s = s;

    tm.year += *val;
    tm.mon += (*fval * MONTHS_PER_YEAR as f64) as i32;
    if unit == b'\0' {
        return Ok((true, s));
    }
    if unit == b'T' {
        *datepart = false;
        *havefield = false;
        return Ok((false, s));
    }

    s = parse_iso8601_number(s, val, fval)?;
    tm.mon += *val;
    adjust_fract_days(*fval, tm, fsec, DAYS_PER_MONTH)?;
    match s.first() {
        None => return Ok((true, s)),
        Some(c) => {
            if c == &b'T' {
                *datepart = false;
                *havefield = false;
                return Ok((false, s));
            }
            if c != &b'-' {
                return Err(DateTimeError::invalid(format!(
                    "expect '-', but it is: {:?}",
                    c
                )));
            }
        }
    }

    s = &s[1..];
    s = parse_iso8601_number(s, val, fval)?;
    tm.mday += *val;
    adjust_fract_seconds(*fval, tm, fsec, SECS_PER_DAY)?;
    match s.first() {
        None => Ok((true, s)),
        Some(c) => {
            if c == &b'T' {
                *datepart = false;
                *havefield = false;
                Ok((false, s))
            } else {
                Err(DateTimeError::invalid(format!(
                    "expect 'T', but it is: {:?}",
                    c
                )))
            }
        }
    }
}

#[inline]
fn decode_time_part_left(
    s: &[u8],
    unit: u8,
    tm: &mut PgTime,
    havefield: bool,
    val: &mut i32,
    fval: &mut f64,
    fsec: &mut i64,
) -> Result<(), DateTimeError> {
    if havefield {
        return Err(DateTimeError::invalid("have field is set".to_string()));
    }

    tm.hour += *val;
    adjust_fract_seconds(*fval, tm, fsec, SECS_PER_HOUR)?;
    if unit == b'\0' {
        return Ok(());
    }

    let mut s = parse_iso8601_number(s, val, fval)?;
    tm.min += *val;
    adjust_fract_seconds(*fval, tm, fsec, SECS_PER_MINUTE)?;
    match s.first() {
        None => return Ok(()),
        Some(c) => {
            if c != &b':' {
                return Err(DateTimeError::invalid(format!(
                    "char: {:?} is unexpected at str: {:?}",
                    s, c
                )));
            }
        }
    }
    s = &s[1..];
    s = parse_iso8601_number(s, val, fval)?;
    tm.sec += *val;
    adjust_fract_seconds(*fval, tm, fsec, 1)?;
    match s.first() {
        None => Ok(()),
        Some(_) => Err(DateTimeError::invalid(format!(
            "str: {:?} is after double",
            s
        ))),
    }
}

/// Decode an ISO 8601 time interval of the "format with designators"
/// (section 4.4.3.2) or "alternative format" (section 4.4.3.3)
/// Examples: P1D for 1 day
///      PT1H for 1 hour
///      P2Y6M7DT1H30M for 2 years, 6 months, 7 days 1 hour 30 min
///      P0002-06-07T01:30:00 the same value in alternative format
///
/// Returns 0 if successful, DTERR code if bogus input detected.
/// Note: error code should be DTERR_BAD_FORMAT if input doesn't look like
/// ISO8601, otherwise this could cause unexpected error messages.
/// dtype, tm, fsec are output parameters.
///
/// A couple exceptions from the spec:
/// - a week field ('W') may coexist with other units
/// - allows decimals in fields other than the least significant unit.
pub fn decode_iso8601_interval(
    s: &[u8],
    dtype: &mut TokenField,
    tm: &mut PgTime,
    fsec: &mut i64,
) -> Result<(), DateTimeError> {
    let mut datepart = true;
    let mut havefield = false;

    *dtype = TokenField::Delta;
    *fsec = 0;

    if s.len() < 2 || s[0] != b'P' {
        return Err(DateTimeError::invalid(format!(
            "str: {:?} is not expect at field",
            s
        )));
    }
    let mut s = &s[1..];
    while !s.is_empty() {
        let mut fval = 0.0;
        let mut val = 0;
        if s[0] == b'T'
        // T indicates the beginning of the time part.
        {
            datepart = false;
            havefield = false;
            s = &s[1..];
            continue;
        }

        let field_start = s;
        s = parse_iso8601_number(s, &mut val, &mut fval)?;

        // Note: we could step off the end of the string here.  Code below
        //must* exit the loop if unit == '\0'.
        let unit = if !s.is_empty() {
            let c = s[0];
            s = &s[1..];
            c
        } else {
            b'\0'
        };
        if datepart {
            match unit        // before T: Y M W D.
            {
                b'Y' => {
                    tm.year += val;
                    tm.mon += (fval * MONTHS_PER_YEAR as f64) as i32;
                }
                b'M' => {
                    tm.mon += val;
                    adjust_fract_days(fval, tm, fsec, DAYS_PER_MONTH)?;
                }
                b'W' => {
                    tm.mday += val * 7;
                    adjust_fract_days(fval, tm, fsec, 7)?;
                }
                b'D' => {
                    tm.mday += val;
                    adjust_fract_seconds(fval, tm, fsec, SECS_PER_DAY)?;
                }
                b'T' | b'\0' => {
                    // ISO 8601 4.4.3.3 Alternative Format / Basic */
                    let width = iso8601_integer_width(field_start);
                    if width == 8 && !havefield
                    {
                        tm.year += val / 10000;
                        tm.mon += (val / 100) % 100;
                        tm.mday += val % 100;
                        adjust_fract_seconds(fval, tm, fsec, SECS_PER_DAY)?;
                        if unit == b'\0' {
                            return Ok(());
                        }
                        datepart = false;
                        havefield = false;
                        continue;
                    }

                    // Other cases should implement as b'-'.
                    let (is_finish, left) = decode_date_part_left(s, unit, tm,
                                                                  &mut datepart, &mut havefield,
                                                                  &mut val, &mut fval, fsec)?;
                    if !is_finish {
                        s = left;
                        continue;
                    } else {
                        return Ok(());
                    }
                }

                b'-' => {
                    let (is_finish, left) = decode_date_part_left(s, unit, tm,
                                                                  &mut datepart, &mut havefield,
                                                                  &mut val, &mut fval, fsec)?;
                    if !is_finish {
                        s = left;
                        continue;
                    } else {
                        return Ok(());
                    }
                }
                _ => {
                    // not a valid date unit suffix */
                    return Err(DateTimeError::invalid(format!("char: {:?} is not expect at iso format interval", unit)));
                }
            }
        } else {
            match unit        /* after T: H M S */
            {
                b'H' => {
                    tm.hour += val;
                    adjust_fract_seconds(fval, tm, fsec, SECS_PER_HOUR)?;
                }
                b'M' => {
                    tm.min += val;
                    adjust_fract_seconds(fval, tm, fsec, SECS_PER_MINUTE)?;
                }
                b'S' => {
                    tm.sec += val;
                    adjust_fract_seconds(fval, tm, fsec, 1)?;
                }
                b'\0' => { // ISO 8601 4.4.3.3 Alternative Format
                    let len = iso8601_integer_width(field_start);
                    if len == 6 && !havefield
                    {
                        tm.hour += val / 10000;
                        tm.min += (val / 100) % 100;
                        tm.sec += val % 100;
                        adjust_fract_seconds(fval, tm, fsec, 1)?;
                        return Ok(());
                    }
                    // Else fall through to extended alternative format.
                    // FALLTHROUGH.
                    return decode_time_part_left(
                        s,
                        unit,
                        tm,
                        havefield,
                        &mut val,
                        &mut fval,
                        fsec);
                }
                b':' => {// ISO 8601 4.4.3.3 Alternative Format, Extended
                    return decode_time_part_left(
                        s,
                        unit,
                        tm,
                        havefield,
                        &mut val,
                        &mut fval,
                        fsec);
                }
                _ => return Err(DateTimeError::invalid(format!("char: {:?} is not expect at iso format interval", unit))),
            }
        }

        havefield = true;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DateTimeError;
    use crate::parse::{
        check_mask_not_empty, check_mask_set, decode_time_part_left, eat_digits, eat_whitespaces,
        parse_date_time, str2i32, TokenField,
    };
    use std::mem::MaybeUninit;

    #[test]
    fn test_eat_whitespace() {
        let test_str = "  1 23  ".as_bytes();
        let test_str = eat_whitespaces(test_str);
        assert!(test_str == b"1 23  ");
        let test_str = eat_whitespaces(&test_str[1..]);
        assert!(test_str == b"23  ");
        let test_str = eat_whitespaces(&test_str[2..]);
        assert!(test_str.is_empty())
    }

    #[test]
    fn test_eat_digit() {
        let test_str = "1234".as_bytes();
        let (digit_str, test_str) = eat_digits(test_str);
        assert!(test_str.is_empty());
        assert!(digit_str == b"1234");
        let test_str = "1234abd".as_bytes();
        let (digit_str, test_str) = eat_digits(test_str);
        assert!(digit_str == b"1234");
        assert!(test_str == b"abd");
        let test_str = "abd123".as_bytes();
        let (digit_str, test_str) = eat_digits(test_str);
        assert!(digit_str.is_empty());
        assert!(test_str == b"abd123");
    }

    fn test_pare_date_time_common(time_str: &[u8], ex_fields: &[&[u8]], ex_f_types: &[TokenField]) {
        let mut worker_buf: [u8; 128] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut fields: [&[u8]; 18] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut f_types: [TokenField; 18] = unsafe { MaybeUninit::uninit().assume_init() };
        let ret = parse_date_time(time_str, &mut worker_buf, &mut fields, &mut f_types, 18);
        assert!(ret.is_ok());
        let nf = ret.unwrap();
        assert_eq!(nf as usize, ex_fields.len());
        for (i, field) in ex_fields.iter().enumerate() {
            assert_eq!(f_types[i], ex_f_types[i]);
            assert_eq!(*field, ex_fields[i]);
        }
    }

    #[test]
    fn test_pare_date_time_digit_begin() {
        test_pare_date_time_common(b"2000", &[b"2000"], &[TokenField::Number]);
        test_pare_date_time_common(b"14:20", &[b"14:20"], &[TokenField::Time]);
        test_pare_date_time_common(b"2000-12", &[b"2000-12"], &[TokenField::Date]);
        test_pare_date_time_common(b"2000/12", &[b"2000/12"], &[TokenField::Date]);
        test_pare_date_time_common(b"2000.12", &[b"2000.12"], &[TokenField::Number]);
        test_pare_date_time_common(b"2000-12-12", &[b"2000-12-12"], &[TokenField::Date]);
        test_pare_date_time_common(b"2000.12.12", &[b"2000.12.12"], &[TokenField::Date]);
        test_pare_date_time_common(b"2000.am12", &[b"2000.am12"], &[TokenField::Date]);
    }

    #[test]
    fn test_to_i32() -> Result<(), DateTimeError> {
        let s = "123".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_ok());
        let (v, _s) = ret.unwrap();
        assert_eq!(v, 123);

        let s = "+123".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_ok());
        let (v, _s) = ret.unwrap();
        assert_eq!(v, 123);

        let s = "-123".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_ok());
        let (v, _s) = ret.unwrap();
        assert_eq!(v, -123);

        let s = "abc".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_ok());
        let (v, _s) = ret.unwrap();
        assert_eq!(v, 0);

        let s = "100abc123".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_ok());
        let (v, _s) = ret.unwrap();
        assert_eq!(v, 100);

        let s = "2147483647".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_ok());
        let (v, _s) = ret.unwrap();
        assert_eq!(v, 2147483647);

        let s = "2147483648".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_err());
        let s = "2147483648000000".as_bytes();
        let ret = str2i32(s);
        assert!(ret.is_err());
        let s = "123".as_bytes();
        let b = &s[3..];
        assert_eq!(b.len(), 0);

        let (v, _) = str2i32("+".as_bytes())?;
        assert_eq!(v, 0);
        Ok(())
    }

    #[test]
    fn test_decode_time_part_left() -> Result<(), DateTimeError> {
        let mut tm = PgTime::new();
        let mut fval = 0.0;
        let mut fsec = 56;
        let mut val = 40;

        let ret = decode_time_part_left(
            b"10.23n", b'S', &mut tm, false, &mut val, &mut fval, &mut fsec,
        );
        assert!(ret.is_err());

        let ret = decode_time_part_left(
            b"10.23.45.0.45n",
            b'S',
            &mut tm,
            false,
            &mut val,
            &mut fval,
            &mut fsec,
        );
        assert!(ret.is_err());
        fval = 0.0;
        fsec = 0;
        val = 40;
        let mut tm = PgTime::new();
        let _ret = decode_time_part_left(
            b"10:45", b'S', &mut tm, false, &mut val, &mut fval, &mut fsec,
        );
        assert_eq!(tm.sec, 45);

        let ret = decode_time_part_left(
            b"-10:3333377777777777777778888845",
            b'S',
            &mut tm,
            false,
            &mut val,
            &mut fval,
            &mut fsec,
        );
        assert!(ret.is_err());

        Ok(())
    }

    #[test]
    fn test_check_mask_set() -> Result<(), DateTimeError> {
        let ret = check_mask_set(1i32 << 10, 1 << 10);
        assert!(ret.is_err());
        check_mask_set(1i32 << 12 | 1i32 << 11, 1 << 10)?;
        let ret = check_mask_not_empty(0);
        assert!(ret.is_err());
        check_mask_not_empty(1)?;
        Ok(())
    }

    #[test]
    fn test_binary_search() -> Result<(), DateTimeError> {
        let ret = search_delta_token(b"microseconds");
        assert!(ret.is_some());

        let ret = search_delta_token(b"microsecon");
        assert!(ret.is_some());

        let ret = search_delta_token(b"microsec");
        assert!(ret.is_none());

        let (t, v) = decode_units(1, b"microsecon");
        assert_eq!(t, TokenType::UNITS);
        assert_eq!(v, 30);

        let (t, v) = decode_units(1, b"microseconds");
        assert_eq!(t, TokenType::UNITS);
        assert_eq!(v, 30);
        Ok(())
    }

    #[test]
    fn test_to_double() -> Result<(), DateTimeError> {
        let (v, s) = str2d(".23456".as_bytes())?;
        assert!(s.len() == 0);
        assert_eq!(v, 0.23456);

        let (v, s) = str2d("+".as_bytes())?;
        assert!(s.len() == 0);
        assert_eq!(v, 0.0);

        let (v, s) = str2d("23.23456".as_bytes())?;
        assert!(s.len() == 0);
        assert_eq!(v, 23.23456);

        let (v, s) = str2d("+23.23456".as_bytes())?;
        assert!(s.len() == 0);
        assert_eq!(v, 23.23456);

        let (v, s) = str2d("-23.23456".as_bytes())?;
        assert!(s.len() == 0);
        assert_eq!(v, -23.23456);

        let (v, s) = str2d("-23.23456s3".as_bytes())?;
        assert!(s.len() == 2);
        assert_eq!(v, -23.23456);

        let (v, s) = str2d("23.23456s3".as_bytes())?;
        assert!(s.len() == 2);
        assert_eq!(v, 23.23456);

        let (v, _s) = str2d("".as_bytes())?;
        assert_eq!(v, 0.0);

        Ok(())
    }

    #[test]
    fn test_parse_frac_second() -> Result<(), DateTimeError> {
        let ret = parse_fractional_second(b".2345")?;
        assert_eq!(ret, 234500);
        let ret = parse_fractional_second(b".1234567")?;
        assert_eq!(ret, 123457);
        let ret = parse_fractional_second(b".1")?;
        assert_eq!(ret, 100000);
        let ret = parse_fractional_second(b".1b");
        assert!(ret.is_err());
        Ok(())
    }

    #[test]
    fn test_decode_number() -> Result<(), DateTimeError> {
        let mut tm = PgTime::new();
        let mut fsec = 0;
        let mut is2digits = false;
        let mut t_mask = 0;
        let date_order = DateOrder::YMD;
        let ret = decode_number(
            b"b222222",
            false,
            0,
            &mut t_mask,
            &mut tm,
            &mut fsec,
            &mut is2digits,
            date_order,
        );
        assert!(ret.is_err());

        let ret = decode_number(
            b"222f",
            false,
            0,
            &mut t_mask,
            &mut tm,
            &mut fsec,
            &mut is2digits,
            date_order,
        );
        assert!(ret.is_err());

        let ret = decode_number(
            b"22222222222222222222222222222222",
            false,
            0,
            &mut t_mask,
            &mut tm,
            &mut fsec,
            &mut is2digits,
            date_order,
        );
        assert!(ret.is_err());

        decode_number(
            b"19881216",
            false,
            0,
            &mut t_mask,
            &mut tm,
            &mut fsec,
            &mut is2digits,
            date_order,
        )?;
        assert_eq!(tm.year, 19881216);

        decode_number(
            b"123022.23",
            false,
            0,
            &mut t_mask,
            &mut tm,
            &mut fsec,
            &mut is2digits,
            date_order,
        )?;
        assert_eq!(tm.hour, 12);
        assert_eq!(tm.min, 30);
        assert_eq!(tm.sec, 22);
        assert_eq!(fsec, 230000);

        decode_number(
            b"12.23",
            false,
            0,
            &mut t_mask,
            &mut tm,
            &mut fsec,
            &mut is2digits,
            date_order,
        )?;
        assert_eq!(fsec, 230000);

        Ok(())
    }

    #[test]
    fn test_decode_time() -> Result<(), DateTimeError> {
        let mut t_mask = 0;
        let mut tm = PgTime::new();
        let mut f_sec = 0;
        let ret = decode_time(b"12", 0, 0, &mut t_mask, &mut tm, &mut f_sec);
        assert!(ret.is_err());

        let ret = decode_time(b"12y", 0, 0, &mut t_mask, &mut tm, &mut f_sec);
        assert!(ret.is_err());

        let ret = decode_time(b"12:-12:-34.345", 0, 0, &mut t_mask, &mut tm, &mut f_sec);
        assert!(ret.is_err());

        let ret = decode_time(b"12:12y", 0, 0, &mut t_mask, &mut tm, &mut f_sec);
        assert!(ret.is_err());

        let ret = decode_time(b"12:12:34y", 0, 0, &mut t_mask, &mut tm, &mut f_sec);
        assert!(ret.is_err());

        decode_time(
            b"12:34",
            0,
            INTERVAL_MASK_MS,
            &mut t_mask,
            &mut tm,
            &mut f_sec,
        )?;
        assert_eq!(tm.hour, 0);
        assert_eq!(tm.min, 12);
        assert_eq!(tm.sec, 34);
        assert_eq!(f_sec, 0);

        decode_time(
            b"12:34",
            0,
            INTERVAL_MASK_MS,
            &mut t_mask,
            &mut tm,
            &mut f_sec,
        )?;
        assert_eq!(tm.hour, 0);
        assert_eq!(tm.min, 12);
        assert_eq!(tm.sec, 34);
        assert_eq!(f_sec, 0);

        decode_time(
            b"12:23.34",
            0,
            INTERVAL_MASK_MS,
            &mut t_mask,
            &mut tm,
            &mut f_sec,
        )?;
        assert_eq!(tm.hour, 0);
        assert_eq!(tm.min, 12);
        assert_eq!(tm.sec, 23);
        assert_eq!(f_sec, 340000);

        Ok(())
    }

    #[test]
    fn test_decode_date() -> Result<(), DateTimeError> {
        let fields: [&[u8]; 1] = [&("34".as_bytes()); 1];
        let f_types: [TokenField; 1] = [TokenField::Ago; 1];
        let mut tm = PgTime::new();
        let ret = decode_date_time(&fields, &f_types, &mut tm, &mut None, DateOrder::YMD);
        assert!(ret.is_err());
        Ok(())
    }
}
