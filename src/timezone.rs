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

//! SQL timezone type written in Rust, compatible with PostgreSQL's date type.

use crate::token::DateToken;

const TZ_MAX_TIMES: usize = 2000;
const TZ_MAX_TYPES: usize = 256;
#[allow(dead_code)]
const TZ_MAX_CHARS: usize = 50;
const TZ_STRLEN_MAX: usize = 255;
const TZ_MAX_LEAPS: usize = 50;

#[derive(Copy, Clone)]
#[allow(dead_code)]
struct TimeInfo {
    // time type information.
    tt_utoff: i32,    // UT offset in seconds
    tt_isdst: bool,   // used to set tm_isdst
    tt_desigidx: i32, // abbreviation list index
    tt_ttisstd: bool, // transition is std time
    tt_ttisut: bool,  // transition is UT
}

impl TimeInfo {
    #[inline]
    const fn new() -> Self {
        Self {
            tt_utoff: 0,
            tt_isdst: false,
            tt_desigidx: 0,
            tt_ttisstd: false,
            tt_ttisut: false,
        }
    }
}

#[derive(Copy, Clone)]
#[allow(dead_code)]
struct LeapInfo {
    // leap second information.
    ls_trans: i64, // transition time.
    ls_corr: i64,  // correction to apply.
}

impl LeapInfo {
    #[inline]
    const fn new() -> Self {
        Self {
            ls_trans: 0,
            ls_corr: 0,
        }
    }
}

#[allow(dead_code)]
pub struct State {
    leap_cnt: i32,
    time_cnt: i32,
    type_cnt: i32,
    char_cnt: i32,
    go_back: bool,
    o_ahead: bool,
    ats: [i64; TZ_MAX_TIMES],
    types: [u8; TZ_MAX_TIMES],
    ttis: [TimeInfo; TZ_MAX_TYPES],
    chars: [u8; 2 * (TZ_STRLEN_MAX + 1)],
    ls_info: [LeapInfo; TZ_MAX_LEAPS],
    // The time type to use for early times or if no transitions. It is always
    // zero for recent tzdb releases. It might be nonzero for data from tzdb
    // 2018e or earlier.
    default_type: i32,
}

impl State {
    #[inline]
    const fn new() -> Self {
        Self {
            leap_cnt: 0,
            time_cnt: 0,
            type_cnt: 0,
            char_cnt: 0,
            go_back: false,
            o_ahead: false,
            ats: [0; TZ_MAX_TIMES],
            types: [0; TZ_MAX_TIMES],
            ttis: [TimeInfo::new(); TZ_MAX_TYPES],
            chars: [0; 2 * (TZ_STRLEN_MAX + 1)],
            ls_info: [LeapInfo::new(); TZ_MAX_LEAPS],
            default_type: 0,
        }
    }
}

pub struct PgTimezone {
    //TZname contains the canonically-cased name of the timezone.
    pub name: [u8; TZ_STRLEN_MAX],
    pub state: State,
}

impl PgTimezone {
    #[inline]
    pub const fn new() -> Self {
        Self {
            name: [0; TZ_STRLEN_MAX],
            state: State::new(),
        }
    }
}

// one of its uses is in tables of time zone abbreviations.
#[allow(dead_code)]
pub struct TimeZoneAbbrevTable {
    pub tbl_size: usize,   // size in bytes of TimeZoneAbbrevTable.
    pub num_abb_revs: i32, // number of entries in abbrevs[] array.
    pub abbrevs: *mut DateToken, // VARIABLE LENGTH ARRAY.
                           // DynamicZoneAbbrev(s) may follow the abbrevs[] array.
}

#[cfg(test)]
mod tests {
    use crate::common::*;
    use crate::timezone::PgTimezone;
    use crate::token::*;

    #[test]
    fn test_date_token() {
        let token = DateToken::new("Reserve".as_bytes(), TokenType::MONTH, 2);
        assert_eq!(token.ty, TokenType::MONTH);
        let _timezone = PgTimezone::new();

        let (t, q) = time_modulo(30, 40);
        assert_eq!(t, 30);
        assert_eq!(q, 0);
    }
}
