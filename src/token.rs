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

//! Const type values.

use crate::common::SECS_PER_HOUR;
use crate::DateTimeError;
use std::convert::TryFrom;

pub const MAX_DATE_FIELDS: usize = 25;
pub const MAX_TOKEN_LEN: usize = 10;

const DAGO: &[u8] = b"ago";
const DCURRENT: &[u8] = b"current";
const EPOCH: &[u8] = b"epoch";
const INVALID: &[u8] = b"invalid";
pub const EARLY: &[u8] = b"-infinity";
pub const LATE: &[u8] = b"infinity";
const NOW: &[u8] = b"now";
const TODAY: &[u8] = b"today";
const TOMORROW: &[u8] = b"tomorrow";
const YESTERDAY: &[u8] = b"yesterday";
const ZULU: &[u8] = b"zulu";

const DMICROSEC: &[u8] = b"usecond";
const DMILLISEC: &[u8] = b"msecond";
const DSECOND: &[u8] = b"second";
const DMINUTE: &[u8] = b"minute";
const DHOUR: &[u8] = b"hour";
const DDAY: &[u8] = b"day";
const DWEEK: &[u8] = b"week";
const DMONTH: &[u8] = b"month";
const DQUARTER: &[u8] = b"quarter";
const DYEAR: &[u8] = b"year";
const DDECADE: &[u8] = b"decade";
const DCENTURY: &[u8] = b"century";
const DMILLENNIUM: &[u8] = b"millennium";
const DA_D: &[u8] = b"ad";
const DB_C: &[u8] = b"bc";
const DTIMEZONE: &[u8] = b"timezone";

/// Keep this struct small; it gets used a lot.
pub struct DateToken {
    pub token: &'static [u8],
    pub ty: TokenType,
    pub value: i32,
}

impl<'a> DateToken {
    pub const fn new(token: &'static [u8], ty: TokenType, value: i32) -> Self {
        Self { token, ty, value }
    }
}

// Fundamental time field definitions for parsing.
//
// Meridian:  am, pm, or 24-hour style.
// Millennium: ad, bc

pub const AM: i32 = 0;
pub const PM: i32 = 1;
pub const HR24: i32 = 2;

const AD: i32 = 0;
pub const BC: i32 = 1;

// Field types for time decoding
// Can't have more of these than there are bits in an unsigned int
// since these are turned into bit masks during parsing and decoding.
// Furthermore, the values for YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
// must be in the range 0..14 so that the associated bitmasks can fit
// into the left half of an INTERVAL's typmod value.  Since those bits
// are stored in typmods, you can't change them without initdb!

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq)]
#[repr(u8)]
pub enum TokenType {
    RESERV = 0,
    MONTH = 1,
    YEAR = 2,
    DAY = 3,
    #[allow(dead_code)]
    JULIAN = 4,
    /// fixed-offset timezone abbreviation.
    TZ = 5,
    /// fixed-offset timezone abbrev, DST.
    DTZ = 6,
    /// Dynamic timezone abbreviation.
    #[allow(dead_code)]
    DYNTZ = 7,
    IgnoreDtf = 8,
    AMPM = 9,
    HOUR = 10,
    MINUTE = 11,
    SECOND = 12,
    MILLISECOND = 13,
    MICROSECOND = 14,
    DOY = 15,
    DOW = 16,
    UNITS = 17,
    ADBC = 18,
    /// These are only for relative dates.
    AGO = 19,
    #[allow(dead_code)]
    AbsBefore = 20,
    #[allow(dead_code)]
    AbsAfter = 21,
    /// Generic fields to help with parsing.
    #[allow(dead_code)]
    ISODATE = 22,
    ISOTIME = 23,
    /// These are only for parsing intervals.
    WEEK = 24,
    DECADE = 25,
    CENTURY = 26,
    MILLENNIUM = 27,
    /// hack for parsing two-word timezone specs "MET DST" etc.
    DTZMOD = 28,
    /// "DST" as a separate word.
    /// reserved for unrecognized string values.
    UnknownField = 31,
}

impl TokenType {
    #[inline]
    pub const fn mask(self) -> i32 {
        0x01i32 << (self as i32)
    }
}

impl TryFrom<u8> for TokenType {
    type Error = DateTimeError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value >= 31 {
            Err(DateTimeError::invalid(format!(
                "Invalid token type {:?}",
                value
            )))
        } else {
            Ok(unsafe { std::mem::transmute(value) })
        }
    }
}

/// Convenience: a second, plus any fractional component.
pub const DTK_ALL_SECS_M: i32 =
    TokenType::SECOND.mask() | TokenType::MILLISECOND.mask() | TokenType::MICROSECOND.mask();
pub const DTK_YEAR_M: i32 = TokenType::YEAR.mask();
pub const DTK_MONTH_M: i32 = TokenType::MONTH.mask();
pub const DTK_DAY_M: i32 = TokenType::DAY.mask();
pub const DTK_YEAR_MONTH_M: i32 = TokenType::YEAR.mask() | TokenType::MONTH.mask();
pub const DTK_MONTH_DAY_M: i32 = TokenType::MONTH.mask() | TokenType::DAY.mask();
pub const DTK_DATE_M: i32 =
    TokenType::YEAR.mask() | TokenType::MONTH.mask() | TokenType::DAY.mask();
pub const DTK_TIME_M: i32 = TokenType::HOUR.mask() | TokenType::MINUTE.mask() | DTK_ALL_SECS_M;
#[allow(dead_code)]
pub const DTK_TIME_ZONE_M: i32 = TokenType::TZ.mask();

// Token field definitions for time parsing and decoding.
// Some field type codes (see above) use these as the "value" in datetktbl[].
// These are also used for bit masks in DecodeDateTime and friends
// so actually restrict them to within [0,31] for now.
// - thomas 97/06/19
// Not all of these fields are used for masks in DecodeDateTime
// so allow some larger than 31. - thomas 1997-11-17
// Caution: there are undocumented assumptions in the code that most of these
// values are not equal to IGNORE_DTF nor RESERV.  Be very careful when
// renumbering values in either of these apparently-independent lists :-(
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq)]
#[repr(i32)]
pub enum TokenField {
    Number = 0,
    String = 1,
    Date = 2,
    Time = 3,
    Tz = 4,
    #[allow(dead_code)]
    Ago = 5,
    Special = 6,
    Invalid = 7,
    Current = 8,
    Early = 9,
    Late = 10,
    Epoch = 11,
    Now = 12,
    Yesterday = 13,
    Today = 14,
    Tomorrow = 15,
    Zulu = 16,
    Delta = 17,
    Second = 18,
    Minute = 19,
    Hour = 20,
    Day = 21,
    Week = 22,
    Month = 23,
    Quarter = 24,
    Year = 25,
    Decade = 26,
    Century = 27,
    Millennium = 28,
    Millisec = 29,
    Microsec = 30,
    Julian = 31,

    Dow = 32,
    Doy = 33,
    TzHour = 34,
    TzMinute = 35,
    #[allow(dead_code)]
    Isoyear = 36,
    Isodow = 37,
}

impl TokenField {
    #[inline]
    pub const fn value(self) -> i32 {
        self as i32
    }
}

impl TryFrom<i32> for TokenField {
    type Error = DateTimeError;

    #[inline]
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 && value > 37 {
            Err(DateTimeError::invalid(format!(
                "Invalid token field {:?}",
                value
            )))
        } else {
            Ok(unsafe { std::mem::transmute(value) })
        }
    }
}

use TokenField::*;
use TokenType::*;

const DATE_TOKEN_TABLE: [DateToken; 280] = [
    // Text, token, lexval
    DateToken::new(EARLY, RESERV, Early.value()), // "-infinity" reserved for "early time"
    DateToken::new(b"acsst", DTZ, 37800),         // Cent. Australia
    DateToken::new(b"acst", DTZ, -14400),         // Atlantic/Porto Acre
    DateToken::new(b"act", TZ, -18000),           // Atlantic/Porto Acre
    DateToken::new(DA_D, ADBC, AD),               // "ad" for years >= 0
    DateToken::new(b"adt", DTZ, -10800),          // Atlantic Daylight Time
    DateToken::new(b"aesst", DTZ, 39600),         // E. Australia
    DateToken::new(b"aest", TZ, 36000),           // Australia Eastern Std Time
    DateToken::new(b"aft", TZ, 16200),            // Kabul
    DateToken::new(b"ahst", TZ, -36000),          // Alaska-Hawaii Std Time
    DateToken::new(b"akdt", DTZ, -28800),         // Alaska Daylight Time
    DateToken::new(b"akst", DTZ, -32400),         // Alaska Standard Time
    DateToken::new(b"allballs", RESERV, Zulu.value()), // 00:00:00
    DateToken::new(b"almst", TZ, 25200),          // Almaty Savings Time
    DateToken::new(b"almt", TZ, 21600),           // Almaty Time
    DateToken::new(b"am", AMPM, AM),
    DateToken::new(b"amst", DTZ, 18000), // Armenia Summer Time (Yerevan)
    DateToken::new(b"amt", TZ, 14400),   // Armenia Time (Yerevan)
    DateToken::new(b"anast", DTZ, 46800), // Anadyr Summer Time (Russia)
    DateToken::new(b"anat", TZ, 43200),  // Anadyr Time (Russia)
    DateToken::new(b"apr", MONTH, 4),
    DateToken::new(b"april", MONTH, 4),
    DateToken::new(b"art", TZ, -10800),  // Argentina Time
    DateToken::new(b"ast", TZ, -14400),  // Atlantic Std Time (Canada)
    DateToken::new(b"at", IgnoreDtf, 0), // "at" (throwaway)
    DateToken::new(b"aug", MONTH, 8),
    DateToken::new(b"august", MONTH, 8),
    DateToken::new(b"awsst", DTZ, 32400), // W. Australia
    DateToken::new(b"awst", TZ, 28800),   // W. Australia
    DateToken::new(b"awt", DTZ, -10800),
    DateToken::new(b"azost", DTZ, 0),     // Azores Summer Time
    DateToken::new(b"azot", TZ, -3600),   // Azores Time
    DateToken::new(b"azst", DTZ, 18000),  // Azerbaijan Summer Time
    DateToken::new(b"azt", TZ, 14400),    // Azerbaijan Time
    DateToken::new(DB_C, ADBC, BC),       // "bc" for years < 0
    DateToken::new(b"bdst", TZ, 7200),    // British Double Summer Time
    DateToken::new(b"bdt", TZ, 21600),    // Dacca
    DateToken::new(b"bnt", TZ, 28800),    // Brunei Darussalam Time
    DateToken::new(b"bort", TZ, 28800),   // Borneo Time (Indonesia)
    DateToken::new(b"bot", TZ, -14400),   // Bolivia Time
    DateToken::new(b"bra", TZ, -10800),   // Brazil Time
    DateToken::new(b"bst", DTZ, 3600),    // British Summer Time
    DateToken::new(b"bt", TZ, 10800),     // Baghdad Time
    DateToken::new(b"btt", TZ, 21600),    // Bhutan Time
    DateToken::new(b"cadt", DTZ, 37800),  // Central Australian DST
    DateToken::new(b"cast", TZ, 34200),   // Central Australian ST
    DateToken::new(b"cat", TZ, -36000),   // Central Alaska Time
    DateToken::new(b"cct", TZ, 28800),    // China Coast Time
    DateToken::new(b"cdt", DTZ, -18000),  // Central Daylight Time
    DateToken::new(b"cest", DTZ, 7200),   // Central European Dayl.Time
    DateToken::new(b"cet", TZ, 3600),     // Central European Time
    DateToken::new(b"cetdst", DTZ, 7200), // Central European Dayl.Time
    DateToken::new(b"chadt", DTZ, 49500), // Chatham Island Daylight Time (13:45)
    DateToken::new(b"chast", TZ, 45900),  // Chatham Island Time (12:45)
    DateToken::new(b"ckt", TZ, 43200),    // Cook Islands Time
    DateToken::new(b"clst", DTZ, -10800), // Chile Summer Time
    DateToken::new(b"clt", TZ, -14400),   // Chile Time
    DateToken::new(b"cot", TZ, -18000),   // Columbia Time
    DateToken::new(b"cst", TZ, -21600),   // Central Standard Time
    DateToken::new(DCURRENT, RESERV, Current.value()), // "current" is always now
    DateToken::new(b"cvt", TZ, 25200),    // Christmas Island Time (Indian Ocean)
    DateToken::new(b"cxt", TZ, 25200),    // Christmas Island Time (Indian Ocean)
    DateToken::new(b"d", UNITS, Day.value()), // "day of month" for ISO input
    DateToken::new(b"davt", TZ, 25200),   // Davis Time (Antarctica)
    DateToken::new(b"ddut", TZ, 36000),   // Dumont-d'Urville Time (Antarctica)
    DateToken::new(b"dec", MONTH, 12),
    DateToken::new(b"december", MONTH, 12),
    DateToken::new(b"dnt", TZ, 3600),           // Dansk Normal Tid
    DateToken::new(b"dow", UNITS, Dow.value()), // day of week
    DateToken::new(b"doy", UNITS, Doy.value()), // day of year
    DateToken::new(b"dst", DTZMOD, SECS_PER_HOUR),
    DateToken::new(b"easst", DTZ, -18000), // Easter Island Summer Time
    DateToken::new(b"east", TZ, -21600),   // Easter Island Time
    DateToken::new(b"eat", TZ, 10800),     // East Africa Time
    DateToken::new(b"edt", DTZ, -14400),   // Eastern Daylight Time
    DateToken::new(b"eest", DTZ, 10800),   // Eastern Europe Summer Time
    DateToken::new(b"eet", TZ, 7200),      // East. Europe, USSR Zone 1
    DateToken::new(b"eetdst", DTZ, 10800), // Eastern Europe Daylight Time
    DateToken::new(b"egst", DTZ, 0),       // East Greenland Summer Time
    DateToken::new(b"egt", TZ, -3600),     // East Greenland Time
    DateToken::new(EPOCH, RESERV, Epoch.value()), // "epoch" reserved for system epoch time
    DateToken::new(b"est", TZ, -18000),    // Eastern Standard Time
    DateToken::new(b"fe", MONTH, 2),
    DateToken::new(b"february", MONTH, 2),
    DateToken::new(b"fjst", DTZ, -46800), // Fiji Summer Time (13 hour offset!)
    DateToken::new(b"fjt", TZ, -43200),   // Fiji Time
    DateToken::new(b"fkst", DTZ, -10800), // Falkland Islands Summer Time
    DateToken::new(b"fkt", TZ, -7200),    // Falkland Islands Time
    DateToken::new(b"fri", DOW, 5),
    DateToken::new(b"friday", DOW, 5),
    DateToken::new(b"fst", TZ, 3600),           // French Summer Time
    DateToken::new(b"fwt", DTZ, 7200),          // French Winter Time
    DateToken::new(b"galt", TZ, -21600),        // Galapagos Time
    DateToken::new(b"gamt", TZ, -32400),        // Gambier Time
    DateToken::new(b"gest", DTZ, 18000),        // Georgia Summer Time
    DateToken::new(b"get", TZ, 14400),          // Georgia Time
    DateToken::new(b"gft", TZ, -10800),         // French Guiana Time
    DateToken::new(b"gilt", TZ, 43200),         // Gilbert Islands Time
    DateToken::new(b"gmt", TZ, 0),              // Greenwish Mean Time
    DateToken::new(b"gst", TZ, 36000),          // Guam Std Time, USSR Zone 9
    DateToken::new(b"gyt", TZ, -14400),         // Guyana Time
    DateToken::new(b"h", UNITS, Hour.value()),  // "hour"
    DateToken::new(b"hdt", DTZ, -32400),        // Hawaii/Alaska Daylight Time
    DateToken::new(b"hkt", TZ, 28800),          // Hong Kong Time
    DateToken::new(b"hst", TZ, -36000),         // Hawaii Std Time
    DateToken::new(b"ict", TZ, 25200),          // Indochina Time
    DateToken::new(b"idle", TZ, 43200),         // Intl. Date Line, East
    DateToken::new(b"idlw", TZ, -43200),        // Intl. Date Line, West
    DateToken::new(LATE, RESERV, Late.value()), // "infinity" reserved for "late time"
    DateToken::new(INVALID, RESERV, Invalid.value()), // "invalid" reserved for bad time
    DateToken::new(b"iot", TZ, 18000),          // Indian Chagos Time
    DateToken::new(b"irkst", DTZ, 32400),       // Irkutsk Summer Time
    DateToken::new(b"irkt", TZ, 28800),         // Irkutsk Time
    DateToken::new(b"irt", TZ, 12600),          // Iran Time
    DateToken::new(b"isodow", UNITS, Isodow.value()), // ISO day of week, Sunday == 7
    DateToken::new(b"ist", TZ, 7200),           // Israel
    DateToken::new(b"it", TZ, 12600),           // Iran Time
    DateToken::new(b"j", UNITS, Julian.value()),
    DateToken::new(b"jan", MONTH, 1),
    DateToken::new(b"january", MONTH, 1),
    DateToken::new(b"javt", TZ, 25200), // Java Time (07:00? see JT)
    DateToken::new(b"jayt", TZ, 32400), // Jayapura Time (Indonesia)
    DateToken::new(b"jd", UNITS, Julian.value()),
    DateToken::new(b"jst", TZ, 32400), // Japan Std Time,USSR Zone 8
    DateToken::new(b"jt", TZ, 27000),  // Java Time (07:30? see JAVT)
    DateToken::new(b"jul", MONTH, 7),
    DateToken::new(b"julian", UNITS, Julian as i32),
    DateToken::new(b"july", MONTH, 7),
    DateToken::new(b"jun", MONTH, 6),
    DateToken::new(b"june", MONTH, 6),
    DateToken::new(b"kdt", DTZ, 36000),   // Korea Daylight Time
    DateToken::new(b"kgst", DTZ, 21600),  // Kyrgyzstan Summer Time
    DateToken::new(b"kgt", TZ, 18000),    // Kyrgyzstan Time
    DateToken::new(b"kost", TZ, 43200),   // Kosrae Time
    DateToken::new(b"krast", DTZ, 25200), // Krasnoyarsk Summer Time
    DateToken::new(b"krat", TZ, 28800),   // Krasnoyarsk Standard Time
    DateToken::new(b"kst", TZ, 32400),    // Korea Standard Time
    DateToken::new(b"lhdt", DTZ, 39600),  // Lord Howe Daylight Time, Australia
    DateToken::new(b"lhst", TZ, 37800),   // Lord Howe Standard Time, Australia
    DateToken::new(b"ligt", TZ, 36000),   // From Melbourne, Australia
    DateToken::new(b"lint", TZ, 50400),   // Line Islands Time (Kiribati; +14 hours!)
    DateToken::new(b"lkt", TZ, 21600),    // Lanka Time
    DateToken::new(b"m", UNITS, Month.value()), // "month" for ISO input
    DateToken::new(b"magst", DTZ, 43200), // Magadan Summer Time
    DateToken::new(b"magt", TZ, 39600),   // Magadan Time
    DateToken::new(b"mar", MONTH, 3),
    DateToken::new(b"march", MONTH, 3),
    DateToken::new(b"mart", TZ, -34200), // Marquesas Time
    DateToken::new(b"mawt", TZ, 21600),  // Mawson, Antarctica
    DateToken::new(b"may", MONTH, 5),
    DateToken::new(b"mdt", DTZ, -21600), // Mountain Daylight Time
    DateToken::new(b"mest", DTZ, 7200),  // Middle Europe Summer Time
    DateToken::new(b"met", TZ, 3600),    // Middle Europe Time
    DateToken::new(b"metdst", DTZ, 7200), // Middle Europe Daylight Time
    DateToken::new(b"mewt", TZ, 3600),   // Middle Europe Winter Time
    DateToken::new(b"mez", TZ, 3600),    // Middle Europe Zone
    DateToken::new(b"mht", TZ, 43200),   // Kwajalein
    DateToken::new(b"mm", UNITS, Minute.value()), // "minute" for ISO input
    DateToken::new(b"mmt", TZ, 23400),   // Myannar Time
    DateToken::new(b"mon", DOW, 1),
    DateToken::new(b"monday", DOW, 1),
    DateToken::new(b"mpt", TZ, 36000), // North Mariana Islands Time
    DateToken::new(b"msd", DTZ, 14400), // Moscow Summer Time
    DateToken::new(b"msk", TZ, 10800), // Moscow Time
    DateToken::new(b"mst", TZ, -25200), // Mountain Standard Time
    DateToken::new(b"mt", TZ, 30600),  // Moluccas Time
    DateToken::new(b"mut", TZ, 14400), // Mauritius Island Time
    DateToken::new(b"mvt", TZ, 18000), // Maldives Island Time
    DateToken::new(b"myt", TZ, 28800), // Malaysia Time
    DateToken::new(b"nct", TZ, 39600), // New Caledonia Time
    DateToken::new(b"ndt", DTZ, -9000), // Nfld. Daylight Time
    DateToken::new(b"nft", TZ, -12600), // Newfoundland Standard Time
    DateToken::new(b"nor", TZ, 3600),  // Norway Standard Time
    DateToken::new(b"nov", MONTH, 11),
    DateToken::new(b"november", MONTH, 11),
    DateToken::new(b"novst", DTZ, 25200), // Novosibirsk Summer Time
    DateToken::new(b"novt", TZ, 21600),   // Novosibirsk Standard Time
    DateToken::new(NOW, RESERV, Now.value()), // current transaction time
    DateToken::new(b"npt", TZ, 20700),    // Nepal Standard Time (GMT-5:45)
    DateToken::new(b"nst", TZ, -12600),   // Nfld. Standard Time
    DateToken::new(b"nt", TZ, -39600),    // Nome Time
    DateToken::new(b"nut", TZ, -39600),   // Niue Time
    DateToken::new(b"nzdt", DTZ, 46800),  // New Zealand Daylight Time
    DateToken::new(b"nzst", TZ, 43200),   // New Zealand Standard Time
    DateToken::new(b"nzt", TZ, 43200),    // New Zealand Time
    DateToken::new(b"oct", MONTH, 10),
    DateToken::new(b"october", MONTH, 10),
    DateToken::new(b"omsst", DTZ, 25200), // Omsk Summer Time
    DateToken::new(b"omst", TZ, 21600),   // Omsk Time
    DateToken::new(b"on", IgnoreDtf, 0),  // "on" (throwaway)
    DateToken::new(b"pdt", DTZ, -25200),  // Pacific Daylight Time
    DateToken::new(b"pet", TZ, -18000),   // Peru Time
    DateToken::new(b"petst", DTZ, 46800), // Petropavlovsk-Kamchatski Summer Time
    DateToken::new(b"pett", TZ, 43200),   // Petropavlovsk-Kamchatski Time
    DateToken::new(b"pgt", TZ, 36000),    // Papua New Guinea Time
    DateToken::new(b"phot", TZ, 46800),   // Phoenix Islands (Kiribati) Time
    DateToken::new(b"pht", TZ, 28800),    // Philippine Time
    DateToken::new(b"pkt", TZ, 18000),    // Pakistan Time
    DateToken::new(b"pm", AMPM, PM),
    DateToken::new(b"pmdt", DTZ, -7200), // Pierre & Miquelon Daylight Time
    DateToken::new(b"pont", TZ, 39600),  // Ponape Time (Micronesia)
    DateToken::new(b"pst", TZ, -28800),  // Pacific Standard Time
    DateToken::new(b"pwt", TZ, 32400),   // Palau Time
    DateToken::new(b"pyst", DTZ, -10800), // Paraguay Summer Time
    DateToken::new(b"pyt", TZ, -14400),  // Paraguay Time
    DateToken::new(b"ret", DTZ, 14400),  // Reunion Island Time
    DateToken::new(b"s", UNITS, Second.value()), // "seconds" for ISO input
    DateToken::new(b"sadt", DTZ, 37800), // S. Australian Dayl. Time
    DateToken::new(b"sast", TZ, 34200),  // South Australian Std Time
    DateToken::new(b"sat", DOW, 6),
    DateToken::new(b"saturday", DOW, 6),
    DateToken::new(b"sct", DTZ, 14400), // Mahe Island Time
    DateToken::new(b"sep", MONTH, 9),
    DateToken::new(b"sept", MONTH, 9),
    DateToken::new(b"september", MONTH, 9),
    DateToken::new(b"set", TZ, -3600), // Seychelles Time ??
    DateToken::new(b"sst", DTZ, 7200), // Swedish Summer Time
    DateToken::new(b"sun", DOW, 0),
    DateToken::new(b"sunday", DOW, 0),
    DateToken::new(b"swt", TZ, 3600), // Swedish Winter Time
    DateToken::new(b"t", ISOTIME, Time.value()), // Filler for ISO time fields
    DateToken::new(b"tft", TZ, 18000), // Kerguelen Time
    DateToken::new(b"that", TZ, -36000), // Tahiti Time
    DateToken::new(b"thu", DOW, 4),
    DateToken::new(b"thur", DOW, 4),
    DateToken::new(b"thurs", DOW, 4),
    DateToken::new(b"thursday", DOW, 4),
    DateToken::new(b"tjt", TZ, 18000),            // Tajikistan Time
    DateToken::new(b"tkt", TZ, -36000),           // Tokelau Time
    DateToken::new(b"tmt", TZ, 18000),            // Turkmenistan Time
    DateToken::new(TODAY, RESERV, Today.value()), // midnight
    DateToken::new(TOMORROW, RESERV, Tomorrow.value()), // tomorrow midnight
    DateToken::new(b"tot", TZ, 46800),            // Tonga Time
    DateToken::new(b"truk", TZ, 36000),           // Truk Time
    DateToken::new(b"tue", DOW, 2),
    DateToken::new(b"tues", DOW, 2),
    DateToken::new(b"tuesday", DOW, 2),
    DateToken::new(b"tvt", TZ, 43200),    // Tuvalu Time
    DateToken::new(b"ulast", DTZ, 32400), // Ulan Bator Summer Time
    DateToken::new(b"ulat", TZ, 28800),   // Ulan Bator Time
    DateToken::new(b"undefined", RESERV, Invalid as i32), // pre-v6.1 invalid time
    DateToken::new(b"ut", TZ, 0),
    DateToken::new(b"utc", TZ, 0),
    DateToken::new(b"uyst", DTZ, -7200),  // Uruguay Summer Time
    DateToken::new(b"uyt", TZ, -10800),   // Uruguay Time
    DateToken::new(b"uzst", DTZ, 21600),  // Uzbekistan Summer Time
    DateToken::new(b"uzt", TZ, 18000),    // Uzbekistan Time
    DateToken::new(b"vet", TZ, -14400),   // Venezuela Time
    DateToken::new(b"vlast", DTZ, 39600), // Vladivostok Summer Time
    DateToken::new(b"vlat", TZ, 36000),   // Vladivostok Time
    DateToken::new(b"vut", TZ, 39600),    // Vanuata Time
    DateToken::new(b"wadt", DTZ, 28800),  // West Australian DST
    DateToken::new(b"wakt", TZ, 43200),   // Wake Time
    DateToken::new(b"wast", TZ, 25200),   // West Australian Std Time
    DateToken::new(b"wat", TZ, -3600),    // West Africa Time
    DateToken::new(b"wdt", DTZ, 32400),   // West Australian DST
    DateToken::new(b"wed", DOW, 3),
    DateToken::new(b"wednesday", DOW, 3),
    DateToken::new(b"weds", DOW, 3),
    DateToken::new(b"west", DTZ, 3600), // Western Europe Summer Time
    DateToken::new(b"wet", TZ, 0),      // Western Europe
    DateToken::new(b"wetdst", DTZ, 3600), // Western Europe Daylight Savings Time
    DateToken::new(b"wft", TZ, 43200),  // Wallis and Futuna Time
    DateToken::new(b"wgst", DTZ, -7200), // West Greenland Summer Time
    DateToken::new(b"wgt", TZ, -10800), // West Greenland Time
    DateToken::new(b"wst", TZ, 28800),  // West Australian Standard Time
    DateToken::new(b"y", UNITS, Year.value()), // "year" for ISO input
    DateToken::new(b"yakst", DTZ, 36000), // Yakutsk Summer Time
    DateToken::new(b"yakt", TZ, 32400), // Yakutsk Time
    DateToken::new(b"yapt", TZ, 36000), // Yap Time (Micronesia)
    DateToken::new(b"ydt", DTZ, -28800), // Yukon Daylight Time
    DateToken::new(b"yekst", DTZ, 21600), // Yekaterinburg Summer Time
    DateToken::new(b"yekt", TZ, 18000), // Yekaterinburg Time
    DateToken::new(YESTERDAY, RESERV, Yesterday.value()), // yesterday midnight
    DateToken::new(b"yst", TZ, -32400), // Yukon Standard Time
    DateToken::new(b"z", TZ, 0),        // time zone tag per ISO-8601
    DateToken::new(b"zp4", TZ, -14400), // UTC +4  hours.
    DateToken::new(b"zp5", TZ, -18000), // UTC +5  hours.
    DateToken::new(b"zp6", TZ, -21600), // UTC +6  hours.
    DateToken::new(ZULU, TZ, 0),        // UTC
];

const DELTA_TOKEN_TABLE: [DateToken; 63] = [
    // text, token, lexval
    DateToken::new(b"@", IgnoreDtf, 0), // postgres relative prefix
    DateToken::new(DAGO, AGO, 0),       // "ago" indicates negative time offset
    DateToken::new(b"c", UNITS, Century.value()), // "century" relative
    DateToken::new(b"cent", UNITS, Century.value()), // "century" relative
    DateToken::new(b"centuries", UNITS, Century.value()), // "centuries" relative
    DateToken::new(DCENTURY, UNITS, Century.value()), // "century" relative
    DateToken::new(b"d", UNITS, Day.value()), // "day" relative
    DateToken::new(DDAY, UNITS, Day.value()), // "day" relative
    DateToken::new(b"days", UNITS, Day.value()), // "days" relative
    DateToken::new(b"dec", UNITS, Decade.value()), // "decade" relative
    DateToken::new(DDECADE, UNITS, Decade.value()), // "decade" relative
    DateToken::new(b"decades", UNITS, Decade.value()), // "decades" relative
    DateToken::new(b"decs", UNITS, Decade.value()), // "decades" relative
    DateToken::new(b"h", UNITS, Hour.value()), // "hour" relative
    DateToken::new(DHOUR, UNITS, Hour.value()), // "hour" relative
    DateToken::new(b"hours", UNITS, Hour.value()), // "hours" relative
    DateToken::new(b"hr", UNITS, Hour.value()), // "hour" relative
    DateToken::new(b"hrs", UNITS, Hour.value()), // "hours" relative
    DateToken::new(INVALID, RESERV, Invalid.value()), // reserved for invalid time
    DateToken::new(b"m", UNITS, Minute.value()), // "minute" relative
    DateToken::new(b"microsecon", UNITS, Microsec.value()), // "microsecond" relative
    DateToken::new(b"mil", UNITS, Millennium.value()), // "millennium" relative
    DateToken::new(b"millennia", UNITS, Millennium.value()), // "millennia" relative
    DateToken::new(DMILLENNIUM, UNITS, Millennium.value()), // "millennium" relative
    DateToken::new(b"millisecon", UNITS, Millisec.value()), // relative
    DateToken::new(b"mils", UNITS, Millennium.value()), // "millennia" relative
    DateToken::new(b"min", UNITS, Minute.value()), // "minute" relative
    DateToken::new(b"mins", UNITS, Minute.value()), // "minutes" relative
    DateToken::new(DMINUTE, UNITS, Minute.value()), // "minute" relative
    DateToken::new(b"minutes", UNITS, Minute.value()), // "minutes" relative
    DateToken::new(b"mon", UNITS, Month.value()), // "months" relative
    DateToken::new(b"mons", UNITS, Month.value()), // "months" relative
    DateToken::new(DMONTH, UNITS, Month.value()), // "month" relative
    DateToken::new(b"months", UNITS, Month.value()),
    DateToken::new(b"ms", UNITS, Millisec.value()),
    DateToken::new(b"msec", UNITS, Millisec.value()),
    DateToken::new(DMILLISEC, UNITS, Millisec.value()),
    DateToken::new(b"mseconds", UNITS, Millisec.value()),
    DateToken::new(b"msecs", UNITS, Millisec.value()),
    DateToken::new(b"qtr", UNITS, Quarter.value()), // "quarter" relative
    DateToken::new(DQUARTER, UNITS, Quarter.value()), // "quarter" relative
    DateToken::new(b"s", UNITS, Second.value()),
    DateToken::new(b"sec", UNITS, Second.value()),
    DateToken::new(DSECOND, UNITS, Second.value()),
    DateToken::new(b"seconds", UNITS, Second.value()),
    DateToken::new(b"secs", UNITS, Second.value()),
    DateToken::new(DTIMEZONE, UNITS, Tz.value()), // "timezone" time offset
    DateToken::new(b"timezone_h", UNITS, TzHour.value()), // timezone hour units
    DateToken::new(b"timezone_m", UNITS, TzMinute.value()), // timezone minutes units
    DateToken::new(b"undefined", RESERV, Invalid.value()), // pre-v6.1 invalid time
    DateToken::new(b"us", UNITS, Microsec.value()), // "microsecond" relative
    DateToken::new(b"usec", UNITS, Microsec.value()), // "microsecond" relative
    DateToken::new(DMICROSEC, UNITS, Microsec.value()), // "microsecond" relative
    DateToken::new(b"useconds", UNITS, Microsec.value()), // "microseconds" relative
    DateToken::new(b"usecs", UNITS, Microsec.value()), // "microseconds" relative
    DateToken::new(b"w", UNITS, Week.value()),    // "week" relative
    DateToken::new(DWEEK, UNITS, Week.value()),   // "week" relative
    DateToken::new(b"weeks", UNITS, Week.value()), // "weeks" relative
    DateToken::new(b"y", UNITS, Year.value()),    // "year" relative
    DateToken::new(DYEAR, UNITS, Year.value()),   // "year" relative
    DateToken::new(b"years", UNITS, Year.value()), // "years" relative
    DateToken::new(b"yr", UNITS, Year.value()),   // "year" relative
    DateToken::new(b"yrs", UNITS, Year.value()),  // "years" relative
];

#[inline]
fn date_token_binary_search<'a, 'b>(key: &'a [u8], base: &'b [DateToken]) -> Option<&'b DateToken> {
    // truncate key to keep max token len.
    let new_key = if key.len() > MAX_TOKEN_LEN {
        &key[0..MAX_TOKEN_LEN]
    } else {
        key
    };
    let ret = base.binary_search_by_key(&new_key, |date_token| date_token.token);
    match ret {
        Ok(token) => Some(&base[token]),
        Err(_) => None,
    }
}

#[inline]
pub fn search_date_token(key: &[u8]) -> Option<&'static DateToken> {
    date_token_binary_search(key, &DATE_TOKEN_TABLE)
}

#[inline]
pub fn search_delta_token<'a>(key: &'a [u8]) -> Option<&'static DateToken> {
    date_token_binary_search(key, &DELTA_TOKEN_TABLE)
}
