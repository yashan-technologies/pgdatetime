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

//! Date and time error handling.

use std::error::Error;
use std::fmt;

/// Error code of data time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateTimeErrorKind {
    Empty(String),
    Invalid(String),
    Overflow,
}

/// An error that can be returned when uses date/time types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateTimeError {
    kind: DateTimeErrorKind,
}

impl fmt::Display for DateTimeError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self.kind {
            DateTimeErrorKind::Empty(ref s) => {
                write!(f, "cannot parse date from empty string :{:?}", s)
            }
            DateTimeErrorKind::Invalid(ref s) => write!(f, "invalid :{:?}", s),
            DateTimeErrorKind::Overflow => write!(f, "value overflows date format"),
        }
    }
}

impl Error for DateTimeError {}

impl DateTimeError {
    #[inline]
    pub(crate) const fn new(kind: DateTimeErrorKind) -> Self {
        DateTimeError { kind }
    }

    #[inline]
    pub(crate) const fn empty(s: String) -> Self {
        Self::new(DateTimeErrorKind::Empty(s))
    }

    #[inline]
    pub(crate) fn is_invalid(&self) -> bool {
        match self.kind {
            DateTimeErrorKind::Invalid(ref _s) => true,
            _ => false,
        }
    }

    #[inline]
    pub(crate) const fn invalid(s: String) -> Self {
        Self::new(DateTimeErrorKind::Invalid(s))
    }

    #[inline]
    pub(crate) const fn overflow() -> Self {
        Self::new(DateTimeErrorKind::Overflow)
    }
}

#[cfg(test)]
mod tests {
    use crate::DateTimeError;

    #[test]
    fn test_error_string() -> Result<(), DateTimeError> {
        let invalid = DateTimeError::invalid("test valid".to_string());
        let empty = DateTimeError::empty("date is empty".to_string());
        let overflow = DateTimeError::overflow();

        let s = format!("{}", invalid);
        assert_eq!(s, "invalid :\"test valid\"");

        let s = format!("{}", empty);
        assert_eq!(s, "cannot parse date from empty string :\"date is empty\"");

        let s = format!("{}", overflow);
        assert_eq!(s, "value overflows date format");
        Ok(())
    }
}
