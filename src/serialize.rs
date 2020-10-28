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

//! Impl the `serde::Serialize` and `serde::Deserialize` traits.

use crate::{Date, DateOrder, DateStyle, Interval, IntervalStyle, Time, Timestamp};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

impl Serialize for Date {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let str = self
                .format(DateStyle::ISO, DateOrder::YMD)
                .map_err(ser::Error::custom)?;
            serializer.serialize_str(&str)
        } else {
            serializer.serialize_i32(self.value())
        }
    }
}

impl<'de> Deserialize<'de> for Date {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DateVisitor;

        impl<'de> Visitor<'de> for DateVisitor {
            type Value = Date;

            #[inline]
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a Date")
            }

            #[inline]
            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Date::new(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Date::try_from_str(v, DateOrder::YMD).map_err(de::Error::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(DateVisitor)
        } else {
            deserializer.deserialize_i32(DateVisitor)
        }
    }
}

impl Serialize for Time {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let str = self.format(DateStyle::ISO).map_err(ser::Error::custom)?;
            serializer.serialize_str(&str)
        } else {
            serializer.serialize_i64(self.value())
        }
    }
}

impl<'de> Deserialize<'de> for Time {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TimeVisitor;

        impl<'de> Visitor<'de> for TimeVisitor {
            type Value = Time;

            #[inline]
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a Time")
            }

            #[inline]
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Time::from(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Time::try_from_str(v, -1, DateOrder::YMD).map_err(de::Error::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(TimeVisitor)
        } else {
            deserializer.deserialize_i64(TimeVisitor)
        }
    }
}

impl Serialize for Timestamp {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let str = self
                .format(DateStyle::ISO, DateOrder::YMD)
                .map_err(ser::Error::custom)?;
            serializer.serialize_str(&str)
        } else {
            serializer.serialize_i64(self.value())
        }
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TimestampVisitor;

        impl<'de> Visitor<'de> for TimestampVisitor {
            type Value = Timestamp;

            #[inline]
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a Timestamp")
            }

            #[inline]
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Timestamp::new(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Timestamp::try_from_str(v, -1, DateOrder::YMD).map_err(de::Error::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(TimestampVisitor)
        } else {
            deserializer.deserialize_i64(TimestampVisitor)
        }
    }
}

impl Serialize for Interval {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let str = self
                .format(IntervalStyle::SQLStandard)
                .map_err(ser::Error::custom)?;
            serializer.serialize_str(&str)
        } else {
            let mut interval = serializer.serialize_struct("Interval", 3)?;
            interval.serialize_field("time", &self.time())?;
            interval.serialize_field("day", &self.day())?;
            interval.serialize_field("month", &self.month())?;
            interval.end()
        }
    }
}

impl<'de> Deserialize<'de> for Interval {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["time", "day", "month"];

        enum Field {
            Time,
            Day,
            Month,
        }

        impl<'de> Deserialize<'de> for Field {
            #[inline]
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    #[inline]
                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`day` or `time` or `month`")
                    }

                    #[inline]
                    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        match v {
                            "time" => Ok(Field::Time),
                            "day" => Ok(Field::Day),
                            "month" => Ok(Field::Month),
                            _ => Err(de::Error::unknown_field(v, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct IntervalVisitor;

        impl<'de> Visitor<'de> for IntervalVisitor {
            type Value = Interval;

            #[inline]
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "struct Interval")
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Interval::try_from_str(v, -1, IntervalStyle::SQLStandard).map_err(de::Error::custom)
            }

            #[inline]
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let time = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let day = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let month = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(Interval::from_mdt(month, day, time))
            }

            #[inline]
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut time = None;
                let mut day = None;
                let mut month = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Time => match time {
                            None => time = Some(map.next_value()?),
                            Some(_) => return Err(de::Error::duplicate_field("time")),
                        },
                        Field::Day => match day {
                            None => day = Some(map.next_value()?),
                            Some(_) => return Err(de::Error::duplicate_field("day")),
                        },
                        Field::Month => match month {
                            None => month = Some(map.next_value()?),
                            Some(_) => return Err(de::Error::duplicate_field("month")),
                        },
                    }
                }
                let time = time.ok_or_else(|| de::Error::missing_field("time"))?;
                let day = day.ok_or_else(|| de::Error::missing_field("day"))?;
                let month = month.ok_or_else(|| de::Error::missing_field("month"))?;
                Ok(Interval::from_mdt(month, day, time))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(IntervalVisitor)
        } else {
            deserializer.deserialize_struct("Interval", FIELDS, IntervalVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Test {
            date: Date,
            time: Time,
            timestamp: Timestamp,
            interval: Interval,
        }

        let t = Test {
            date: Date::try_from_str("2020-7-6", DateOrder::YMD).unwrap(),
            time: Time::try_from_str("04:05:06.456", -1, DateOrder::YMD).unwrap(),
            timestamp: Timestamp::try_from_str("2020-07-06 10:28:30.456", -1, DateOrder::YMD)
                .unwrap(),
            interval: Interval::try_from_str(
                "+1987-8 +7 +6:40:50.02",
                -1,
                IntervalStyle::SQLStandard,
            )
            .unwrap(),
        };

        let json = serde_json::to_string(&t).unwrap();
        println!("json: {}", json);
        let json_decode: Test = serde_json::from_str(&json).unwrap();
        assert_eq!(json_decode, t);

        let bin = bincode::serialize(&t).unwrap();
        println!("bin: {:?}", bin);
        let bin_decode: Test = bincode::deserialize(&bin).unwrap();
        assert_eq!(bin_decode, t);
    }
}
