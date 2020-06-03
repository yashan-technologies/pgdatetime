// Copyright 2020 CoD Team

//! pgdatetime benchmark

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pgdatetime::{
    Date, DateOrder, DateStyle, DateTime, DateUnit, FieldType, Interval, IntervalStyle, Time,
    Timestamp,
};

fn parse_benchmark(c: &mut Criterion) {
    c.bench_function("parse_date_for_date", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str("1999-01-08", DateOrder::YMD));
        })
    });

    c.bench_function("parse_date_for_number", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str("19990108", DateOrder::YMD));
        })
    });

    c.bench_function("parse_time", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str("12:30:21", DateOrder::YMD));
        })
    });

    c.bench_function("parse_timestamp_for_date_time", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str(
                "1999-01-08 04:05:06.456789",
                DateOrder::YMD,
            ));
        })
    });

    c.bench_function("parse_timestamp_for_number_time", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str(
                "19990108 04:05:06.456789",
                DateOrder::YMD,
            ));
        })
    });

    c.bench_function("parse_timestamp_for_date_number", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str(
                "1999-01-08 040506.456789",
                DateOrder::YMD,
            ));
        })
    });

    c.bench_function("parse_timestamp_for_number_number", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_str("19990108 040506.456789", DateOrder::YMD));
        })
    });

    c.bench_function("parse_interval_for_iso", |b| {
        b.iter(|| {
            let _ret = black_box(Interval::try_from_str(
                "P19870708T06:40:50.02",
                -1,
                IntervalStyle::ISO8601,
            ));
        })
    });

    c.bench_function("parse_interval_for_postgres", |b| {
        b.iter(|| {
            let _ret = black_box(Interval::try_from_str(
                "1987 years 7 mons 8 days 06:40:50.02",
                -1,
                IntervalStyle::Postgres,
            ));
        })
    });

    c.bench_function("parse_interval_for_sql_standard", |b| {
        b.iter(|| {
            let _ret = black_box(Interval::try_from_str(
                "@ 1 millennium 9 century 8 decade 7 year  7 month  8 day 6 hour 40 minute 50 second 20 millisecond",
                -1,
                IntervalStyle::SQLStandard,
            ));
        })
    });

    c.bench_function("parse_interval_for_sql_postgres_verbose", |b| {
        b.iter(|| {
            let _ret = black_box(Interval::try_from_str(
                "@ 1987 years 8 mons 7 days 6 hours 40 mins 50.02 secs",
                -1,
                IntervalStyle::PostgresVerbose,
            ));
        })
    });
}

fn try_from_number_benchmark(c: &mut Criterion) {
    c.bench_function("date_try_from_ymd", |b| {
        b.iter(|| {
            let _ret = black_box(Date::try_from_ymd(2020, 12, 23));
        })
    });

    c.bench_function("time_try_from_hms", |b| {
        b.iter(|| {
            let _ret = black_box(Time::try_from_hms(12, 20, 30.567123));
        })
    });

    c.bench_function("timestamp_try_from_ymd_hms", |b| {
        b.iter(|| {
            let _ret = black_box(Timestamp::try_from_ymd_hms(2020, 12, 30, 12, 20, 45.567123));
        })
    });

    c.bench_function("interval_try_from_ymwd_hms", |b| {
        b.iter(|| {
            let _ret = black_box(Interval::try_from_ymwd_hms(
                2020, 12, 10, 30, 12, 20, 45.567123,
            ));
        })
    });
}

fn to_string_benchmark(c: &mut Criterion) {
    let date = Date::try_from_ymd(2020, 12, 23);
    assert!(date.is_ok());
    let date = date.unwrap();
    c.bench_function("date_to_iso_string", |b| {
        b.iter(|| {
            let _ret = black_box(date.format(DateStyle::ISO, DateOrder::YMD));
        })
    });

    c.bench_function("date_to_sql_string", |b| {
        b.iter(|| {
            let _ret = black_box(date.format(DateStyle::SQL, DateOrder::YMD));
        })
    });

    c.bench_function("date_to_postgres_string", |b| {
        b.iter(|| {
            let _ret = black_box(date.format(DateStyle::Postgres, DateOrder::YMD));
        })
    });

    c.bench_function("date_to_xsd_string", |b| {
        b.iter(|| {
            let _ret = black_box(date.format(DateStyle::XSD, DateOrder::YMD));
        })
    });

    c.bench_function("date_to_german_string", |b| {
        b.iter(|| {
            let _ret = black_box(date.format(DateStyle::German, DateOrder::YMD));
        })
    });

    let time = Time::try_from_hms(12, 20, 30.234516);
    assert!(time.is_ok());
    let time = time.unwrap();
    c.bench_function("date_to_iso_string", |b| {
        b.iter(|| {
            let _ret = black_box(time.format(DateStyle::ISO));
        })
    });

    c.bench_function("date_to_sql_string", |b| {
        b.iter(|| {
            let _ret = black_box(time.format(DateStyle::SQL));
        })
    });

    c.bench_function("date_to_postgres_string", |b| {
        b.iter(|| {
            let _ret = black_box(time.format(DateStyle::Postgres));
        })
    });

    c.bench_function("date_to_xsd_string", |b| {
        b.iter(|| {
            let _ret = black_box(time.format(DateStyle::XSD));
        })
    });

    c.bench_function("date_to_german_string", |b| {
        b.iter(|| {
            let _ret = black_box(time.format(DateStyle::German));
        })
    });

    let timestamp = Timestamp::try_from_ymd_hms(2010, 7, 14, 16, 10, 30.234517);
    assert!(timestamp.is_ok());
    let timestamp = timestamp.unwrap();
    c.bench_function("timestamp_to_iso_string", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.format(DateStyle::ISO, DateOrder::YMD));
        })
    });

    c.bench_function("timestamp_to_xsd_string", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.format(DateStyle::XSD, DateOrder::YMD));
        })
    });

    c.bench_function("timestamp_to_postgres_string", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.format(DateStyle::Postgres, DateOrder::YMD));
        })
    });

    c.bench_function("timestamp_to_sql_string", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.format(DateStyle::SQL, DateOrder::YMD));
        })
    });

    c.bench_function("timestamp_to_german_string", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.format(DateStyle::German, DateOrder::YMD));
        })
    });

    let interval = Interval::try_from_ymwd_hms(2010, 7, 1, 7, 16, 10, 30.345412);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    c.bench_function("interval_to_iso_string", |b| {
        b.iter(|| {
            let _ret = black_box(interval.format(IntervalStyle::ISO8601));
        })
    });

    c.bench_function("interval_to_postgres_string", |b| {
        b.iter(|| {
            let _ret = black_box(interval.format(IntervalStyle::Postgres));
        })
    });

    c.bench_function("interval_to_postgresverbose_string", |b| {
        b.iter(|| {
            let _ret = black_box(interval.format(IntervalStyle::PostgresVerbose));
        })
    });

    c.bench_function("interval_to_sqlstandard_string", |b| {
        b.iter(|| {
            let _ret = black_box(interval.format(IntervalStyle::SQLStandard));
        })
    });
}

fn add_benchmark(c: &mut Criterion) {
    let timestamp = Timestamp::try_from_ymd_hms(2011, 12, 09, 1, 15, 5.456);
    assert!(timestamp.is_ok());
    let timestamp = timestamp.unwrap();

    let time = Time::try_from_hms(1, 20, 30.23);
    assert!(time.is_ok());
    let time = time.unwrap();

    let date = Date::try_from_ymd(2020, 1, 20);
    assert!(date.is_ok());
    let date = date.unwrap();

    let interval = Interval::try_from_ymwd_hms(1, 1, 1, 2, 25, 15, 5.456);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    let interval2 = Interval::try_from_ymwd_hms(10, 10, 2, 4, 45, 55, 15.456);
    assert!(interval2.is_ok());
    let interval2 = interval2.unwrap();

    c.bench_function("timestamp_add_time", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.add_time(time));
        })
    });

    c.bench_function("date_add_int", |b| {
        b.iter(|| {
            let _ret = black_box(date.add_days(1024));
        })
    });

    c.bench_function("date_add_interval", |b| {
        b.iter(|| {
            let _ret = black_box(date.add_interval(interval));
        })
    });

    c.bench_function("date_add_time", |b| {
        b.iter(|| {
            let _ret = black_box(date.add_time(time));
        })
    });

    c.bench_function("interval_add_interval", |b| {
        b.iter(|| {
            let _ret = black_box(interval.add_interval(interval2));
        })
    });

    c.bench_function("timestamp_add_interval", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.add_interval(interval));
        })
    });

    c.bench_function("time_add_interval", |b| {
        b.iter(|| {
            let _ret = black_box(time.add_interval(interval));
        })
    });
}

fn negative_benchmark(c: &mut Criterion) {
    let span = Interval::try_from_ymwd_hms(12, 23, 6, 31, 25, 15, 5.456);
    assert!(span.is_ok());
    let span = span.unwrap();

    c.bench_function("interval_negative", |b| {
        b.iter(|| {
            let _ret = black_box(span.negate());
        })
    });
}

fn sub_benchmark(c: &mut Criterion) {
    let timestamp = Timestamp::try_from_ymd_hms(2011, 12, 09, 1, 15, 5.456);
    assert!(timestamp.is_ok());
    let timestamp = timestamp.unwrap();

    let timestamp2 = Timestamp::try_from_ymd_hms(2001, 2, 21, 14, 45, 5.456);
    assert!(timestamp2.is_ok());
    let timestamp2 = timestamp2.unwrap();

    let time = Time::try_from_hms(1, 20, 30.23);
    assert!(time.is_ok());
    let time = time.unwrap();

    let time2 = Time::try_from_hms(0, 10, 50.23);
    assert!(time2.is_ok());
    let time2 = time2.unwrap();

    let date = Date::try_from_ymd(2020, 1, 20);
    assert!(date.is_ok());
    let date = date.unwrap();

    let date2 = Date::try_from_ymd(2000, 2, 28);
    assert!(date2.is_ok());
    let date2 = date2.unwrap();

    let interval = Interval::try_from_ymwd_hms(1, 1, 1, 2, 25, 15, 5.456);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    let interval2 = Interval::try_from_ymwd_hms(10, 10, 2, 4, 45, 55, 15.456);
    assert!(interval2.is_ok());
    let interval2 = interval2.unwrap();

    c.bench_function("date_sub_date", |b| {
        b.iter(|| {
            let _ret = black_box(date.sub_date(date2));
        })
    });

    c.bench_function("date_sub_int", |b| {
        b.iter(|| {
            let _ret = black_box(date.sub_days(20));
        })
    });

    c.bench_function("date_sub_interval", |b| {
        b.iter(|| {
            let _ret = black_box(date.sub_interval(interval));
        })
    });

    c.bench_function("time_sub_time", |b| {
        b.iter(|| {
            let _ret = black_box(time.sub_time(time2));
        })
    });

    c.bench_function("time_sub_interval", |b| {
        b.iter(|| {
            let _ret = black_box(time.sub_interval(interval));
        })
    });

    c.bench_function("timestamp_sub_interval", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.sub_interval(interval));
        })
    });

    c.bench_function("interval_sub_interval", |b| {
        b.iter(|| {
            let _ret = black_box(interval.sub_interval(interval2));
        })
    });

    c.bench_function("timestamp_sub_timestamp", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.sub_timestamp(timestamp2));
        })
    });
}

fn mul_benchmark(c: &mut Criterion) {
    let interval = Interval::try_from_ymwd_hms(321, 11, 1, 2, 25, 15, 5.456);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    c.bench_function("interval_multiply_f64", |b| {
        b.iter(|| {
            let _ret = black_box(interval.mul_f64(67.69753));
        })
    });
}

fn divide_benchmark(c: &mut Criterion) {
    let interval = Interval::try_from_ymwd_hms(212, 12, 1, 2, 25, 15, 5.456);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    c.bench_function("interval_divide_f64", |b| {
        b.iter(|| {
            let _ret = black_box(interval.div_f64(67.69753));
        })
    });
}

fn cmp_benchmark(c: &mut Criterion) {
    let time1 = Time::try_from_hms(23, 45, 56.785612);
    assert!(time1.is_ok());
    let time1 = time1.unwrap();

    let time2 = Time::try_from_hms(12, 56, 56.456723);
    assert!(time2.is_ok());
    let time2 = time2.unwrap();
    c.bench_function("time_compare_time", |b| {
        b.iter(|| {
            let _ret = black_box(time1 > time2);
        })
    });

    let timestamp1 = Timestamp::try_from_ymd_hms(2020, 1, 2, 1, 23, 34.78);
    assert!(timestamp1.is_ok());
    let timestamp1 = timestamp1.unwrap();

    let timestamp2 = Timestamp::try_from_ymd_hms(2019, 10, 12, 11, 43, 54.78);
    assert!(timestamp2.is_ok());
    let timestamp2 = timestamp2.unwrap();

    c.bench_function("timestamp_compare_timestamp.", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp1 > timestamp2);
        })
    });

    let date1 = Date::try_from_ymd(2019, 7, 12);
    assert!(date1.is_ok());
    let date1 = date1.unwrap();
    let date2 = Date::try_from_ymd(2018, 8, 31);
    assert!(date2.is_ok());
    let date2 = date2.unwrap();

    c.bench_function("date_compare_date", |b| {
        b.iter(|| {
            let _ret = black_box(date1 > date2);
        })
    });

    let interval1 = Interval::try_from_ymwd_hms(10, 1, 2, 1, 23, 56, 34.734568);
    assert!(interval1.is_ok());
    let interval1 = interval1.unwrap();
    let interval2 = Interval::try_from_ymwd_hms(8, 10, 5, 12, 11, 43, 54.734568);
    assert!(interval2.is_ok());
    let interval2 = interval2.unwrap();

    c.bench_function("interval_compare_interval", |b| {
        b.iter(|| {
            let _ret = black_box(interval1 > interval2);
        })
    });
}

fn date_part_benchmark(c: &mut Criterion) {
    let date = Date::try_from_ymd(2019, 7, 12);
    assert!(date.is_ok());
    let date = date.unwrap();

    c.bench_function("date_date_part", |b| {
        b.iter(|| {
            let _ret = black_box(date.date_part(FieldType::Unit, DateUnit::Year));
        })
    });

    let time = Time::try_from_str("20:45:34.673452", -1, DateOrder::YMD);
    assert!(time.is_ok());
    let time = time.unwrap();
    c.bench_function("time_date_part", |b| {
        b.iter(|| {
            let _ret = black_box(time.date_part(FieldType::Unit, DateUnit::Hour));
        })
    });

    let timestamp = Timestamp::try_from_str("2001-02-16 20:38:40.4567890", 6, DateOrder::YMD);
    assert!(timestamp.is_ok());
    let timestamp = timestamp.unwrap();
    c.bench_function("timestamp_date_part", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.date_part(FieldType::Unit, DateUnit::Hour));
        })
    });

    let interval = Interval::try_from_ymwd_hms(2001, 2, 0, 16, 20, 38, 40.4567890);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    c.bench_function("interval_date_part", |b| {
        b.iter(|| {
            let _ret = black_box(interval.date_part(FieldType::Unit, DateUnit::Hour));
        })
    });
}

fn truncate_benchmark(c: &mut Criterion) {
    let date = Date::try_from_ymd(2019, 7, 12);
    assert!(date.is_ok());
    let date = date.unwrap();

    c.bench_function("date_truncate", |b| {
        b.iter(|| {
            let _ret = black_box(date.truncate(FieldType::Unit, DateUnit::Year));
        })
    });

    let time = Time::try_from_str("20:45:34.673452", -1, DateOrder::YMD);
    assert!(time.is_ok());
    let time = time.unwrap();
    c.bench_function("time_truncate", |b| {
        b.iter(|| {
            let _ret = black_box(time.truncate(FieldType::Unit, DateUnit::Hour));
        })
    });

    let timestamp = Timestamp::try_from_str("2001-02-16 20:38:40.4567890", 6, DateOrder::YMD);
    assert!(timestamp.is_ok());
    let timestamp = timestamp.unwrap();
    c.bench_function("timestamp_truncate", |b| {
        b.iter(|| {
            let _ret = black_box(timestamp.truncate(FieldType::Unit, DateUnit::Hour));
        })
    });

    let interval = Interval::try_from_ymwd_hms(2001, 2, 0, 16, 20, 38, 40.4567890);
    assert!(interval.is_ok());
    let interval = interval.unwrap();

    c.bench_function("interval_truncate", |b| {
        b.iter(|| {
            let _ret = black_box(interval.truncate(FieldType::Unit, DateUnit::Hour));
        })
    });
}

criterion_group!(
    benches,
    parse_benchmark,
    try_from_number_benchmark,
    to_string_benchmark,
    add_benchmark,
    negative_benchmark,
    sub_benchmark,
    mul_benchmark,
    divide_benchmark,
    cmp_benchmark,
    date_part_benchmark,
    truncate_benchmark
);
criterion_main!(benches);
