use chrono::{Datelike, NaiveDate, Utc};
use rand::Rng;

use crate::error::Result;
use crate::mutator::MutationContext;

pub fn date(ctx: &mut MutationContext) -> Result<String> {
    let current_year = Utc::now().year();
    let start_year = ctx
        .kwargs
        .get("start")
        .and_then(|v| v.as_i64())
        .unwrap_or((current_year - 1) as i64) as i32;
    let end_year = ctx
        .kwargs
        .get("end")
        .and_then(|v| v.as_i64())
        .unwrap_or(current_year as i64) as i32;
    let date_format = ctx
        .get_str_kwarg("date_format")
        .unwrap_or_else(|| "%Y-%m-%d".to_string());
    let unique = ctx.get_bool_kwarg("unique");

    let mut gen = || {
        let year = ctx.rng.gen_range(start_year..=end_year);
        let month = ctx.rng.gen_range(1..=12u32);
        let max_day = days_in_month(year, month);
        let day = ctx.rng.gen_range(1..=max_day);
        let d = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(year, month, 1).unwrap());
        d.format(&date_format).to_string()
    };

    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}
