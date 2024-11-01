use chrono::Utc;
use chrono_tz::Tz;
use croner::Cron;

/// Checks if the provided timezone string is valid.
pub fn is_valid_timezone(timezone_str: &str) -> bool {
    timezone_str.parse::<Tz>().is_ok()
}

/// Validates the given cron string.
pub fn is_valid_cron_string(cron_string: &str) -> bool {
    Cron::new(cron_string)
        .with_seconds_required()
        .with_dom_and_dow()
        .parse()
        .is_ok()
}

/// Calculates the next run time based on a cron string and timezone.
pub fn next_run(cron_str: &str, timezone_str: &str, last_run: i64) -> Option<i64> {
    let schedule = Cron::new(cron_str)
        .with_seconds_required()
        .with_dom_and_dow()
        .parse()
        .ok()?;

    let tz: Tz = timezone_str.parse().ok()?;

    // Find the next valid run time.
    for timestamp in schedule.iter_from(Utc::now().with_timezone(&tz)) {
        let millis = timestamp.timestamp_millis();
        if millis > last_run {
            return Some(millis);
        }
    }

    None
}
