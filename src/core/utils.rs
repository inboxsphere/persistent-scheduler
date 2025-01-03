#[macro_export(local_inner_macros)]
macro_rules! generate_token {
    () => {
        ulid::Ulid::new().to_string()
    };
}

#[macro_export(local_inner_macros)]
macro_rules! utc_now {
    () => {{
        use chrono::Utc;
        Utc::now().timestamp_millis()
    }};
}
