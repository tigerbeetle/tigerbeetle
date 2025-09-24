// We are going to try to test the Rust redefinitions of TigerBeetle status
// codes and bitflags with a round-trip between the C status codes and the Rust
// representations. This will involve parsing the defs out of the tb_client.h
// header.
//
// We want to do this because the addition of new status codes will not trigger
// compilation failures, and may lead to silent bugs in application code.
//
// It would be much preferable to do this once for all clients, but I don't
// forsee Vortex being able to do that in the near future, and it's not clear
// exactly how it would do that anyway. But at least we can do it for the Rust
// client. Rust client is best client!

use tigerbeetle as tb;

static TB_CLIENT_H: &str = include_str!("../assets/tb_client.h");

/// Parse the enum values out of a C source.
fn parse_c_enum_values(name: &str) -> Vec<u32> {
    let enum_body_lines = find_enum_body_lines(TB_CLIENT_H, name);

    if enum_body_lines.is_empty() {
        panic!("enum {name} has no parsable body");
    }

    let mut enum_values = Vec::new();

    for line in enum_body_lines {
        enum_values.push(parse_enum_value(line));
    }

    enum_values
}

fn find_enum_body_lines<'t>(text: &'t str, name: &str) -> Vec<&'t str> {
    let start_line = format!("typedef enum {name} {{");
    let end_line = format!("}} {name};");

    let lines = text
        .lines()
        .skip_while(|line| line != &start_line)
        .skip(1)
        .take_while(|line| line != &end_line);

    lines.collect()
}

fn parse_enum_value(text: &str) -> u32 {
    let after_equals = text.split("=").skip(1).next();
    let after_equals = after_equals.expect(&format!("missing '=' in enum variant text '{text}'"));

    assert!(after_equals.ends_with(","));
    let expr = &after_equals[..after_equals.len() - 1];
    let expr = expr.trim();

    // At this point expr should either be an uint or "uint << uint" for flags.
    if !expr.contains("<<") {
        match expr.parse() {
            Ok(val) => val,
            Err(_) => {
                panic!("enum text '{text}' didn't parse as u32");
            }
        }
    } else {
        let mut split = expr.split("<<");
        let arg1 = split
            .next()
            .expect(&format!("missing bitshift arg1 in '{text}'"));
        let arg2 = split
            .next()
            .expect(&format!("missing bitshift arg2 in '{text}'"));
        let arg1: u32 = arg1
            .trim()
            .parse()
            .expect(&format!("enum arg1 in '{text}' didn't parse as u32"));
        let arg2: u32 = arg2
            .trim()
            .parse()
            .expect(&format!("enum arg1 in '{text}' didn't parse as u32"));
        arg1 << arg2
    }
}

#[test]
fn does_our_c_enum_parser_even_work() {
    let tb_log_level_values_expected = [0, 1, 2, 3];
    let tb_log_level_values_actual = parse_c_enum_values("TB_LOG_LEVEL");

    for val in tb_log_level_values_expected.iter() {
        assert!(tb_log_level_values_actual.contains(val));
    }

    for val in tb_log_level_values_actual.iter() {
        assert!(tb_log_level_values_expected.contains(val));
    }
}

fn round_trip_test<RustType, CType>(
    c_enum_name: &str,
    ignore_list: &[CType],
    rust_from_c: impl Fn(CType) -> RustType,
    c_from_rust: impl Fn(RustType) -> CType,
) where
    RustType: std::fmt::Debug,
    CType: std::fmt::Debug + Eq + Copy,
    CType: TryFrom<u32>,
    <CType as TryFrom<u32>>::Error: std::fmt::Debug,
{
    let c_values = parse_c_enum_values(c_enum_name);
    assert!(!c_values.is_empty());
    for c_value_u32 in c_values {
        let c_value_original = CType::try_from(c_value_u32)
            .expect(&format!("unexpected value {c_value_u32} for enum"));
        if ignore_list.contains(&c_value_original) {
            continue;
        }
        let rust_value = rust_from_c(c_value_original);
        let c_value_new = c_from_rust(rust_value);
        assert_eq!(c_value_original, c_value_new);
    }
}

#[test]
fn round_trip_create_account_result() {
    round_trip_test::<tb::CreateAccountResult, u32>(
        "TB_CREATE_ACCOUNT_RESULT",
        &[],
        |c_value| tb::CreateAccountResult::from(c_value),
        |rust_value| u32::from(rust_value),
    );
}

#[test]
fn round_trip_create_transfer_result() {
    round_trip_test::<tb::CreateTransferResult, u32>(
        "TB_CREATE_TRANSFER_RESULT",
        &[],
        |c_value| tb::CreateTransferResult::from(c_value),
        |rust_value| u32::from(rust_value),
    );
}

#[test]
fn round_trip_init_status() {
    round_trip_test::<tb::InitStatus, i32>(
        "TB_INIT_STATUS",
        // Success not represented in tb::InitStatus
        &[0],
        |c_value| tb::InitStatus::from(c_value),
        |rust_value| i32::from(rust_value),
    );
}

#[test]
fn round_trip_packet_status() {
    round_trip_test::<tb::PacketStatus, u8>(
        "TB_PACKET_STATUS",
        // Success not represented in tb::PacketStatus
        &[0],
        |c_value| tb::PacketStatus::from(c_value),
        |rust_value| u8::from(rust_value),
    );
}

#[test]
fn round_trip_account_flags() {
    round_trip_test::<tb::AccountFlags, u16>(
        "TB_ACCOUNT_FLAGS",
        &[],
        // We use from_bits_truncate here to discard unknown flags.
        // This will fail a round-trip test if we see one.
        |c_value| tb::AccountFlags::from_bits_truncate(c_value),
        |rust_value| rust_value.bits(),
    );
}

#[test]
fn round_trip_transfer_flags() {
    round_trip_test::<tb::TransferFlags, u16>(
        "TB_TRANSFER_FLAGS",
        &[],
        |c_value| tb::TransferFlags::from_bits_truncate(c_value),
        |rust_value| rust_value.bits(),
    );
}

#[test]
fn round_trip_account_filter_flags() {
    round_trip_test::<tb::AccountFilterFlags, u32>(
        "TB_ACCOUNT_FILTER_FLAGS",
        &[],
        |c_value| tb::AccountFilterFlags::from_bits_truncate(c_value),
        |rust_value| rust_value.bits(),
    );
}

#[test]
fn round_trip_query_filter_flags() {
    round_trip_test::<tb::QueryFilterFlags, u32>(
        "TB_QUERY_FILTER_FLAGS",
        &[],
        |c_value| tb::QueryFilterFlags::from_bits_truncate(c_value),
        |rust_value| rust_value.bits(),
    );
}
