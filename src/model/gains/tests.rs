use super::*;
use chrono::TimeZone;

fn usd(n: i64) -> UsdAmount {
    UsdAmount::from_int(n)
}

#[test]
fn pr_statement24_from_sums() {
    let sums = Sums {
        ledger_proceeds: usd(0),
        gain_matrix: GainMatrix {
            income: usd(0),
            us_short: GainMatrixShort::default(),
            us_long: GainMatrixLong::default(),
            bona_fide_short: Some(GainMatrixShort {
                trade_proceeds: usd(500),
                ..Default::default()
            }),
            bona_fide_long: Some(GainMatrixLong {
                trade_proceeds: usd(1000),
                ..Default::default()
            }),
        },
        gains_us_short: usd(50),
        gains_us_long: usd(200),
        gains_bona_fide_short: Some(usd(100)),
        gains_bona_fide_long: Some(usd(300)),
    };

    let dates = PrStatement24Dates {
        lt_earliest_acquired: Some(Utc.with_ymd_and_hms(2020, 1, 15, 0, 0, 0).unwrap()),
        lt_latest_sold: Some(Utc.with_ymd_and_hms(2024, 12, 15, 0, 0, 0).unwrap()),
        st_earliest_acquired: Some(Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap()),
        st_latest_sold: Some(Utc.with_ymd_and_hms(2024, 11, 20, 0, 0, 0).unwrap()),
    };

    let worksheet_name = WorksheetName::from("test-exchange".to_string());
    let statement = PrStatement24::from_worksheet(&worksheet_name, &sums, &dates);

    let expected = concat!(
        r#""Property Description","Date Acquired","Date Sold","A: Sale Price","B: Market Value","C: Adjusted Basis","D: Gain or Loss","E: US-Sourced Gain","F: PR-Sourced Gain""#,
        "\n",
        r#""investment assets test-exchange LT","2020-01-15","2024-12-15","1000.0000","700.0000","500.0000","500.0000","200.0000","300.0000""#,
        "\n",
        r#""investment assets test-exchange ST","2024-03-01","2024-11-20","500.0000","400.0000","350.0000","150.0000","50.0000","100.0000""#,
        "\n",
    );
    assert_eq!(statement.to_string(), expected);
}
