use super::*;
use crate::model::kraken_amount::{BitcoinAmount, KrakenAmount};
use chrono::TimeZone;

fn usd(n: i64) -> UsdAmount {
    UsdAmount::from_int(n)
}

/// Regression test for issue #3: pr_statement24_dates() must collect dates from
/// tx_fees, not only trade_details. A worksheet with fee-only bona fide activity
/// must still produce dates for the PR Statement-24 row.
#[test]
fn pr_statement24_dates_from_fees_only() {
    let basis_date = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
    let event_date = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

    let worksheet = CapGainsWorksheet {
        worksheet: vec![CapGainsWorksheetRow {
            event_date,
            internal_account: String::new(),
            ledger_row_id: String::new(),
            event_subtype: EventSubType::Trade,
            event_name: String::new(),
            asset_out_exchange_rate: String::new(),
            asset_in_exchange_rate: String::new(),
            proceeds: usd(0),
            trade_details: vec![],
            income_details: vec![],
            position_details: vec![],
            tx_fees: vec![EventFee {
                asset_fee: KrakenAmount::Btc(BitcoinAmount::default()),
                net_loss: GainTerm::ShortBonaFide(GainPortion {
                    basis: usd(5000),
                    basis_date,
                    basis_synthetic_id: String::new(),
                    net_gain: usd(-5000),
                }),
            }],
            position_fees: vec![],
        }],
    };

    let dates = worksheet.pr_statement24_dates();

    assert_eq!(dates.st_earliest_acquired, Some(basis_date));
    assert_eq!(dates.st_latest_sold, Some(event_date));
    assert_eq!(dates.lt_earliest_acquired, None);
    assert_eq!(dates.lt_latest_sold, None);
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
