use super::{WalletCsvRow, WalletError};
use serde::Deserialize;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerLiveError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("CSV parsing error")]
    Csv(#[from] csv::Error),

    #[error("Asset Name error")]
    AssetName(#[from] crate::errors::AssetNameError),

    #[error("Auditor error")]
    Auditor(#[from] super::AuditorError),

    #[error("Client error")]
    Client(#[from] crate::errors::ClientError),
}

#[derive(Debug, Deserialize)]
pub struct LedgerLiveCsvRow {
    #[serde(rename = "Currency Ticker")]
    pub(crate) asset: String,

    #[serde(rename = "Operation Hash")]
    pub(crate) txid: String,

    #[serde(rename = "Account Name")]
    pub(crate) account: String,

    #[serde(rename = "Account xpub")]
    pub(crate) xpub: String,
}

/// Read an LedgerLive history CSV.
pub fn read_ledgerlive(
    path: impl AsRef<Path>,
    rows: &mut Vec<WalletCsvRow>,
) -> Result<(), WalletError> {
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    for row in reader.deserialize() {
        rows.push(WalletCsvRow::LedgerLive(row?));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{read_tx_tags, resolve, tests::MockClient, Auditor};
    use super::*;
    use crate::{basis::AssetName, model::kraken_amount::KrakenAmount};
    use bdk::bitcoin::Network;
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn test_resolve_ledgerlive_wallet() {
        let _ = tracing_log::LogTracer::init();

        let path = "./references/ledgerlive-testnet-wallets/ledgerlive-history-testnet-wallet-20231128.csv";
        let client = MockClient::new("./fixtures/wallet/electrum_testnet_a.ron");
        let mut rows = Vec::new();
        read_ledgerlive(path, &mut rows).unwrap();
        let mut auditor = Auditor::new(Network::Testnet);
        let tags =
            read_tx_tags("./references/ledgerlive-testnet-wallets/testnet-tx-tags.csv").unwrap();
        let actual = resolve(rows, &client, &mut auditor, &tags);

        let fifos = actual.unwrap();
        assert_eq!(fifos.len(), 1);
        let tx = &fifos[&AssetName::Btc];

        assert_eq!(tx.len(), 9);

        // faucet_1 income
        assert_eq!(tx[0].ins.len(), 1);
        let expected = "53bf4d98a29eedd87791566b703e97ec3a73fb504d83084053af315704aca666:1";
        assert_eq!(tx[0].ins[0].external_id, expected);
        assert!(!tx[0].ins[0].mine);
        assert_eq!(tx[0].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00010000").unwrap();
        assert_eq!(tx[0].outs[0].amount, expected);
        assert!(tx[0].outs[0].mine);
        assert_eq!(tx[0].outs[0].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        assert!(!tx[0].outs[1].mine);
        let expected = KrakenAmount::new("ZUSD", "100.00").unwrap();
        assert_eq!(tx[0].exchange_rate, Some(expected));

        // faucet_2 income
        assert_eq!(tx[1].ins.len(), 1);
        let expected = "1732c92500f7e362e34913417eb235d758fb7eb0484271ab8ab53fd839c3d31f:0";
        assert_eq!(tx[1].ins[0].external_id, expected);
        assert!(!tx[1].ins[0].mine);
        assert_eq!(tx[1].outs.len(), 2);
        assert!(!tx[1].outs[0].mine);
        let expected = KrakenAmount::new("XXBT", "0.00001000").unwrap();
        assert_eq!(tx[1].outs[1].amount, expected);
        assert!(tx[1].outs[1].mine);
        assert_eq!(tx[1].outs[1].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        let expected = KrakenAmount::new("ZUSD", "120.00").unwrap();
        assert_eq!(tx[1].exchange_rate, Some(expected));

        // move_1 a -> b (tb1qavwp)
        assert_eq!(tx[2].ins.len(), 1);
        let expected = "19e06c33d6870057cad36ed17c45ec3fd95bfe4bf8802f26fe1c47b8b06c6805:1";
        assert_eq!(tx[2].ins[0].external_id, expected);
        assert!(tx[2].ins[0].mine);
        assert_eq!(tx[2].outs.len(), 1);
        let expected = KrakenAmount::new("XXBT", "0.00000500").unwrap();
        assert_eq!(tx[2].outs[0].amount, expected);
        assert!(tx[2].outs[0].mine);
        assert_eq!(
            tx[2].outs[0].accounts().collect::<Vec<_>>(),
            &["wallet-a", "wallet-b"],
        );
        assert_eq!(tx[2].exchange_rate, None);

        // exchange_deposit (tb1qws6)
        assert_eq!(tx[3].ins.len(), 1);
        let expected = "60575f03d3457dd8c67eed1c37ef6b6b7950a1ce109b300a6d770665bfaa9fe7:0";
        assert_eq!(tx[3].ins[0].external_id, expected);
        assert!(tx[3].ins[0].mine);
        assert_eq!(tx[3].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00001800").unwrap();
        assert_eq!(tx[3].outs[0].amount, expected);
        assert!(tx[3].outs[0].mine);
        assert_eq!(tx[3].outs[0].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        let expected = KrakenAmount::new("XXBT", "0.00008000").unwrap();
        assert_eq!(tx[3].outs[1].amount, expected);
        assert!(!tx[3].outs[1].mine);
        assert_eq!(tx[3].outs[1].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        assert_eq!(tx[3].exchange_rate, None);

        // ice cream (tb1qd4f) @ BTCUSD $110
        assert_eq!(tx[4].ins.len(), 1);
        let expected = "feceb335210ee31662a8f251cfac24b605b51db3d53d10f436470e5f473a6fa3:0";
        assert_eq!(tx[4].ins[0].external_id, expected);
        assert!(tx[4].ins[0].mine);
        assert_eq!(tx[4].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00000500").unwrap();
        assert_eq!(tx[4].outs[0].amount, expected);
        assert!(!tx[4].outs[0].mine);
        assert_eq!(tx[4].outs[0].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        let expected = KrakenAmount::new("XXBT", "0.00001100").unwrap();
        assert_eq!(tx[4].outs[1].amount, expected);
        assert!(tx[4].outs[1].mine);
        assert_eq!(tx[4].outs[1].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        let expected = KrakenAmount::new("ZUSD", "110.00").unwrap();
        assert_eq!(tx[4].exchange_rate, Some(expected));

        // exchange_withdrawal (tb1q9y4)
        assert_eq!(tx[5].ins.len(), 1);
        let expected = "66952b2fae8b675625a79b6b90411162335cb51296f2f8e2e963e0d1bc8a8ff2:0";
        assert_eq!(tx[5].ins[0].external_id, expected);
        assert!(!tx[5].ins[0].mine);
        assert_eq!(tx[5].outs.len(), 2);
        assert!(!tx[5].outs[0].mine);
        let expected = KrakenAmount::new("XXBT", "0.00006500").unwrap();
        assert_eq!(tx[5].outs[1].amount, expected);
        assert!(tx[5].outs[1].mine);
        assert_eq!(tx[5].outs[1].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        assert_eq!(tx[5].exchange_rate, None);

        // move_3 b->a (tb1qppp)
        assert_eq!(tx[6].ins.len(), 1);
        let expected = "1112f46698e586f781ada13bd5db58c54eb24495ab669d8518dd37c4af6cb622:0";
        assert_eq!(tx[6].ins[0].external_id, expected);
        assert!(tx[6].ins[0].mine);
        assert_eq!(tx[6].outs.len(), 1);
        let expected = KrakenAmount::new("XXBT", "0.00000390").unwrap();
        assert_eq!(tx[6].outs[0].amount, expected);
        assert!(tx[6].outs[0].mine);
        assert_eq!(
            tx[6].outs[0].accounts().collect::<Vec<_>>(),
            &["wallet-a", "wallet-b"],
        );
        assert_eq!(tx[6].exchange_rate, None);

        // move_2 a -> b (tb1qav)
        assert_eq!(tx[7].ins.len(), 1);
        let expected = "cb8e1ca8865f921eef861cd40ea9f29de1450fd1d922c4aa02e8acc83728dc1c:1";
        assert_eq!(tx[7].ins[0].external_id, expected);
        assert!(tx[7].ins[0].mine);
        assert_eq!(tx[7].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00001000").unwrap();
        assert_eq!(tx[7].outs[0].amount, expected);
        assert!(tx[7].outs[0].mine);
        assert_eq!(
            tx[7].outs[0].accounts().collect::<Vec<_>>(),
            &["wallet-a", "wallet-b"]
        );
        let expected = KrakenAmount::new("XXBT", "0.00005300").unwrap();
        assert_eq!(tx[7].outs[1].amount, expected);
        assert!(tx[7].outs[1].mine);
        assert_eq!(
            tx[7].outs[1].accounts().collect::<Vec<_>>(),
            &["wallet-a", "wallet-b"]
        );
        assert_eq!(tx[7].exchange_rate, None);

        // ice cream 2 (oops ice cream address reuse)
        assert_eq!(tx[8].ins.len(), 3);
        let expected = "2192c1aef57f976db693d1444e9d1465db55f81624b8834689ced974e5532000:0";
        assert_eq!(tx[8].ins[0].external_id, expected);
        assert!(tx[8].ins[0].mine);
        let expected = "33f312a2585e8df768b406c118bed170c8a87aebc9e0ae371faeb06b6e3e9507:1";
        assert_eq!(tx[8].ins[1].external_id, expected);
        assert!(tx[8].ins[1].mine);
        let expected = "fc62eb2e25bb44a146a93c75471f229ef79b93eac2e9307300b9fa6b28e481ee:1";
        assert_eq!(tx[8].ins[2].external_id, expected);
        assert!(tx[8].ins[2].mine);
        assert_eq!(tx[8].outs.len(), 1);
        let expected = KrakenAmount::new("XXBT", "0.00006250").unwrap();
        assert_eq!(tx[8].outs[0].amount, expected);
        assert!(!tx[8].outs[0].mine);
        assert_eq!(tx[8].outs[0].accounts().collect::<Vec<_>>(), &["wallet-a"]);
        let expected = KrakenAmount::new("ZUSD", "110.00").unwrap();
        assert_eq!(tx[8].exchange_rate, Some(expected));
    }
}
