use super::{Auditor, WalletCsvRow, WalletError};
use bdk::bitcoin::{Address, Network};
use serde::Deserialize;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ElectrumError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error")]
    Json(#[from] serde_json::Error),

    #[error("Auditor error")]
    Auditor(#[from] super::AuditorError),

    #[error("Bitcoin address parsing error")]
    Address(#[from] bdk::bitcoin::address::Error),

    #[error("CSV parsing error")]
    Csv(#[from] csv::Error),

    #[error("Invalid asset")]
    Asset(#[from] crate::errors::ConvertAmountError),

    #[error("Client error")]
    Client(#[from] crate::errors::ClientError),
}

#[derive(Debug, Deserialize)]
pub struct ElectrumCsvRow {
    pub(crate) transaction_hash: String,
    pub(crate) label: String,
    #[serde(default)]
    pub(crate) account: String,
}

#[derive(Debug, Deserialize)]
struct Wallet {
    addresses: Addresses,
    keystore: Keystore,
}

#[derive(Debug, Deserialize)]
struct Addresses {
    change: Vec<String>,
    receiving: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Keystore {
    xpub: String,
}

impl Auditor {
    // TODO: This is not called by the CLI.
    /// Create an auditor from Electrum JSON wallet exports.
    pub fn from_electrum<I, P>(paths: I, network: Network) -> Result<Self, ElectrumError>
    where
        I: Iterator<Item = P>,
        P: AsRef<Path>,
    {
        let mut auditor = Auditor::new(network);

        let wallets = paths
            .into_iter()
            .map(|path| {
                let data = std::fs::read_to_string(path.as_ref())?;
                let wallet: Wallet = serde_json::from_str(&data)?;
                Ok(wallet)
            })
            .collect::<Result<Vec<_>, ElectrumError>>()?;

        // Derive addresses in parallel.
        auditor.add_xpubs(wallets.iter().map(|wallet| wallet.keystore.xpub.as_str()))?;

        for wallet in wallets {
            auditor.sanity_check(&wallet, network)?;
        }

        Ok(auditor)
    }

    // Sanity check the wallet.
    fn sanity_check(&mut self, wallet: &Wallet, network: Network) -> Result<(), ElectrumError> {
        for address in &wallet.addresses.change {
            let address = address.parse::<Address<_>>()?.require_network(network)?;
            assert!(self.is_mine(address.script_pubkey().as_script()));
        }
        for address in &wallet.addresses.receiving {
            let address = address.parse::<Address<_>>()?.require_network(network)?;
            assert!(self.is_mine(address.script_pubkey().as_script()));
        }

        Ok(())
    }
}

/// Read an Electrum history CSV.
pub fn read_electrum(
    path: impl AsRef<Path>,
    rows: &mut Vec<WalletCsvRow>,
) -> Result<(), WalletError> {
    let account = path
        .as_ref()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    for row in reader.deserialize() {
        let mut row: ElectrumCsvRow = row?;
        row.account = account.clone();
        rows.push(WalletCsvRow::Electrum(row));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{read_tx_tags, resolve, tests::MockClient, TxoInfo};
    use super::*;
    use crate::{basis::AssetName, model::kraken_amount::KrakenAmount};
    use std::{collections::BTreeSet, path::PathBuf};
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn test_auditor_from_electrum() {
        let _ = tracing_log::LogTracer::init();

        let root = PathBuf::from("./references/electrum-testnet");

        Auditor::from_electrum(
            [
                root.join("testnet-wallet-a-20231128"),
                root.join("testnet-wallet-b-20231128"),
                root.join("testnet-wallet-icecream-20231128"),
                root.join("testnet-wallet-PJFargo-20231128"),
            ]
            .into_iter(),
            Network::Testnet,
        )
        .unwrap();
    }

    // TODO: Write tests like `test_resolve_tx_wallet_a` for the following files:
    //
    // - "history-testnet-wallet-b-20231128.csv"
    // - "history-testnet-wallet-icecream-20231128.csv"
    // - "history-testnet-wallet-PJFargo-20231128.csv"

    #[test]
    #[traced_test]
    fn test_resolve_tx_wallet_a_2023() {
        let _ = tracing_log::LogTracer::init();

        let root = PathBuf::from("./references/electrum-testnet");

        let name = "history-testnet-wallet-a-2023.csv";
        let client = MockClient::new("./fixtures/wallet/electrum_testnet_a.ron");
        let mut rows = Vec::new();
        read_electrum(root.join(name), &mut rows).unwrap();
        let mut auditor = Auditor::from_electrum(
            [
                root.join("testnet-wallet-a-20231128"),
                root.join("testnet-wallet-b-20231128"),
            ]
            .into_iter(),
            Network::Testnet,
        )
        .unwrap();
        let tags = read_tx_tags("./references/electrum-testnet/testnet-tx-tags.csv").unwrap();
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
        assert_eq!(
            tx[0].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "faucet_1 income".to_string()
            }])
        );
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
        assert_eq!(
            tx[1].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "faucet_2 income".to_string()
            }])
        );
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
            tx[2].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "move_1 a -> b (tb1qavwp)".to_string()
            }])
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
        assert_eq!(
            tx[3].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "exchange_deposit (tb1qws6)".to_string()
            }])
        );
        let expected = KrakenAmount::new("XXBT", "0.00008000").unwrap();
        assert_eq!(tx[3].outs[1].amount, expected);
        assert!(!tx[3].outs[1].mine);
        assert_eq!(
            tx[3].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "exchange_deposit (tb1qws6)".to_string()
            }])
        );
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
        assert_eq!(
            tx[4].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "ice cream (tb1qd4f) @ BTCUSD $110".to_string()
            }])
        );
        let expected = KrakenAmount::new("XXBT", "0.00001100").unwrap();
        assert_eq!(tx[4].outs[1].amount, expected);
        assert!(tx[4].outs[1].mine);
        assert_eq!(
            tx[4].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "ice cream (tb1qd4f) @ BTCUSD $110".to_string()
            }])
        );
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
        assert_eq!(
            tx[5].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "exchange_withdrawal (tb1q9y4)".to_string()
            }])
        );
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
            tx[6].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "move_3 b->a (tb1qppp)".to_string()
            }])
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
            tx[7].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "move_2 a -> b (tb1qav)".to_string()
            }])
        );
        let expected = KrakenAmount::new("XXBT", "0.00005300").unwrap();
        assert_eq!(tx[7].outs[1].amount, expected);
        assert!(tx[7].outs[1].mine);
        assert_eq!(
            tx[7].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "move_2 a -> b (tb1qav)".to_string()
            }])
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
        assert_eq!(
            tx[8].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: name.to_string(),
                note: "ice cream 2 (oops ice cream address reuse)".to_string()
            }])
        );
        let expected = KrakenAmount::new("ZUSD", "110.00").unwrap();
        assert_eq!(tx[8].exchange_rate, Some(expected));
    }
}
