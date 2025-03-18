use esploda::bitcoin::Txid;
use esploda::esplora::{Esplora, Transaction};
use ron::ser::PrettyConfig;
use std::env;
use ureq::tls::{TlsConfig, TlsProvider};
use ureq::Agent;

fn main() -> anyhow::Result<()> {
    let agent = Agent::from(
        Agent::config_builder()
            .tls_config(
                TlsConfig::builder()
                    .provider(TlsProvider::NativeTls)
                    .build(),
            )
            .build(),
    );
    let api_server = env::var("ESPLORA_URL")
        .unwrap_or_else(|_| "https://blockstream.info/testnet/api/".to_string());
    let esplora = Esplora::new(api_server)?;

    let txid: Txid = env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("Missing TxId"))?
        .parse()?;

    let mut resp = agent.run(esplora.get_tx(txid))?;

    let tx: Transaction = resp.body_mut().read_json()?;
    let config = PrettyConfig::new().struct_names(true);

    println!(r#""{txid}": {},"#, ron::ser::to_string_pretty(&tx, config)?);

    Ok(())
}
