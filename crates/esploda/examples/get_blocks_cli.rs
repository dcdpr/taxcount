use esploda::bitcoin::{BlockHash, Txid};
use esploda::esplora::Esplora;
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

    let block_hash: BlockHash = env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("Missing BlockHash"))?
        .parse()?;

    let mut resp = agent.run(esplora.get_block_txids(block_hash))?;

    let txids: Vec<Txid> = resp.body_mut().read_json()?;
    let config = PrettyConfig::new();

    println!(
        r#""{block_hash}": {},"#,
        ron::ser::to_string_pretty(&txids, config)?
    );

    Ok(())
}
