use esploda::{bitcoin::Txid, esplora::Esplora};
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
    let esplora = Esplora::new("https://blockstream.info/testnet/api/")?;
    let block_hash = "00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435";
    let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74".parse()?;

    let mut resp = agent.run(esplora.get_block_txids(block_hash.parse()?))?;

    let txids: Vec<Txid> = resp.body_mut().read_json()?;
    let index = txids.iter().position(|id| *id == txid).unwrap();

    println!("TXID is at index {index} in the block");

    Ok(())
}
