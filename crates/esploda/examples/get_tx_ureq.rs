use esploda::esplora::{Esplora, Transaction};
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
    let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74";

    let mut resp = agent.run(esplora.get_tx(txid.parse()?))?;

    let tx: Transaction = resp.body_mut().read_json()?;

    println!("{tx:#?}");

    Ok(())
}
