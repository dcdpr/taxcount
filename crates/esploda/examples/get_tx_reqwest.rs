use esploda::esplora::{Esplora, Transaction};
use reqwest::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let esplora = Esplora::new("https://blockstream.info/testnet/api/")?;
    let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74";

    let resp = client
        .execute(esplora.get_tx(txid.parse()?).map(|_| "").try_into()?)
        .await?;

    let tx: Transaction = resp.json().await?;

    println!("{tx:#?}");

    Ok(())
}
