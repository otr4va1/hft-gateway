use tokio_tungstenite::{connect_async,tungstenite::error::Error};
use futures_util::{StreamExt,};
#[tokio::main]
async fn main() -> Result<(),Error> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    let (ws_stream, _) = connect_async(url).await?;

    let (_ws_writer, mut ws_reader) = ws_stream.split();
    while let Some(message_result) = ws_reader.next().await {
        match message_result {
            Ok(message) => {
                if message.is_text() {
                    println!("Received message: {}", message.to_text()?);
                }
            }
            Err(e) => {
                println!("Error receiving message: {}", e);
            }
        }
    }
    Ok(())
}