use tokio::time::Duration;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        error::Error,
        Message::{Binary, Close, Frame, Ping, Pong, Text},
    }
};   

const RECONECT_AFTER: Duration = Duration::from_secs(5);


#[tokio::main]
async fn main() -> Result<(),Error> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    
    loop {
        println!("Connecting to {}", url);
        
        let (ws_stream, _) = match connect_async(url).await {
            Ok(s) => {
                println!("Succecfully connected!");
                s
            },
            Err(e) => {
                println!("Connection error: {}, Retring in {:?}...", e, RECONECT_AFTER);
                tokio::time::sleep(RECONECT_AFTER).await;
                continue;
            }
        };
        let (mut ws_writer, mut ws_reader) = ws_stream.split();

        while let Some(message_result) = ws_reader.next().await {
                match message_result {
                    Ok(message) => match message {
                        Text(trade) => {
                            println!("Received trade: {}", trade.as_str());
                        },
                        Ping(playload) => {
                            println!("Recieved Ping. Replying with Pong...");
                            if let Err(e) = ws_writer.send(Pong(playload)).await {
                                println!("Failed to pong server: {}. Reconnecting...", e);
                                break;
                            }
                        },
                        Pong(_) => {},
                        Binary(_) => {},
                        Close(_) => {
                            println!("Server closed connection. Reconnecting...");
                            break;
                        },
                        Frame(_) => {}
                    }
                
                    Err(e) => {
                        println!("Error receiving message: {}. Reconnecting", e);
                        break;
                    }
                }
            }
        tokio::time::sleep(RECONECT_AFTER).await;
    }
}