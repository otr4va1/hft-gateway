use tokio::time::Duration;
use serde::Deserialize;
use serde_json;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        error::Error,
        Message::{Binary, Close, Frame, Ping, Pong, Text},
    }
};

use crate::rtrb::SpscRingBuffer;   
mod rtrb;

const RECONECT_AFTER: Duration = Duration::from_secs(5);
pub enum ParseError {
    Er
}
#[repr(C)]
pub struct Trade {
    pub id: i64,
    pub price: f64,
    pub qty: f64,
    pub time: i64,
    pub is_buyer_maker: bool
}
#[derive(Deserialize)]
struct BinanceTradeDto<'a> {
    #[serde(rename = "t")]
    agg_trade_id: i64,
    #[serde(rename = "p")]
    price: &'a str,
    #[serde(rename = "q")]
    quantity: &'a str,
    #[serde(rename = "T")]
    trade_time: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}
impl Trade {
    pub async fn parse_from_json(json_str: &str) -> Result<Trade, ParseError> {
        let dto: BinanceTradeDto = serde_json::from_str(json_str)
            .map_err(|_| ParseError::Er)?;
        
        let price = dto.price.parse().unwrap_or(0.0);
        let qty = dto.quantity.parse().unwrap_or(0.0);
            
        Ok(Trade {
            id: dto.agg_trade_id,
            price: price,
            qty: qty,
            time: dto.trade_time,
            is_buyer_maker: dto.is_buyer_maker
        })
    }
}
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
                        Text(trade_json) => {
                            let start_time = std::time::Instant::now();

                            match Trade::parse_from_json(&trade_json).await {
                                Ok(trade) => {
                                    let (mut prd, mut cmr) = SpscRingBuffer::<16, Trade>::new().await;

                                    match prd.push(trade).await {
                                        Ok(_) => {},
                                        Err(_) => { println!("Ignored")}
                                    };
                                    let elapsed_nanos = start_time.elapsed().as_nanos();
                                    tokio::spawn(async move {
                                        let maybe_trade = cmr.pop().await;
                                        match maybe_trade {
                                            Some(trade) => {
                                                println!("Trade #{} | Price: {:.2} | Qty: {:.4} | Latency: {} ns",
                                                    trade.id, trade.price, trade.qty, elapsed_nanos
                                                );
                                            },
                                            None => {}
                                        }
                                    });
                                },
                                Err(_) => {
                                    println!("Failed to parse JSON");
                                }
                            }
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