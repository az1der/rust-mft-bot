// import 
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use serde::Deserialize;

// overbook: a:ask/ b:bid
#[derive(Deserialize, Debug)]
struct OrderbookData {
    #[serde(rename= "s")]
    symbol: String,

    #[serde(default)]
    b: Vec<Vec<String>>,

    #[serde(default)]
    #[serde(rename= "a")]
    asks: Vec<Vec<String>>,
}

#[derive(Deserialize,Debug)]
struct BybitResponse {
    topic: String,
    data: Option<OrderbookData>,
}

// tokio asynchronous runtime
#[tokio::main]
async fn main() {
    // connection: bybit/testnet/futures
    let connect_addr: &str= "wss://stream-testnet.bybit.com/v5/public/linear";
    let url= Url::parse(connect_addr).unwrap();
    println!("connecting...");

    let (ws_stream, _)= connect_async(url)
        .await
        .expect("failed to connect");
    println!("connected");

    let (mut write, mut read)= ws_stream.split();
    let subscribe_msg= r#"{"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}"#;

    write.send(Message::Text(subscribe_msg.to_string()))
        .await
        .expect("failed to send subscription message");
    
    println!("waiting for data...");

    //loop
    while let Some(msg)= read.next().await {
        match msg {
            Ok(message)=> {
                if let Message::Text(text)= message {
                    match serde_json::from_str::<BybitResponse>(&text) {
                        Ok(parsed)=> {
                            if let Some(data)= parsed.data {
                                let best_bid= data.b.get(0);
                                let best_ask= data.asks.get(0);

                                if let (Some(bid), Some(ask))= (best_bid, best_ask) {
                                    // bid[0]: price, bid[1]: vol
                                    println!("{} | Buy: ${} (x{}) | Sell: ${} (x{})",
                                    data.symbol,
                                    bid[0], bid[1],
                                    ask[0], ask[1]);
                                }
                            } 
                        },
                        Err(_)=> {}
                    }
                } 
            }
            Err(e)=> {
                eprintln!("network error: {:?}", e);
                break;
            }
        }
    }
}



