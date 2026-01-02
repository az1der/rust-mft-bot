// import 
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

// tokio asynchronous runtime
#[tokio::main]
async fn main() {
    // connection: bybit/testnet/futures
    let connect_addr: &str= "wss://stream-testnet.bybit.com/v5/public/linear";
    let url= Url::parse(connect_addr).unwrap();
    println!("connecting...");

    let (ws_stream, _)= connect_async(url).await.unwrap();
    println!("connected");

    let (mut write, mut read)= ws_stream.split();
    let subscribe_msg= r#"{"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}"#;

    write.send(Message::Text(subscribe_msg.to_string())).await.unwrap();

    //loop
    while let Some(msg)= read.next().await {
        let message= msg.unwrap();

        //raw data
        println!("data: {}", message);
    }
}



