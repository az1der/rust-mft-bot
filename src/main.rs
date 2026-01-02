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
                    println!("recived data: {}", text);
                } 
            }
            Err(e)=> {
                eprintln!("network error: {:?}", e);
                break;
            }
        }
    }
}



