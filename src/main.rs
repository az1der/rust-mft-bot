use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use serde::Deserialize;
use chrono::{DateTime, Utc};
use std::time::{Duration, Instant};
use std::fs::File;
use std::sync::Arc;

// parquet/ arrow
use arrow::array::{Float64Builder, StringBuilder, Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

const BATCH_SIZE: usize = 100;
const RUN_TIME_HOURS: u64 = 2;

// orderbook: a:ask/ b:bid
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
    ts: u64,
    data: Option<OrderbookData>,
}

// tokio asynchronous runtime
#[tokio::main]
async fn main() {
    println!("MFT data collecting..."); // <- POPRAWKA 1: Dodano średnik

    let start_time = Instant::now();
    let max_duration = Duration::from_secs(RUN_TIME_HOURS * 60 * 60);

    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid_price", DataType::Float64, false), 
        Field::new("bid_qty", DataType::Float64, false),   
        Field::new("ask_price", DataType::Float64, false), 
        Field::new("ask_qty", DataType::Float64, false),
    ]);
    let schema_ref = Arc::new(schema);
    let file = File::create("market_data.parquet").expect("error");
    let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), None).expect("initialization error");

    let mut ts_builder = StringBuilder::new();
    let mut sym_builder = StringBuilder::new();
    let mut bid_p_builder = Float64Builder::new();
    let mut bid_q_builder = Float64Builder::new();
    let mut ask_p_builder = Float64Builder::new();
    let mut ask_q_builder = Float64Builder::new();

    // connection: bybit/mainnet/futures
    let connect_addr: &str= "wss://stream.bybit.com/v5/public/linear";
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
    
    let mut count= 0;    

    println!("waiting for data...");

    //loop
    while let Some(msg)= read.next().await {

        if start_time.elapsed() > max_duration {
            println!("\n time's up!");
            break; 
        }

        match msg {
            Ok(message)=> {
                if let Message::Text(text)= message {
                    if let Ok(parsed) = serde_json::from_str::<BybitResponse>(&text) {
                            if let Some(data)= parsed.data {
                                let best_bid= data.b.get(0);
                                let best_ask= data.asks.get(0);
                                
                                // POPRAWKA 2 i 3:
                                // Najpierw sprawdzamy czy bid i ask istnieją (if let)
                                // Dopiero w środku tworzymy zmienne bp, bq...
                                if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                                    
                                    // POPRAWKA 4: Przywrócono obliczanie czasu (time_str)
                                    let d = std::time::UNIX_EPOCH + Duration::from_millis(parsed.ts);
                                    let time_str = DateTime::<Utc>::from(d).format("%H:%M:%S%.3f").to_string();

                                    let bp = bid[0].parse::<f64>().unwrap_or(0.0);
                                    let bq = bid[1].parse::<f64>().unwrap_or(0.0);
                                    let ap = ask[0].parse::<f64>().unwrap_or(0.0);
                                    let aq = ask[1].parse::<f64>().unwrap_or(0.0);

                                    ts_builder.append_value(&time_str);
                                    sym_builder.append_value(&data.symbol);
                                    bid_p_builder.append_value(bp);
                                    bid_q_builder.append_value(bq);
                                    ask_p_builder.append_value(ap);
                                    ask_q_builder.append_value(aq);

                                    count += 1;

                                    let remaining = max_duration.saturating_sub(start_time.elapsed()).as_secs();
                                    let rem_h = remaining / 3600;
                                    let rem_m = (remaining % 3600) / 60;
                                    print!("\rPrice: ${} | Bufor: {}/{} | Left: {:02}h {:02}m", bp, count, BATCH_SIZE, rem_h, rem_m);

                                    if count >= BATCH_SIZE {
                                        // println!("\n saving batch..."); // Opcjonalne

                                        let batch = RecordBatch::try_new(
                                            schema_ref.clone(),
                                            vec![
                                                Arc::new(ts_builder.finish()),
                                                Arc::new(sym_builder.finish()),
                                                Arc::new(bid_p_builder.finish()),
                                                Arc::new(bid_q_builder.finish()),
                                                Arc::new(ask_p_builder.finish()),
                                                Arc::new(ask_q_builder.finish()),
                                            ],
                                        ).unwrap();
                                        
                                        writer.write(&batch).expect("saving error");
                                        // writer.flush().expect("flushing error"); // ArrowWriter robi to sam

                                        count = 0;
                                        ts_builder = StringBuilder::new();
                                        sym_builder = StringBuilder::new();
                                        bid_p_builder = Float64Builder::new();
                                        bid_q_builder = Float64Builder::new();
                                        ask_p_builder = Float64Builder::new();
                                        ask_q_builder = Float64Builder::new();
                                    } 
                                } // Koniec if let
                            }                       
                    }
                } 
            }
            Err(e)=> {
                eprintln!("network error: {:?}", e);
                break;
            }
        }
    }
    println!("\nclosing parquet...");
    writer.close().unwrap();
    println!("market_data.parquet is saved");
}