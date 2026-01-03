// change to Binance due to API limitations

// import
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use url::Url;
use chrono::{DateTime, Utc};
use std::time::{Duration, Instant};
use std::fs::File;
use std::sync::Arc;
use serde_json::Value;

use arrow::array::{Float64Builder, StringBuilder, Int64Builder, Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

// 10 min of collecting data
const BATCH_SIZE: usize = 100;
const RUN_TIME_MINUTES: u64 = 10;

#[tokio::main]
async fn main() {
    println!("binance full-depth collector (l20 + trades)...");

    let start_time = Instant::now();
    let max_duration = Duration::from_secs(RUN_TIME_MINUTES * 60);

    // schema: json columns for bids/asks
    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("latency_ms", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        
        // trade data
        Field::new("trade_price", DataType::Float64, false),
        Field::new("trade_qty", DataType::Float64, false),
        Field::new("trade_side", DataType::Utf8, false), 

        // orderbook data (full l20 as json string)
        Field::new("bids_json", DataType::Utf8, false),
        Field::new("asks_json", DataType::Utf8, false),
    ]);
    let schema_ref = Arc::new(schema);

    let file = File::create("binance_full_depth.parquet").expect("create file error");
    let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), None).expect("writer init error");

    // builders
    let mut ts_builder = StringBuilder::new();
    let mut type_builder = StringBuilder::new();
    let mut lat_builder = Int64Builder::new();
    let mut sym_builder = StringBuilder::new();
    
    let mut tp_builder = Float64Builder::new(); 
    let mut tq_builder = Float64Builder::new(); 
    let mut ts_side_builder = StringBuilder::new(); 
    
    let mut bids_builder = StringBuilder::new(); 
    let mut asks_builder = StringBuilder::new(); 

    let connect_addr = "wss://stream.binance.com:9443/ws/btcusdc@depth20@100ms/btcusdc@trade";
    let url = Url::parse(connect_addr).unwrap();

    println!("connecting...");
    let (ws_stream, _) = connect_async(url).await.expect("connection failed");
    println!("connected");

    let (_, mut read) = ws_stream.split();
    let mut count = 0;

    while let Some(msg) = read.next().await {
        if start_time.elapsed() > max_duration {
            println!("\ntime's up!");
            break;
        }

        match msg {
            Ok(message) => {
                if let tokio_tungstenite::tungstenite::protocol::Message::Text(text) = message {
                    
                    let now = Utc::now();
                    let local_ts_ms = now.timestamp_millis();
                    let time_str = now.format("%H:%M:%S%.3f").to_string();

                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        
                        let mut event_type = "UNKNOWN";
                        let mut latency: i64 = 0;
                        let symbol = "BTCUSDC";
                        
                        // temp vars
                        let mut t_price = 0.0;
                        let mut t_qty = 0.0;
                        let mut t_side = "";
                        let mut bids_str = "".to_string();
                        let mut asks_str = "".to_string();

                        // 1. handle trade
                        if v.get("e") == Some(&Value::String("trade".to_string())) {
                            event_type = "TRADE";
                            let trade_time = v["T"].as_i64().unwrap_or(local_ts_ms);
                            latency = local_ts_ms - trade_time;
                            
                            t_price = v["p"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                            t_qty = v["q"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                            
                            // is maker? true=sell, false=buy
                            let is_maker = v["m"].as_bool().unwrap_or(false); 
                            t_side = if is_maker { "SELL" } else { "BUY" };

                        // 2. handle depth (l20)
                        } else if !v["bids"].is_null() {
                            event_type = "DEPTH";
                            latency = -1;
                            
                            // save full array as json string
                            bids_str = v["bids"].to_string();
                            asks_str = v["asks"].to_string();
                        }

                        if event_type != "UNKNOWN" {
                            ts_builder.append_value(&time_str);
                            type_builder.append_value(event_type);
                            lat_builder.append_value(latency);
                            sym_builder.append_value(symbol);
                            
                            // fill columns
                            if event_type == "TRADE" {
                                tp_builder.append_value(t_price);
                                tq_builder.append_value(t_qty);
                                ts_side_builder.append_value(t_side);
                                bids_builder.append_value(""); 
                                asks_builder.append_value(""); 
                            } else {
                                tp_builder.append_value(0.0);
                                tq_builder.append_value(0.0);
                                ts_side_builder.append_value("");
                                bids_builder.append_value(&bids_str); 
                                asks_builder.append_value(&asks_str); 
                            }

                            count += 1;

                            // terminal preview
                            if event_type == "TRADE" {
                                println!("[TRADE] latency: {}ms | price: {} | side: {}", latency, t_price, t_side);
                            }

                            if count >= BATCH_SIZE {
                                let batch = RecordBatch::try_new(
                                    schema_ref.clone(),
                                    vec![
                                        Arc::new(ts_builder.finish()),
                                        Arc::new(type_builder.finish()),
                                        Arc::new(lat_builder.finish()),
                                        Arc::new(sym_builder.finish()),
                                        Arc::new(tp_builder.finish()),
                                        Arc::new(tq_builder.finish()),
                                        Arc::new(ts_side_builder.finish()),
                                        Arc::new(bids_builder.finish()),
                                        Arc::new(asks_builder.finish()),
                                    ],
                                ).unwrap();

                                writer.write(&batch).expect("write error");
                                
                                ts_builder = StringBuilder::new();
                                type_builder = StringBuilder::new();
                                lat_builder = Int64Builder::new();
                                sym_builder = StringBuilder::new();
                                tp_builder = Float64Builder::new();
                                tq_builder = Float64Builder::new();
                                ts_side_builder = StringBuilder::new();
                                bids_builder = StringBuilder::new();
                                asks_builder = StringBuilder::new();
                                count = 0;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("network error: {:?}", e);
                break;
            }
        }
    }

    println!("\nclosing parquet...");
    writer.close().unwrap();
    println!("done");
}