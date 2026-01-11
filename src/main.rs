// mft pro 24h silent mode

// import
use futures_util::StreamExt;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use sysinfo::{System, Pid};
use chrono::prelude::*;
use async_compression::tokio::write::ZstdEncoder;

#[derive(Debug, Deserialize)]
struct DepthUpdate {
    #[serde(rename = "E")] event_time: u64,
    #[serde(rename = "b")] bids: Vec<Vec<String>>,
    #[serde(rename = "a")] asks: Vec<Vec<String>>,
}

fn price_to_u64(price_str: &str) -> u64 {
    let price_f64: f64 = price_str.parse().unwrap_or(0.0);
    (price_f64 * 100_000_000.0) as u64 
}

fn qty_to_f64(qty_str: &str) -> f64 {
    qty_str.parse().unwrap_or(0.0)
}

#[tokio::main]
async fn main() {
    let url = "wss://fstream.binance.com/ws/btcusdt@depth"; 
    let log_dir = "data_logs";
    
    // settings
    let file_rotation_interval = Duration::from_secs(24 * 60 * 60); // 24h
    let snapshot_interval = Duration::from_millis(20);              // 50hz
    
    create_dir_all(log_dir).await.expect("dir create error");
    let mut sys = System::new_all();
    let pid = Pid::from(std::process::id() as usize);
    let start_time = Instant::now();

    let mut next_snapshot = Instant::now() + snapshot_interval;
    let mut next_rotation = Instant::now() + file_rotation_interval;
    
    // track full hours
    let mut last_reported_hour: u64 = 0;

    let mut local_bids: BTreeMap<u64, f64> = BTreeMap::new();
    let mut local_asks: BTreeMap<u64, f64> = BTreeMap::new();

    println!("--- MFT PRO: 24H SILENT MODE ---");
    println!(">>> Cel: BTCUSDT | Snapshot: 20ms | Rotacja: 24h");
    println!(">>> Logi: Tylko rotacja i status godzinowy (1/24...)");

    let (ws_stream, _) = connect_async(url).await.expect("connection failed");
    let (_, mut read) = ws_stream.split();

    async fn create_new_encoder(dir: &str) -> ZstdEncoder<tokio::fs::File> {
        let now: DateTime<Local> = Local::now();
        let filename = format!("{}/mft_24h_{}.csv.zst", dir, now.format("%Y-%m-%d")); 
        println!("\n>>> [SYSTEM START] creating file: {}", filename);
        
        let file = OpenOptions::new().create(true).append(true).open(filename).await.expect("file error");
        let mut encoder = ZstdEncoder::new(file);
        
        let mut header = String::from("timestamp,latency");
        for i in 0..20 { header.push_str(&format!(",bid_p{},bid_q{}", i, i)); }
        for i in 0..20 { header.push_str(&format!(",ask_p{},ask_q{}", i, i)); }
        header.push('\n');
        let _ = encoder.write_all(header.as_bytes()).await;
        
        encoder
    }

    let mut current_encoder = create_new_encoder(log_dir).await;

    while let Some(message) = read.next().await {
        if let Ok(Message::Text(text)) = message {
            if let Ok(update) = serde_json::from_str::<DepthUpdate>(&text) {
                
                // 1. update lob
                for b in update.bids {
                    let price = price_to_u64(&b[0]);
                    let qty = qty_to_f64(&b[1]);
                    if qty == 0.0 { local_bids.remove(&price); } else { local_bids.insert(price, qty); }
                }
                for a in update.asks {
                    let price = price_to_u64(&a[0]);
                    let qty = qty_to_f64(&a[1]);
                    if qty == 0.0 { local_asks.remove(&price); } else { local_asks.insert(price, qty); }
                }

                // 2. file rotation (24h)
                if Instant::now() >= next_rotation {
                    println!("\n>>> [ROTATION 24H] closing old, opening new...");
                    let _ = current_encoder.shutdown().await;
                    current_encoder = create_new_encoder(log_dir).await;
                    next_rotation = Instant::now() + file_rotation_interval;
                    
                    // reset hour counter (optional)
                    // last_reported_hour = 0; 
                }

                // 3. save snapshot
                if Instant::now() >= next_snapshot {
                    let bids_l20: Vec<_> = local_bids.iter().rev().take(20).collect();
                    let asks_l20: Vec<_> = local_asks.iter().take(20).collect();

                    if bids_l20.len() >= 20 && asks_l20.len() >= 20 {
                        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                        let mut line = format!("{},{}", now_ms, now_ms.saturating_sub(update.event_time));

                        for (price_u64, qty_f64) in bids_l20 { 
                            let p_str = format!("{:.2}", *price_u64 as f64 / 100_000_000.0);
                            line.push_str(&format!(",{},{:.8}", p_str, qty_f64)); 
                        }
                        for (price_u64, qty_f64) in asks_l20 { 
                            let p_str = format!("{:.2}", *price_u64 as f64 / 100_000_000.0);
                            line.push_str(&format!(",{},{:.8}", p_str, qty_f64)); 
                        }
                        line.push('\n');

                        let _ = current_encoder.write_all(line.as_bytes()).await;
                        next_snapshot += snapshot_interval;
                    }
                }

                // 4. hourly status
                let elapsed_secs = start_time.elapsed().as_secs();
                let current_hour = elapsed_secs / 3600;

                // log only on new hour (skip start)
                if current_hour > last_reported_hour {
                    sys.refresh_all();
                    if let Some(proc) = sys.process(pid) {
                        use std::io::Write;
                        println!(">>> [STATUS] uptime: {}/24h | ram: {} mb", 
                            current_hour, 
                            proc.memory() / 1024 / 1024
                        );
                    }
                    last_reported_hour = current_hour;
                }
            }
        }
    }
}