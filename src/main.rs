use chrono::{DateTime, Local, Utc};
use futures::stream;
use influxdb2::models::DataPoint;
use influxdb2::Client;
use influxdb2_derive::WriteDataPoint;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let (tx, rx) = mpsc::channel(32);

    // データ生成タスクをスポーン
    let generate_task = tokio::spawn(generate_data(tx));

    // データ送信タスクをスポーン
    let send_task = tokio::spawn(send_data(rx));

    // 両方のタスクが終了するまで待機
    let _ = tokio::join!(generate_task, send_task);

    // 他の処理
    Ok(())
}

#[derive(Default, WriteDataPoint)]
#[measurement = "temp_sensor_data"]
struct TempSensorData {
    #[influxdb(tag)]
    unit: Option<String>,
    #[influxdb(field)]
    sensor_1: f64,
    #[influxdb(field)]
    sensor_2: f64,
    #[influxdb(field)]
    sensor_3: f64,
    #[influxdb(timestamp)]
    time: i64,
}

async fn generate_data(tx: mpsc::Sender<Vec<TempSensorData>>) -> anyhow::Result<()> {
    // データ生成処理
    // 50msごとにデータを生成してチャンネルに送信
    // 200データ⇒10s毎にtxに送信×60⇒10分分のデータ
    let mut field1 = 50.0;
    let mut field2 = 50.0;
    let mut field3 = 50.0;
    let mut next_loop_start_time = Instant::now();

    for _ in 0..60 {
        let mut points: Vec<TempSensorData> = Vec::<TempSensorData>::new();
        for _ in 0..200 {
            next_loop_start_time += Duration::from_millis(50);

            // let dt: DateTime<Local> = Local::now();
            // let timestamp: i64 = dt.timestamp();
            // let point = DataPoint::builder("stat")
            //     .tag("unit", "temperature")
            //     .field("field1", field1)
            //     .field("field2", field2)
            //     .field("field3", field3)
            //     .timestamp(timestamp)
            //     .build()?;
            let point = TempSensorData {
                unit: Some("machine_1".to_owned()),
                sensor_1: field1,
                sensor_2: field2,
                sensor_3: field3,
                time: Local::now().timestamp_nanos_opt().unwrap(),
            };
            points.push(point);

            field1 += (rand::random::<i32>() % 200 - 100) as f64 / 10.0;
            field2 += (rand::random::<i32>() % 200 - 100) as f64 / 10.0;
            field3 += (rand::random::<i32>() % 200 - 100) as f64 / 10.0;

            let now = Instant::now();
            if next_loop_start_time > now {
                tokio::time::sleep(next_loop_start_time - now).await;
            }
        }

        tx.send(points).await?;
    }

    Ok(())
}

async fn send_data(mut rx: mpsc::Receiver<Vec<TempSensorData>>) -> anyhow::Result<()> {
    // クライアント設定
    let host = std::env::var("INFLUXDB_HOST").unwrap();
    let org = std::env::var("INFLUXDB_ORG").unwrap();
    let token = std::env::var("INFLUXDB_TOKEN").unwrap();
    let bucket = std::env::var("INFLUXDB_BUCKET").unwrap();

    let client = Client::new(host, org, token);

    // データ送信処理

    while let Some(points) = rx.recv().await {
        let dt: DateTime<Local> = Local::now();

        println!("{:?}:receive {:?} data", dt, points.len());

        let result = client.write(&bucket, stream::iter(points)).await;

        match result {
            Ok(()) => {}
            Err(r) => {
                println!("{:?}", r)
            }
        }
    }

    Ok(())
}

// async fn _bing_fn() {
//     let host = std::env::var("INFLUXDB_HOST").unwrap();
//     let org = std::env::var("INFLUXDB_ORG").unwrap();
//     let token = std::env::var("INFLUXDB_TOKEN").unwrap();
//     let bucket = "bucket"; // Specify your bucket name

//     let client = Client::new(host, org, token);

//     let mut field1 = 50.0;
//     let mut field2 = 50.0;
//     let mut field3 = 50.0;

//     // 100ms毎にデータを生成
//     // 5s毎に纏めてデータを送信
//     //
//     for _ in 0..180 {
//         let point = DataPoint::builder("stat")
//             .tag("unit", "temperature")
//             .field("field1", field1)
//             .field("field2", field2)
//             .time(DateTime::from(Utc::now()))
//             .build()
//             .unwrap();

//         client.write(bucket, vec![point]).await.unwrap();

//         field1 += (rand::random::<i32>() % 200 - 100) as f64 / 10.0;
//         field2 += (rand::random::<i32>() % 200 - 100) as f64 / 10.0;
//         field3 += (rand::random::<i32>() % 200 - 100) as f64 / 10.0;

//         tokio::time::sleep(Duration::from_secs(10)).await;
//     }
// }
