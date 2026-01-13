use bytes::{Buf, BytesMut};
use server::core::config::{get_config, init_config};
use server::core::moka::init_cache;
use server::handler::request_handler::hand;
use server::share::model::fory_init;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_config("./server/config.yml")?;
    init_cache();
    let config = get_config();
    let addr = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on: {}", listener.local_addr()?);
    let fory = fory_init()?;
    loop {
        let Ok((mut socket, addr)) = listener.accept().await else {
            eprintln!("接受连接失败");
            continue;
        };
        println!("接收到来自 {} 的新连接", addr);
        let fory = Arc::clone(&fory);
        //为每个连接生成异步任务
        tokio::spawn(async move {
            let mut buffer = BytesMut::with_capacity(1024); //缓冲区
            loop {
                if buffer.len() < 4 {
                    if let Err(e) = socket.read_buf(&mut buffer).await {
                        eprintln!("读取长度头失败 ({}): {}", addr, e);
                        break;
                    }
                    continue; //直到读到4字节
                }
                let data_length = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                buffer.advance(4); //将长度头从缓冲区消费掉
                // 根据长度头读取数据体
                while buffer.len() < data_length as usize {
                    if let Err(e) = socket.read_buf(&mut buffer).await {
                        eprintln!("读取数据体失败 ({}): {}", addr, e);
                        break;
                    }
                }
                //处理完整的数据包
                let data_packet = buffer.split_to(data_length as usize); //分割出数据体
                if let Err(_) = hand(&mut socket, addr, data_packet, Arc::clone(&fory)).await {
                    eprintln!("处理请求失败 {}", addr);
                    return;
                }
            }
        });
    }
}
