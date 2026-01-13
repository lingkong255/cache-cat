use bytes::{Buf, BufMut, Bytes, BytesMut};
use fory_core::Fory;
use server::share::model::{GetReq, GetRes, PrintTestReq, PrintTestRes, SetReq, SetRes};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
// =======================
// 模拟 share::model
// =======================

struct RpcClient {
    stream: TcpStream,
    fory: Fory,
    next_request_id: u32,
}

impl RpcClient {
    async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;

        let mut fory = Fory::default();
        // 注册类型（必须与服务端一致）
        fory.register::<PrintTestReq>(1)?;
        fory.register::<PrintTestRes>(2)?;
        fory.register::<SetReq>(3)?;
        fory.register::<SetRes>(4)?;
        fory.register::<GetReq>(5)?;
        fory.register::<GetRes>(6)?;
        Ok(Self {
            stream,
            fory,
            next_request_id: 1,
        })
    }

    async fn call<Req, Res>(
        &mut self,
        func_id: u32,
        req: Req,
    ) -> Result<Res, Box<dyn std::error::Error>>
    where
        Req: fory_core::Serializer,
        Res: fory_core::Serializer + fory_core::ForyDefault,
    {
        let request_id = self.next_request_id;
        self.next_request_id += 1;

        // ---------- 序列化请求 ----------
        let payload = self.fory.serialize(&req)?;
        let length = (4 + 4 + payload.len()) as u32;

        let mut buf = BytesMut::with_capacity(12 + payload.len());
        buf.put_u32(length); // frame length
        buf.put_u32(request_id); // request id
        buf.put_u32(func_id); // func id
        buf.extend_from_slice(&payload);

        // ---------- 发送 ----------
        self.stream.write_all(&buf).await?;

        // ---------- 接收响应头 ----------
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        // ---------- 接收响应体 ----------
        let mut resp_buf = BytesMut::with_capacity(resp_len);
        resp_buf.resize(resp_len, 0);
        self.stream.read_exact(&mut resp_buf).await?;

        let mut resp_buf: Bytes = resp_buf.freeze();

        // ---------- 解析响应 ----------
        let resp_request_id = resp_buf.get_u32();
        if resp_request_id != request_id {
            return Err("request_id mismatch".into());
        }

        let res: Res = self.fory.deserialize(resp_buf.as_ref())?;
        Ok(res)
    }
}

// =======================
// main：像 RPC 一样调用
// =======================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RpcClient::connect("127.0.0.1:8080").await?;

    let res2: SetRes = client
        .call(
            2,
            SetReq {
                key: "key".to_string(),
                value: Vec::from("value".to_string()),
                ex_time: 1000000,
            },
        )
        .await?;
    // let res2: SetRes = client
    //     .call(
    //         2,
    //         SetReq {
    //             key: "key".to_string(),
    //             value: Vec::from("value".to_string()),
    //             ex_time: 1,
    //         },
    //     )
    //     .await?;
    thread::sleep(Duration::from_secs(2));

    let res3: GetRes = client
        .call(
            3,
            GetReq {
                key: "key".to_string(),
            },
        )
        .await?;
    match res3 {
        GetRes {
            value: Some(arc_vec),
        } => {
            // 使用 from_utf8_lossy 处理无效的 UTF-8 序列
            let s = String::from_utf8_lossy(&arc_vec);
            println!("{}", s);
        }
        GetRes { value: None } => {
            println!("No value");
        }
    }

    Ok(())
}
