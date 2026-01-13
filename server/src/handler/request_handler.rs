use crate::core::moka::{MyValue, get_cache};
use crate::share::model::{
    DelReq, DelRes, ExistsReq, ExistsRes, GetReq, GetRes, PrintTestReq, PrintTestRes, SetReq,
    SetRes,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fory::Fory;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

type HandlerEntry = (u32, fn() -> Box<dyn RpcHandler>);
static HANDLER_TABLE: &[HandlerEntry] = &[
    (1, || Box::new(RpcMethod { func: print_test })),
    (2, || Box::new(RpcMethod { func: set })),
    (3, || Box::new(RpcMethod { func: get })),
    (4, || Box::new(RpcMethod { func: del })),
    (5, || Box::new(RpcMethod { func: exists })),
];

pub async fn hand(
    mut socket: &mut TcpStream,
    addr: SocketAddr,
    mut package: BytesMut,
    fory: Arc<Fory>,
) -> Result<(), ()> {
    //回显消息,同样添加4 byte的长度头
    let request_id = u32::from_be_bytes(package[0..4].try_into().unwrap());
    let func_id = u32::from_be_bytes(package[4..8].try_into().unwrap());
    package.advance(8);
    //选择对应的方法并调用
    let handler = HANDLER_TABLE
        .iter()
        .find(|(id, _)| *id == func_id)
        .map(|(_, ctor)| ctor())
        .ok_or(())?;

    let response_data = handler.call(&fory, package.freeze());

    let mut response_length = response_data.len() as u32;
    response_length = response_length + 4;

    let mut response_header = BytesMut::with_capacity(8);
    response_header.put_u32(response_length);
    response_header.put_u32(request_id);
    if let Err(e) = socket.write_all(&response_header).await {
        eprintln!("发送响应头失败 ({}): {}", addr, e);
        return Err(());
    }
    if let Err(e) = socket.write_all(&*response_data).await {
        eprintln!("发送响应数据体失败 ({}): {}", addr, e);
        return Err(());
    }
    Ok(())
}

trait RpcHandler: Send + Sync {
    fn call(&self, fory: &Fory, data: Bytes) -> Bytes;
}
struct RpcMethod<Req, Res> {
    func: fn(Req) -> Res,
}

impl<Req, Res> RpcHandler for RpcMethod<Req, Res>
where
    Req: Send + 'static + fory::ForyDefault + fory::Serializer,
    Res: Send + 'static + fory::Serializer,
{
    fn call(&self, fory: &Fory, data: Bytes) -> Bytes {
        let req: Req = fory.deserialize(data.as_ref()).unwrap();
        let res = (self.func)(req);
        fory.serialize(&res).unwrap().into()
    }
}

fn print_test(d: PrintTestReq) -> PrintTestRes {
    println!("{}", d.message);
    PrintTestRes { message: d.message }
}

fn set(req: SetReq) -> SetRes {
    let cache = get_cache();
    let v = MyValue {
        data: Arc::new(req.value),
        ttl_ms: req.ex_time,
    };
    cache.insert(req.key, v);
    SetRes {}
}

fn get(req: GetReq) -> GetRes {
    let cache = get_cache();
    let a = cache.get(&req.key);
    //为避免空指针，返回Option
    GetRes {
        value: a.map(|v| v.data.clone()),
    }
}

fn del(req: DelReq) -> DelRes {
    let cache = get_cache();
    match cache.remove(&req.key) {
        None => DelRes { num: 0 },
        Some(_) => DelRes { num: 1 },
    }
}
fn exists(req: ExistsReq) -> ExistsRes {
    let cache = get_cache();
    if cache.contains_key(&req.key) {
        ExistsRes { num: 1 }
    } else {
        ExistsRes { num: 0 }
    }
}
