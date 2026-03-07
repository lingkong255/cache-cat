use crate::network::node::{GroupId, TypeConfig};
use crate::server::core::config::ONE;
use crate::server::core::moka::load_meta_from_path;
use openraft::SnapshotMeta;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

/// 发送硬链接文件到其他节点的辅助结构体。
/// - 创建时会产生硬链接（async 构造函数 try_create ）
/// - drop 时会删除硬链接（同步删除）
/// FileOperator可以直接在内部使用或发送给客户端，但是客户端收到后要修改file_path
#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct FileOperator {
    group_id: GroupId,
    file_path: PathBuf,
    uuid: Uuid,
}

impl FileOperator {
    /// - 如果原文件不存在，返回 Ok(None)
    /// - 否则创建硬链接并返回 Ok(Some(HardlinkSender))
    pub async fn new<P: AsRef<Path>>(
        group_id: GroupId,
        file_path: P,
    ) -> Result<Option<Self>, io::Error> {
        let snapshot_path = file_path
            .as_ref()
            .join("snapshot")
            .join(format!("snapshot_{}.bin", group_id));
        // println!("snapshot_path: {}", snapshot_path.display());
        // 1. 检查文件是否存在
        match fs::metadata(&snapshot_path).await {
            Ok(_) => {
                let operator = Self {
                    group_id,
                    file_path: file_path.as_ref().to_path_buf(),
                    uuid: Uuid::new_v4(),
                };
                // 2. 构造唯一硬链接路径
                let hardlink_path = operator.get_hard_link_buf();
                // 3. 创建硬链接
                fs::hard_link(snapshot_path, &hardlink_path).await?;

                // 4. 返回构造完成的结构体
                Ok(Some(operator))
            }
            // 文件不存在时返回 None
            Err(_) => Ok(None),
        }
    }
    pub fn get_hard_link_buf(&self) -> PathBuf {
        let hardlink_filename = format!("hardlink_snapshot_{}_{}.tmp", self.uuid, self.group_id);
        let hardlink_path = self.file_path.join(hardlink_filename);
        hardlink_path
    }

    //在收到快照后从节点安装的时候会调用这个方法来获得新的硬链接路径
    pub fn get_local_hard_link_buf(&self, path: &PathBuf) -> PathBuf {
        let hardlink_filename = format!("hardlink_snapshot_{}_{}.tmp", self.uuid, self.group_id);
        let hardlink_path = path.join("snapshot").join(hardlink_filename);
        hardlink_path
    }

    /// 发送文件（使用硬链接路径），返回 send_file_once 的结果（成功时返回 Uuid）。
    /// 注意：这里不删除硬链接，删除由 Drop 完成（或手动调用 close）。
    pub async fn send_file(&self, addr: &str) -> Result<Uuid, Box<dyn Error + Send + Sync>> {
        let hardlink_path = self.get_hard_link_buf();
        let uuid = send_file_once(addr, self.group_id as u32, hardlink_path, self.uuid).await?;
        Ok(uuid)
    }
    pub async fn load_meta_data(&self) -> Result<Option<SnapshotMeta<TypeConfig>>, io::Error> {
        load_meta_from_path(self.get_hard_link_buf()).await
    }
}
// 自动删除
impl Drop for FileOperator {
    fn drop(&mut self) {
        // 在 Drop 里不能做 async，所以用同步 std::fs::remove_file。
        // 这里忽略错误（只打印），避免在 drop 时 panic。
        let hardlink_path = self.get_hard_link_buf();

        if let Err(e) = std::fs::remove_file(&hardlink_path) {
            tracing::info!(
                //没有成功删除硬链接（正常现象）
                "HardlinkSender: failed to remove hardlink {}: {}",
                hardlink_path.display(),
                e
            );
        }
    }
}
//发送的时候一定要转u32
pub async fn send_file_once<P: AsRef<Path>>(
    addr: &str,
    group_id: u32,
    file_path: P,
    uuid: Uuid,
) -> Result<Uuid, Box<dyn Error + Send + Sync>> {
    // 连接
    let mut stream = TcpStream::connect(addr).await?;
    // 关闭 Nagle 以降低延迟 / 确保小包快速发出（与服务端一致）
    stream.set_nodelay(true)?;

    // 第一个字节：模式标识，服务端代码中 0 是 RPC，非 0 是 stream
    stream.write_all(&[1u8]).await?;

    // 紧接着发送 4 字节 group_id（big-endian）
    stream.write_all(&group_id.to_be_bytes()).await?;
    //发送uuid
    stream.write_all(uuid.as_bytes()).await?;

    // 打开文件并把文件内容拷贝到 stream
    let mut file = File::open(file_path).await?;
    //零拷贝，直接将文件发送到网络缓冲区
    let bytes_copied = io::copy(&mut file, &mut stream).await?;

    // 刷新并关闭写端，通知服务端
    stream.shutdown().await?;
    //获取返回的文件名（目前没有其他用处）
    let mut buf = [0u8; 16];
    stream.read_exact(&mut buf).await?;
    let uuid = Uuid::from_bytes(buf);
    Ok(uuid)
}

#[tokio::test]
async fn test_send_file_once() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 示例用法（替换为实际地址、group_id、文件路径）
    let group_id = 1;
    let path = "E:/tmp/raft/1.png";
    match send_file_once(ONE, group_id, path, Uuid::new_v4()).await {
        Ok(n) => println!("文件发送完成，字节数：{:?}", n),
        Err(e) => eprintln!("发送失败: {}", e),
    }
    Ok(())
}

#[tokio::test]
async fn test_file_operator() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 示例用法（替换为实际地址、group_id、文件路径）
    let group_id = 1;
    let path = "E:/tmp/raft";
    match FileOperator::new(group_id, path).await {
        Ok(Some(n)) => {
            println!("文件发送完成：{:?}", n.send_file(ONE).await);
        }
        Ok(None) => println!("文件不存在"),
        Err(e) => eprintln!("发送失败: {}", e),
    }
    Ok(())
}
