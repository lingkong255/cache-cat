# raft

## 常见的快照策略

consul，使用go-memdb进行存储，其实现了mvcc机制。zookeeper，将所有操作变成CAS操作，因此后续的请求可以被应用多次而达到相同的数据逻辑。

dragonboat，通过任期手动标识哪些数据是旧的哪些数据是新的，执行完快照逻辑后手动收集变动部分数据。ETCD，通过存储层的数据库来直接实现MVCC快照。Redis 通过linux下的fork指令。

## cache-cat快照策略

要在不停机的情况下实现快照是复杂的。因为raft必须要求快照是一个时间点的统一快照。并且cache-cat在内存中并没有维护mvcc策略。
以下是cache-cat实现快照的说明。
由于raft的状态机是单线程的。快照线程理论上可以并发，但由于通常快照的瓶颈在于磁盘，因此这里只采用一个线程来实现。
此外业务处理线程只会处理修改操作，而不处理读请求。

***

此外，我们将业务map称为cache_map，快照产生的临时map存放老数据称为old_map，此外还有个removed_map用于存放被删除的数据。请求都是一批一批来，一次append操作可能产生上百个key的改动。也可能是一个key的改动。

每一个value保存一个快照编号。以上俩个map均采用moka实现（moka自带了机制实现相同key的：读读，读写并发。）

该快照字段不会被持久化。如进行反序列化默认是0。

除此之外我们还要记录last_applied_log_id（快照产生时最后应用的日志id），last_membership（最后的成员配置，raft自用不用管），snapshot_num（快照编号 u32），snapshot_state（当前快照状态）。这些数据都被一个锁保护统称为meta_data，并且被一个parking_lot::mutex（待讨论）保护起来。

**快照线程**

获取meta_data的锁，如果获取到了就将快照编号自增。并且保存meta_data元数据。释放锁。

迭代读取cache_map所有数据，如果发现读取到了新版本的数据，那么就访问old_map，来获取老的值，如果读取不到就不写入当前值（老值不存在）。

结束时获取meta_data锁，设置快照完成。然后清空old_map（如果锁能获取到，那么就不会产生清空的时候有人写的情况）。

**业务处理线程**：所有的

1. 当有一批新的数据到来，比如key1，key2俩条。通过锁获取meta_data。（一批数据只获取一次锁，等整批数据处理完了然后释放）
2. 如果当前snapshot_state为false。直接将数据写入cache_map结束。
3. 如果当前snapshot状态为true：
   - 当前操作为put：查询现有数据是否存在于cache_map，且是否比当前快照编号小。如果存在且小（代表这个数据是旧的），放到old_map中。然后原地更新到cache_map的值，并将snapshot_state字段更新。
   - 当前操作为remove：查询现有数据是否存在于cache_map，且是否比当前快照编号小。如果存在且小（代表数据是旧的），放到old_map中。然后原地删除cache_map的值。
4. 更新last_applied_log_id
5. 最后释放锁

















