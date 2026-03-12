# 数据结构文档

> 简单起见，使用Redis的基础数据结构作为标题。大部分操作还处于适配开发阶段。

## String

暂时通过 moka进行实现。包括下列数据结构的所有key-value操作，都使用 moka作为基座。

## Hash

通过 Rust自带的hashmap进行实现。如要读写并发，只能使用flurry。

## List

使用 `std::collections::LinkedList` 实现

## Set

无序集合 暂用rust的hashset实现

## Sorted Set
在 Redis中也叫 Zset 。暂时用自带的 BTreeSet来进行实现

## 其他
已知Redis还支持bitmap，hyperloglog，Geospatial，Streams，Bitfield等。暂不未有计划支持。