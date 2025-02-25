# MiniLsm

简介：MiniLsm是一个LSM树实现，用Zig编写而成, 参考了[mini-lsm](https://skyzh.github.io/mini-lsm/00-preface.html)这个项目。

## 快速上手

1. 下载源码：
   ```bash
   git clone https://github.com/tt67wq/mini-lsm.git
   cd mini-lsm
   ```
2. 编译：
   ```bash
   zig build -Denable-demo=true
   ```
3. 运行Demo：
   ```bash
   ./zig-out/bin/demo
   ```

## 代码结构

```markdown
.
├── .gitignore
├── README.md
├── build.zig
├── build.zig.zon
├── c_src
│   └── swal.c
├── include
│   └── swal.h
├── src
│   ├── FileObject.zig
│   ├── MemTable.zig
│   ├── MergeIterators.zig
│   ├── MiniLsm.zig
│   ├── WAL.zig
│   ├── block.zig
│   ├── bloom_filter
│   │   ├── BitArray.zig
│   │   └── BloomFilter.zig
│   ├── bound.zig
│   ├── compact.zig
│   ├── iterators.zig
│   ├── lru.zig
│   ├── main.zig
│   ├── skiplist.zig
│   ├── smart_pointer.zig
│   ├── ss_table.zig
│   └── storage.zig
```

## LICENSE

该项目使用MIT LICENSE协议授权，请参考LICENSE文件了解详细信息。

## < Beats Headphone Emoji >

⭐ 如果你觉得这个项目对你有帮助，请给我们一个star！ ⭐
