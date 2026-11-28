# data-qd-236 · SQLite 备份与验证恢复

`snapshot(live, target, verify=...)` 复制已提交的 SQLite 状态，先检查结构完整性，
再通过调用者提供的只读业务检查，最后发布新文件。`recover(archive, target, verify=...)`
执行同样的隔离校验，可用不同的恢复目标检查。失败不发布，不覆盖已有文件；
验证者抛出的异常原样返回。恢复产物是可独立连接查询的 SQLite 数据库。

`VersionedArchive(directory, byte_limit=...)` 是进程内版本化发布库：按数据库页保存
共享内容，在硬字节预算内保留活动版和所有未消费许可引用的旧版本。`publish` 成功
才切换 `active()`；`pin()` 返回一次性许可，`restore` 按许可中的原始版本写出独立
数据库，`release` 主动放弃许可。`usage()` / `last_peak()` 是计费观察值，可在业务
校验回调内读取。计费按完整页的未压缩字节计算；相同页只保存一份、只计一次。
合法恢复写到目标父目录的最终文件及其单个原子发布暂存文件不计入库预算。

Python 3.9+，标准库，使用支持同目录硬链接的本地文件系统。只处理可信目录和
可信校验函数；不提供异地备份、覆盖活库、掉电持久化保证或保留策略。SQLite
复制有三秒进度预算，业务验证由调用者负责。演示使用自制行和临时数据库。

```sh
python3 -m unittest -v
python3 demo.py
```

`test_versioned_archive.py` 用真实 SQLite 文件复现离线验收：A→B、B→C 各改一页
时在 `2S+2P` 预算内上线 C、删除源文件后三个版本仍可独立恢复；候选被业务拒绝
后活动版与许可保持不变、失败恢复可用同一许可重试；A 的未消费许可使第三版
空间不足，释放后重试成功。

样例输出 `newer recovery target refused; no output: True` 与
`recovered committed rows: [(1,)]`：复制时写端尚未提交的第二条记录未进入副本。
