# data-qd-236 · SQLite 备份与验证恢复

`snapshot(live, target, verify=...)` 复制已提交的 SQLite 状态，先检查结构完整性，
再通过调用者提供的只读业务检查，最后发布新文件。`recover(archive, target, verify=...)`
执行同样的隔离校验，可用不同的恢复目标检查。失败不发布，不覆盖已有文件；
验证者抛出的异常原样返回。恢复产物是可独立连接查询的 SQLite 数据库。

Python 3.9+，标准库，使用支持同目录硬链接的本地文件系统。只处理可信目录和
可信校验函数；不提供异地备份、覆盖活库、掉电持久化保证或保留策略。
SQLite 复制有三秒进度预算，业务验证由调用者负责。演示使用自制行和临时数据库。

```sh
python3 -m unittest -v
python3 demo.py
```

样例输出 `newer recovery target refused; no output: True` 与
`recovered committed rows: [(1,)]`：复制时写端尚未提交的第二条记录未进入副本。
