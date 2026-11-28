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

## 用恢复出的备份做活库局部路由修复

`recover` 可以把已验证的备份恢复成一个独立 SQLite 文件；`plan_route_repair(backup,
live, route_ids)` 再以该文件为修复来源，在原活库内预览局部修复，预览只读两个库、
不写任何数据。返回的计划是不可变对象，`plan.changes` 是按 `route_id` 排序的
`(route_id, 当前别名, 当前目标, 计划别名, 计划目标)` 元组（含未变化的选中记录）。
备份里的订阅不参与修复；计划别名被范围外路由持有时整单拒绝
（`RouteRepairError.reason == "alias_conflict"`），选中 id 在任一库缺失报
`"missing_route"`，id 非法（非整数、重复、数量不在 1–100）抛普通 `ValueError`。

`apply_route_repair(live, plan, verify=...)` 在**同一个活库写事务**内先复核每个
选中路由的当前别名/目标和每个计划别名（含预览时空闲的别名）的占用者，再执行修改，
因此只容忍订阅变化或无关路由变化；任何观察到的漂移、以及把计划用于另一个活库文件，
都在落盘前以 `reason == "stale_plan"` 拒绝。别名交换靠临时别名打破环，外键的
`ON UPDATE CASCADE` 让每条订阅继续跟随它提交前绑定的 `route_id`，预览后新增的
订阅同理；范围外路由和订阅完全不动。应用时不再打开备份，备份文件可以已经移走。

`verify` 在最终拟提交状态上调用一次，收到同一事务内可执行查询的只读连接：返回
`True` 才提交，其他返回值抛 `InvalidSnapshot`，回调自身抛出的异常原样传播。任何
失败都整体回滚，外部连接只能看到修复前或完整提交后的状态（WAL 下修复事务期间
第二个连接仍读到修复前数据）；回调失败且前置状态未变时，同一计划可原样重试。

```python
from recovery import (
    InvalidSnapshot,
    RouteRepairError,
    apply_route_repair,
    plan_route_repair,
)

# 1) 预览：只读，不写备份也不写活库
try:
    plan = plan_route_repair("recovered-backup.db", "live.db", [10, 20, 40])
except RouteRepairError as exc:
    if exc.reason == "alias_conflict":
        ...  # 计划别名被范围外路由占用：扩大修复范围或人工处理
    raise  # "missing_route"：选中 id 在某个库中不存在

for route_id, cur_alias, cur_dest, new_alias, new_dest in plan.changes:
    print(route_id, cur_alias, cur_dest, "->", new_alias, new_dest)

# 2) 确认：单一写事务，先复核再修改，verify 看到最终拟提交状态
def verify(con):
    return con.execute("PRAGMA foreign_key_check").fetchall() == []

try:
    apply_route_repair("live.db", plan, verify=verify)
except RouteRepairError as exc:
    if exc.reason == "stale_plan":
        # 活库在预览后发生了相关变化（含计划别名被范围外路由占用）。
        # 旧计划不可复用：用当前备份重新预览，再应用新计划。
        plan = plan_route_repair("recovered-backup.db", "live.db", [10, 20, 40])
        apply_route_repair("live.db", plan, verify=verify)
    else:
        raise
except InvalidSnapshot:
    # 校验返回了非 True：事务已整体回滚，状态与应用前完全一致，
    # 修好校验函数后可用同一个 plan 直接重试。
    apply_route_repair("live.db", plan, verify=verify)
```

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

`test_route_repair.py` 用 WAL 活库和经 `recover` 产出的独立备份复现路由修复
验收：10/20 互换别名、40 改到空闲别名（以及 40 仅修目标两种备份变体）下原有及
预览后新增的订阅都跟随原 `route_id`，范围外路由的外部更新保留，两次提交后均
通过 `PRAGMA foreign_key_check`；范围外路由抢占计划别名时旧计划以 `stale_plan`
拒绝且不触发校验；校验回调在最终拟提交状态内确认修复关联、另一连接同时仍读到
修复前状态，回调抛错后原状态完整保留且同一计划可重试成功。

样例输出 `newer recovery target refused; no output: True` 与
`recovered committed rows: [(1,)]`：复制时写端尚未提交的第二条记录未进入副本。
