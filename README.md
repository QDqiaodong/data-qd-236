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

`test_media_backup.py` 用真实 WAL 活库（另有未提交订阅）复现三卷灾备验收：
发布后校验只看到已提交路由与订阅、候选只读，并独立量出 S 与三卷大小核对上限；
校验拒绝与回调异常都不留卷。随后在等长单字节损坏、截断、跨槽位搬运和外来备份卷
下核对巡检只报坏槽，恢复仍呈现原状态；拒绝一次恢复后同一目标重试成功；补卷返回
槽位号、原坏卷与健康卷字节不变、换入后原描述符巡检全健康、再拿走另一健康卷仍能
读出原路由与订阅；子进程把单文件上限设成 `ceil(3S/4)` 证明补卷不写整库。最后
逐一移走每个槽位验证并非只保护固定槽位，并在双坏卷下确认巡检报告全部坏槽、
恢复与补卷均报 `insufficient_volumes`、不留产物、不执行校验。发布源删除后，另起
`python -I` 进程仅凭重新打开的描述符与卷路径完成恢复。

样例输出 `newer recovery target refused; no output: True` 与
`recovered committed rows: [(1,)]`：复制时写端尚未提交的第二条记录未进入副本。

## 三卷离线灾备与单卷自修复

`publish_media(live, volumes, verify=...)` 从活库捕获**一份**已提交 SQLite 状态
（支持 WAL；其他连接未提交的行不会被纳入），在三份独立介质上按 2-of-3 纠删布局
分卷：每 64 字节条带拆成前后两半，槽位 0/1 各存一半，槽位 2 存两者异或。任意
**两卷**即可逐条带重建原始数据库字节，第三卷也可由另两卷逐条带重建，因此不需要
在内存或磁盘上先拼一份整库。返回的 `bytes` 描述符只含识别、布局与摘要，可独立
保管，不含任何业务行。每卷文件长度都有硬上限 `ceil(3S/4)`（S 是捕获到的独立
SQLite 文件字节数，支持 128 KiB–8 MiB）。

三个函数分工明确，**灾备恢复**与**介质补卷**是两项操作：

- `recover_media(descriptor, volumes, target, verify=...)` 用当前可用的两卷生成
  一份独立、可查询的 SQLite 文件。缺一卷仍成功；坏卷数据绝不混入；先过 SQLite
  结构检查，`verify` 只拿到该候选的只读连接。已有目标、校验拒绝、回调异常都
  不会留下目标文件。
- `repair_media(descriptor, volumes, replacement)` 只补**一个坏槽位**：无坏卷返回
  `None` 且不建文件；恰有一卷不可用时在 `replacement`（必须尚不存在）写出该槽位
  的可替换卷并返回槽位号。原三个路径只读不改，也不需要先做一次整库恢复。
- `inspect_media(descriptor, volumes)` 只读巡检，按槽位返回不可用元组；`None`
  表示该卷已遗失。截断、等长改写、跨槽位搬运、来自别的备份的卷都会被判为不可用
  （校验长度、槽位头与整卷摘要，不只看文件头或长度）。

`volumes` 的三项按槽位 0、1、2 排列；卷路径可在移交时改变。补卷过程中只写
`replacement` 或同父目录的单个发布暂存，不会生成整库中转。

### 小型本地示例

```python
from pathlib import Path
from recovery import (
    MediaBackupError,
    inspect_media,
    publish_media,
    recover_media,
    repair_media,
)

vols = [Path("/media/vol0.bin"), Path("/media/vol1.bin"), Path("/media/vol2.bin")]

# 1) 发布：从活库捕获已提交状态，分别写到三份介质
descriptor = publish_media(
    "live.db",
    vols,
    verify=lambda con: con.execute("PRAGMA foreign_key_check").fetchall() == [],
)
Path("descriptor.bin").write_bytes(descriptor)  # 单独保管，可与卷分处异地

# 2) 恢复：任意两卷可用即可（遗失卷用 None 表示）
print(inspect_media(descriptor, [vols[0], None, vols[2]]))  # (1,)
recover_media(
    descriptor,
    [vols[0], None, vols[2]],
    "recovered.db",
    verify=lambda con: con.execute("SELECT count(*) FROM routes").fetchone()[0] > 0,
)

# 3) 补卷：用原描述符，把坏卷替换成新文件；无需改动描述符
try:
    slot = repair_media(descriptor, [vols[0], vols[1], vols[2]], "vol1-new.bin")
except MediaBackupError as exc:
    print(exc.reason)
else:
    if slot is not None:  # 补成功：把 vol1-new.bin 换入该槽位
        vols[slot] = Path("vol1-new.bin")
    print(inspect_media(descriptor, vols))  # 三槽全部健康时为 ()
```

坏卷的处置流程：先 `inspect_media` 确认到底坏了哪一槽（只报坏槽，不猜）；只要
坏槽不超过一个，就用 `repair_media` 写到新路径，再把它物理换入对应槽位；原描述符
不改，换入后巡检应为 `()`，此时再拿走任一原本健康的卷仍可 `recover_media`。

**何时必须停止恢复**：巡检报告**两个或更多**槽位不可用时，无论 `recover_media`
还是 `repair_media` 都会以 `MediaBackupError.reason == "insufficient_volumes"`
拒绝，不产生任何输出、也不调用业务校验；此时不能再靠这套介质兑现灾备，应从另一
份独立备份重来，而不是尝试拼凑或修补损坏数据。格式无效的描述符和非法参数抛
`ValueError`，其他 I/O 错误原样传播。
