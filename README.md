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

`relay_drain.py` 只依赖标准库，仅处理可信本地文件，公开调用按串行约定使用；
不提供并行压力测试，也不模拟中继故障。新增用例在 `test_relay_drain.py`。

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

## 中继下线前的本地疏散规划与接驳控制

`relay_drain` 在现有活库上增加一套**离线规划 + 实际接驳**接口，不发送任何网络
流量；对端是否就绪由运维调用方在 `finish_handoff` 里回报。数据面以
`drain_status()` 返回的**实际服务地址**决定向哪台中继发送，配置文件里的主路由
或规划结果都不代表已经生效的接驳。路由表、订阅表及其外键沿用不动；维护状态存在
同库的 `relay_drain_*` 私有表中，重开 Python 连接后仍可查询和继续操作。

- `configure_drain(live, capacities=..., retiring=..., fallbacks=..., serving=...)`
  在未配置的库建立唯一维护任务，成功返回 `None`。`capacities` 是**物理地址**到
  整数容量的映射（多路由指向同一 `destination` 共用一份容量）；`retiring` 是待
  下线地址；`fallbacks` 和 `serving` 必须覆盖每条订阅，前者是稳定的备用 `route_id`
  序列，后者是当前真实服务地址。初始服务地址必须落在该订阅主/备路由可达范围内且
  不超容量。重复建立报 `already_configured`，不重置既有状态。
- `drain_status(live)` 返回 `(serving, pending)`：前者是订阅 ID 到数据面**当前**
  应使用地址的字典；后者是 `(subscription_id, destination)` 或 `None`。全库最多
  一项待确认接驳，此时新旧两地址各占一个位置，但数据面只向原地址发送。
- `plan_drain(live)` 从已提交状态给出**一整条**可依次执行的
  `(subscription_id, destination)` 元组序列，每步假设配置不再变、对端就绪，并在
  下一步开始时按容量重算（目标必须先有空位，不能先释放自己再换）。它会为受阻的
  订阅搬移**不在下线中继上**的占位订阅。已疏散完成返回 `()`，确实无法疏散返回
  `None`；只读、不预留，存在待确认项报 `handoff_pending`。
- `begin_handoff(live, subscription_id, destination)` 按**当前实况**校验并预留目标
  位置，不改实际服务地址。目标是当前服务地址、待下线地址或不在当前允许范围报
  `not_eligible`；符合资格但没有物理空位报 `no_capacity`；已有待确认项报
  `handoff_pending`。可以执行计划中的一步，也可以执行任何合法一步。
- `finish_handoff(live, subscription_id, ready=...)` 只处理该订阅的待确认项。没有
  对应项报 `no_pending`；`ready=False` 释放预留、保留原地址；`ready=True` 时**重新
  读取当前允许范围**：预留目标已不符合资格则以 `target_changed` 拒绝切换并释放
  预留，原地址继续服务；仍符合资格才切换实际地址、清空待确认项并释放原位置，作为
  一次对外不可分割的提交。

业务错误统一用 `DrainError(ValueError)` 的 `reason` 表达（未配置报
`not_configured`）；非法参数（不存在的 ID、未登记地址、非布尔的 `ready`）抛普通
`ValueError`；底层 SQLite 错误原样传播。除 `target_changed` 明确要求释放预留外，
失败不留下部分变化。

```python
from relay_drain import (
    DrainError,
    begin_handoff,
    configure_drain,
    drain_status,
    finish_handoff,
    plan_drain,
)

# 1) 建立维护任务（一次）
configure_drain(
    "live.db",
    capacities={"A": 1, "B": 1, "C": 1, "D": 1},
    retiring="A",
    fallbacks={101: [20], 202: [30], 303: [40]},
    serving={101: "A", 202: "B", 303: "C"},
)

# 2) 试算：只读，不改变现况
plan = plan_drain("live.db")
if plan is None:
    ...  # 确实无法疏散：需要人工处理
else:
    for subscription_id, destination in plan:
        # 3) 预留并等待运维确认对端就绪
        begin_handoff("live.db", subscription_id, destination)
        serving, pending = drain_status("live.db")
        assert serving[subscription_id] != destination  # 仍走原地址
        try:
            finish_handoff("live.db", subscription_id, ready=True)
        except DrainError as exc:
            if exc.reason == "target_changed":
                # 预留目标在确认前失去资格：预留已释放、原地址继续服务。
                # 不要复用旧计划，重新 plan_drain 再执行。
                plan = plan_drain("live.db")
                break
            raise
```

**遇到未就绪或配置改变时**：`finish_handoff(..., ready=False)` 会保留原服务地址
并释放预留，同一步可以直接重试 `begin_handoff`；若现有路由修复改动了备用路由的
目标，旧计划与旧预留都不再具有授权，必须以 `target_changed` 处理后重新
`plan_drain`。规划不预留位置，并发/串行调用之间配置若变化，每一步的
`begin_handoff` 都会按当时的真实范围与容量重新判定。

`test_relay_drain.py` 用真实 SQLite 复现两组验收：四台单容量中继 A/B/C/D、
101/202/303 分别在 A/B/C，主路由 10/20/30、备用 20/30/40，停 A——直接搬
101→B 报 `no_capacity`，规划的唯一次序是 303→D、202→C、101→B，逐步预留时第二
连接仍读到原地址且不能并行开始另一项，就绪后读到新址，结束时计划为空，独立 SQL
证明主路由绑定未变；第二步先报未就绪再同一步重试，随后用既有
`plan_route_repair`/`apply_route_repair` 把路由 40 的目标从 D 改到 B，相同就绪
回报以 `target_changed` 拒绝、303 仍在 C，此时重新规划返回 `None`（D 空着但没人
可达，B/C 也不能互换），直接搬 303→D 报 `not_eligible`；再用旧修复入口改回 D
后重走三步完成下线。两组容量、订阅与初始接驳完全一致。

