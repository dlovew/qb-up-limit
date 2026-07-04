# EMBY Lucky 连接裁决与边界处理 — 技术文档

> 本文档基于当前代码库（v1.71）整理，描述 **Lucky 反代模式** 下 Emby 外网流量统计的完整落地方案：数据来源、裁决算法、流量分账、播放段/选片段生命周期，以及各类边界与异常处理。

---

## 1. 核心目标

本程序在 Lucky 反代模式下，按 **Emby 用户** 统计其外网访问产生的 **真实上传流量**，并区分：

| 流量类型 | 含义 | 落库形态 |
|----------|------|----------|
| **选片流量** | 用户浏览、选片、缓冲前等非稳定播放阶段的上传 | 选片累加器 → 边沿结算 → `emby_browse_upload_facts` |
| **播放流量** | 用户开始播放后，推流阶段的上传 | 播放累加器 → 播放段结案 → `emby_playback_upload_facts` |

两路数据源：

- **Lucky**：`accessdetail` API，提供 **连接级** `TrafficOut` / `AcceptTime` / `RemoteAddr`
- **Emby**：`/Sessions` API，提供 **用户、会话 ID、播放状态、设备、媒资** 等

程序通过 **裁决算法** 将 Lucky 连接归属到 Emby 外网会话，再写入对应的累加器键（persist key）。

---

## 2. 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                     EmbyInstanceWorker._tick                     │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
  Emby /Sessions                  Lucky accessdetail
  (用户/状态/设备)                 (IP/连接/流量累计)
         │                              │
         ▼                              ▼
  playback_record_store          emby_lucky (增量计算)
  (播放段 open/结案)              calc_ip/conn_traffic_deltas
         │                              │
         │         enrich playback_started_at
         └──────────────┬───────────────┘
                        ▼
              emby_lucky_verdict.analyze_lucky_connections
              (波次聚类 · 评分 · 角色 · 入账状态)
                        │
                        ▼
              emby_playback_traffic.accumulate_wan_upload_by_conn
              (播放桶 / 选片桶 / 绑定记忆 / IP 对账)
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
   browse_upload_settler.tick   annotate_live_sessions_upload
   (选片边沿结算)               (UI 实时展示)
            │
            ▼
   emby_traffic_db (SQLite 持久化)
```

### 2.1 关键模块

| 模块 | 文件 | 职责 |
|------|------|------|
| Lucky API | `app/emby_lucky.py` | 拉取 `accessdetail`、IP/连接累计→增量、WAN 连接展开 |
| 连接裁决 | `app/emby_lucky_verdict.py` | 波次聚类、连接↔会话评分、角色与入账状态 |
| 流量分摊 | `app/emby_playback_traffic.py` | 播放/选片累加器、连接分摊、绑定粘性、开播突发回收 |
| 选片结算 | `app/browse_upload_settler.py` | 选片桶边沿触发、入库门槛、SQLite 事实表 |
| 调度 tick | `app/emby_scheduler.py` | 采集周期、模块调用顺序、调试快照 |
| 播放段 | `app/playback_record_store.py` | JSON 播放段存储、open/结案、checkpoint |
| 连播边界 | `app/emby_continuous_playback.py` | 切集空窗判别、选片/播放路由 |
| 会话归一化 | `app/emby_client.py` | `/Sessions` 归一化、异常断开识别 |
| 持久化 | `app/emby_traffic_db.py` | 基线、累加器、连接绑定、统计快照 |
| 累加键 | `app/emby_traffic_filter.py` | `playback_accumulator_key`、WAN 过滤、账户切换剔除 |

---

## 3. 采集 Tick 周期

每个 Emby 实例由 `EmbyInstanceWorker` 按 **刷新间隔**（默认 5s）执行 `_tick`；其中 **完整 tick**（`full=True`）才拉 Lucky `accessdetail`，轻量 tick 复用上次连接快照。

### 3.1 Lucky 模式下的严格顺序

顺序在 `emby_scheduler.py` 中刻意固定，**不可颠倒**：

1. **拉取 Emby `/Sessions`** → `normalize_session`
2. **`playback_record_store.tick_from_sessions`** — 播放段 open/刷新/结案；换集时先把旧段流量 checkpoint 结转
3. **`enrich_sessions_playback_started_at`** — 为裁决提供 `playback_started_at`
4. **Lucky 拉取与增量** — `calc_ip_traffic_deltas` + `calc_conn_traffic_deltas`
5. **`emby_continuous_playback.tick`**（若开启选片入账）— 更新连播上下文
6. **`accumulate_wan_upload_by_conn`** — 裁决 + 写入累加器（**必须在步骤 2 之后**）
7. **`get_lucky_conn_debug_snapshot`** — 调试面板快照
8. **`browse_upload_settler.tick`** — 选片边沿结算
9. **`annotate_live_sessions_upload`** — 会话卡片实时流量字段

> **换集边界保障**：步骤 2 在 Lucky 分摊之前执行，保证换集 tick 的新增量只进入新播放段，旧段已在结案/换集时完整结转；边界误差被限制在至多一个 tick。

### 3.2 流量采集优先级

| 层级 | 来源 | 函数 |
|------|------|------|
| 首选 | 连接级 `ConnsStatistics` | `accumulate_wan_upload_by_conn` |
| 回退 | IP 级 `TrafficOut` 增量 | `accumulate_wan_upload_by_ip`（按码率权重分摊） |

连接级增量之和通常 ≤ IP 级总量；差额代表 **短连接漏计**，由 IP 级对账补给（见 §7.4）。

---

## 4. Lucky 流量增量计算

实现于 `app/emby_lucky.py`。

### 4.1 基线与首见保护

Lucky 返回的是 **累计值**（`TrafficOut`），本 tick 增量 = `current - last_baseline`。

- **首见**（无持久化基线）：本 tick 增量 = **0**，仅登记基线  
  → 避免把连接建立前的历史累计一次性计入，造成 GB 级尖峰
- **计数器回退**（`current < last`）：本 tick 增量 = **0**，以 `current` 为新基线  
  → 避免把整段累计误判为「重置」而重复入账（v1.70 修复）

基线持久化：

- IP 级 → `emby_lucky_ip_baselines`
- 连接级（`RemoteAddr`）→ `emby_lucky_conn_baselines`

### 4.2 WAN 过滤

默认 `wan_traffic_only=true`：仅处理外网 IP（非 RFC1918 / loopback / link-local）。

连接展开：`iter_wan_conn_statistics(res_list)` 将 Lucky 返回的 IP 条目展开为 `{remote_addr, traffic_out, accept_time, ...}` 列表。

---

## 5. 连接裁决算法

核心入口：`emby_lucky_verdict.analyze_lucky_connections(sessions, conn_rows, conn_deltas, ...)`  
按 **外网 IP 分组**，每组内对所有 Lucky 连接逐条裁决。

### 5.1 波次聚类（「波1 / 波2」）

函数：`_cluster_conn_waves`

- 同 IP 下按 `AcceptTime` 排序
- 相邻建连时间差 **≤ 2 秒**（`_SATELLITE_ACCEPT_SECONDS`）→ 同一波次
- 波编号从 1 递增：第一波 = 波1，第二波 = 波2 …
- 波内 **累计流量最大** 的连接标记为 `wave_primary_addr`

**含义**：Android TV 等客户端常在同一时刻 burst 开多条 TCP（API、媒体、保活）；晚 17 秒新建的重连 socket 会落入下一波。波次用于 **建连时刻对齐评分**，不是 Emby 官方概念。

### 5.2 伴生保活识别

函数：`_satellite_addrs`

- 同秒（≤ 2s）建连的两条连接，若一方 `< 8KB`、另一方 `≥ 64KB` → 小流量方标为 **卫星/保活**（`traffic_role = control`）
- 保活连接 **不入账**（`billing_state = excluded`）

### 5.3 流量形态角色（traffic_role）

函数：`_traffic_role`

| traffic_role | 判定条件 |
|--------------|----------|
| `control` | 卫星连接，或总量 < 8KB 且 delta < 4KB |
| `stream_pending` | 本 tick delta ≥ 200KB，或（累计 ≥ 512KB 且 delta > 0） |
| `browse` | delta > 0 或累计 ≥ 64KB |

### 5.4 连接↔会话匹配评分

函数：`_assign_sessions_to_drafts` + `score_conn_for_session_detail`

**输入信号**：

| 信号 | 用途 |
|------|------|
| IP | 分组前提；同 IP 多会话竞争 |
| AcceptTime vs `playback_started_at` / `last_activity_date` | 时间匹配（±120s 播放、±180s 选片） |
| `session_mode` | playing / paused / viewing / connected / stopped |
| 流量规模与码率 | delta 与 `session_stream_bps` 比值 |
| 波次对齐 | 波内建连时刻与活动时刻对齐 +40 分 |
| 粘性绑定 | 上 tick 匹配记忆 +120 分 |
| 活动新鲜度 | 90s 内活动 +30；同设备最新用户 +15 |

**分配优先级**：

1. 同 IP **唯一外网会话** → 强制匹配（score 下限 80）
2. `stream_pending` 优先匹配 `playing` 会话（每 `persist_key` 仅一个主推流）
3. 无 stream 的波次：主连接 score ≥ 80 → 整波跟随同一会话
4. 波内 `control` / `browse` 跟随已分配会话
5. 剩余按总分排序；分差 < 50 → 标 `ambiguous`（模糊）

**置信度**（`high` / `medium` / `low`）：

- `ambiguous`、`primary_conflict`、`orphan` → `low`
- 低置信连接 **不写入** `binding_targets`（防错误粘性）

### 5.5 裁决后角色（conn_role）与入账状态（billing_state）

| conn_role | 展示标签 | 典型 billing_state |
|-----------|----------|-------------------|
| `control` | 保活 | `excluded` |
| `browse` | 选片 | `browse_credited` 或 `excluded` |
| `stream_pending` | 推流候选 | `pending` |
| `stream_primary` | 主推流 | **`credited`** |
| `stream_secondary` | 副推流 | `pending` |

**主推流分配**（同 IP、同 tick）：

```text
stream_rank 按 (-delta_out, -traffic_out, -accept_epoch) 排序
每个 persist_key 仅第一个 → stream_primary + credited
其余 stream_pending → stream_secondary + pending
```

因此播放时 **主推流标签可能在 tick 间在两条连接间跳动**（客户端/HLS 分片切换），但 **每个 tick 仅一条连接入账**，不会双计同 tick 增量。

### 5.6 选片入账判定

函数：`should_browse_credit_billing`

开启 `lucky_credit_browse_traffic` 时：

| Emby 模式 | 是否选片入账 |
|-----------|-------------|
| `viewing` | 是 |
| 有 `NowViewingItem` | 是 |
| `connected` + browse 角色 | 是，**除非**连播切集空窗抑制 |
| `playing` | 否（走播放桶） |
| `control` 角色 | 否 |

---

## 6. 归属键（Persist Key）

### 6.1 播放键

函数：`playback_accumulator_key`（`emby_traffic_filter.py`）

优先级（从高到低）：

```text
{user}|{client}|sid:{sid}
{user}|sid:{sid}
sid:{sid}
{user}|{client}|{item_id}
{user}|{client}|{series}|{episode_label}
{user}|{client}|{series}|{title}
{user}|{series}|{title}
```

用途：区分同一用户不同设备/会话/集数的 **播放流量累加器**。

### 6.2 选片键

函数：`browse_persist_key_for_session`（`emby_lucky_verdict.py`）

- 主键：`browse:{user_id}:{sid}`
- 遗留：`browse:sid:{sid}`（兼容旧数据，通过 `browse_persist_key_variants_for_session` 合并读取）

### 6.3 连接绑定记忆

| 结构 | 键 | 值 | 用途 |
|------|----|----|------|
| `_conn_bindings` | `remote_addr` | `persist_key` | 跨 tick 粘性归属 |
| `_conn_match_hints` | `remote_addr` | `session_match_key` | 跨 tick 匹配记忆 |

持久化：`emby_lucky_conn_bindings`（SQLite）。  
账户切换时清理旧用户绑定（`browse_upload_settler` / `clear_lucky_bindings_for_persist_key`）。

---

## 7. 流量分摊与入账

入口：`emby_playback_traffic.accumulate_wan_upload_by_conn`

### 7.1 播放流量（credited）

```text
billing_state == 'credited' 且 billing_persist_key 非空
→ conn_shares[pkey] += delta
→ _upload_accumulators[instance][pkey]
```

### 7.2 选片流量（browse_credited）

```text
billing_state == 'browse_credited'
→ 若 should_route_browse_delta_to_play（连播空窗/正在推流）
     → 播放桶
  否则
     → _browse_upload_accumulators[instance][browse_key]
     → 若 _is_pre_play_stream_burst → _tag_preplay_burst（开播前突发追踪）
```

**UI 说明**：多条连接卡片显示相同「选片累计 X MB」，展示的是 **同一会话级选片桶** 的值，不是每条连接各算一份。

### 7.3 不入账与余量

- `excluded` / `pending` / `orphan` → 进入 `unassigned_by_ip`
- 同 IP 有外网会话时，按码率权重 `_distribute_weighted` 补给
- 仍无法归属 → `remainder_bytes`（程序余量，不写桶）

### 7.4 IP 级对账（短连接漏计）

```text
shortfall = ip_deltas[ip] - sum(conn_deltas on ip)
若 shortfall > 0 且该 IP 下 credited 会话唯一
  → 补给该主推流 persist_key
否则 shortfall → remainder（不猜测，防误计）
```

### 7.5 开播前突发回收

场景：Emby 仍报 `connected`/`viewing`，但连接已在推流缓冲。

1. 速率 ≥ `emby_preplay_burst_mbps`（默认 1.5 MB/s）→ 标记 `_browse_preplay_burst`
2. 会话转 `playing` 时 → `settle_preplay_burst_to_play`
3. 仅回收 **最近 N 秒**（`emby_preplay_burst_window_seconds`，默认 3s）内误计入选片桶的突发
4. 更早的保留为真实选片流量

---

## 8. 播放段生命周期

存储：`data/emby_events/{instance}.json`  
模块：`playback_record_store.py`

### 8.1 建段

- Emby 会话满足 `is_current_playback_session`（有媒资且 playing/paused/长暂停空窗）
- 无对应 open 段 → `_new_record`
- **换集**（同 sid、不同 `item_id`）→ 先结案旧段（`settle_reason=item_change`）再新建

### 8.2 刷新

- 每 tick 对 active 段：更新位置、seek、观看时长、`live_upload_checkpoint_bytes`
- checkpoint 来自 `peek_accumulated_upload`（播放累加器快照）

### 8.3 结案

| 触发 | settle_reason | 宽限 |
|------|---------------|------|
| Emby 卡片消失（非暂停） | `emby_confirmed_stop` | 即时 |
| Emby 异常断开（僵尸会话） | `emby_abnormal_disconnect` | 即时 |
| 会话从 API 消失 | `grace_expired` | 5s |
| API 离线 | `timeout_offline`（incomplete） | 5min |
| 换集 | `item_change` | 即时 |

**异常断开**（`emby_client.is_emby_abnormal_disconnect_session`）：

- 有 `NowPlayingItem`、非暂停、`SupportsRemoteControl=false`
- `IsPlaying=false` 或缺失且有 `TranscodingInfo`
- 不再视为当前播放会话，立即结案并打标「Emby侧异常断开」

### 8.4 暂停抖动去重

- 暂停导致卡片短暂消失已结案，位置未推进（≤ 3s 容差）→ **不重复建段**
- 避免大量极小流量停止卡片（v1.71 修复）

### 8.5 流量结转

结案时：`emby_playback_upload_sync.take_accumulated_upload` 将累加器字节写入 `estimated_upload_bytes` 并持久化到 `emby_playback_upload_facts`。

---

## 9. 选片段结算

模块：`browse_upload_settler.py`  
每 tick 在 Lucky 分摊与裁决快照之后执行。

### 9.1 结算触发

| 边沿事件 | settle_reason |
|----------|---------------|
| viewing/connected → playing | `playback_started` 或转播放桶 |
| 会话从 API 消失 | `disconnect` |
| Lucky 行从 browse_credited 消失 | 宽限 5s 后 `browse_conn_end` |
| 同 SessionId 换用户 | `user_switch` |
| 同设备换账户 | `user_switch` |
| 僵尸会话被新账户取代 | `account_superseded` |
| 脱管 orphan 桶 ≥ 30s | `orphan_bucket` |
| API 离线 ≥ 5min | `timeout_offline` |
| 实例重置 | `instance_reset` |

### 9.2 开播时的分支

函数：`_redirect_or_settle_browse_playback_start`

| 条件 | 行为 |
|------|------|
| 连播切集空窗（`should_settle_browse_on_playback_start=false`） | `transfer_browse_bytes_to_play_for_session`，不生成选片记录 |
| 正常开播 | `settle_preplay_burst_to_play` + `_settle_browse_session` |

### 9.3 入库门槛

- 全局参数 `emby_browse_upload_min_mb`（默认 **1.0 MB**）
- 未达阈值：丢弃并清 meta，不入 `emby_browse_upload_facts`

---

## 10. 边界与异常处理

### 10.1 连播切集空窗

模块：`emby_continuous_playback.py`

同时满足：

- `session_mode == connected`，无 viewing 媒资
- 30s 内曾为 playing（`CONTINUOUS_PLAYBACK_MEMORY_SECONDS`）
- connected 持续时间 < **6s**（`EPISODE_SWITCH_GAP_MAX_SECONDS`）
- sid 在 open 播放段列表中

影响：

- 抑制 connected 选片入账
- browse delta 路由到播放桶
- 开播不结算选片（字节转入播放段）

### 10.2 账户切换

- `filter_superseded_wan_sessions`：同 IP+Client+Device 多用户时，剔除活动落后 >120s 的旧会话
- 结算旧用户选片桶，清理 Lucky 绑定

### 10.3 多会话同 IP 争夺主推流

- 仅一个 `stream_primary` / `credited` per persist_key per tick
- 其余标 `stream_secondary` + `pending` + `primary_conflict` → 低置信

### 10.4 API / Lucky 不可用

- Emby 拉取失败：保留上次 sessions，不清 WAN 分摊状态
- Lucky 拉取失败（轻量 tick）：复用 `_lucky_conn_rows_last` / `_lucky_conn_deltas_last`
- 无播放会话：`clear_instance_live_upload_state`（选片桶/meta 保留待 settler）

### 10.5 累加器过期

- 30min 无 touch → `_cleanup_stale_accumulators`
- open 播放段的 persist_key 在 `protected_playback_session_ids` 中 → 不清理

### 10.6 停止播放但 open 段仍存活

- `purge_stopped_wan_live_upload_state` + `checkpoint_stopped_session_upload` 保护已累积流量

---

## 11. 配置参数

### 11.1 实例级（Emby 设置）

| 参数 | 说明 |
|------|------|
| `traffic_collect_mode` | 设为 `lucky` 启用本模式 |
| `lucky_base_url` | Lucky 管理地址 |
| `lucky_rule_key` / `lucky_sub_key` | 反代规则（密钥存 secrets） |
| `lucky_credit_browse_traffic` | 是否计入选片流量（须 lucky 模式） |
| `wan_traffic_only` | 仅外网（默认 true） |

### 11.2 全局（设置 → Emby）

| 参数 | 默认 | 作用 |
|------|------|------|
| `emby_preplay_burst_mbps` | 1.5 MB/s | 开播前突发识别阈值 |
| `emby_preplay_burst_window_seconds` | 3s | 突发从选片桶回收的时间窗 |
| `emby_browse_upload_min_mb` | 1.0 MB | 选片记录入库最低流量 |
| `emby_burst_new_session_window_seconds` | 8s | Docker 模式新会话突发窗 |
| `emby_burst_seek_window_seconds` | 6s | Docker 模式 seek 突发窗 |
| `emby_burst_priority_mode` | seek_first | Docker 模式突发优先级 |

调度器在 `_apply_global_config` 中将 `emby_preplay_burst_*` 同步到 `emby_playback_traffic`，将 `emby_browse_upload_min_mb` 传给 `browse_upload_settler.tick`。

---

## 12. 持久化层

| SQLite 表 / 文件 | 内容 |
|------------------|------|
| `emby_lucky_ip_baselines` | IP 累计 TrafficOut 基线 |
| `emby_lucky_conn_baselines` | RemoteAddr 累计基线 |
| `emby_session_upload_accumulators` | 播放累加器 |
| `emby_browse_upload_accumulators` | 选片累加器 |
| `emby_lucky_conn_bindings` | 连接→persist_key 绑定 |
| `emby_browse_upload_facts` | 选片结算事实 |
| `emby_playback_upload_facts` | 播放段结算事实 |
| `data/emby_events/{instance}.json` | 播放段 JSON（含 checkpoint） |

内存热状态（`_upload_accumulators`、`_browse_upload_accumulators`、`_conn_bindings` 等）在 `hydrate_live_upload_state` 时从 DB 恢复。

---

## 13. 调试面板

地址栏加 `?emby_debug=1` 进入调试模式。

Lucky 连接裁决区展示：

- **波次**（波1/波2）、**角色**（保活/选片/主推流/副推流）
- **入账状态**（选片入账/已入账/不入账/待确认）
- **置信度**（高/中/低）、**匹配分**、**打分详情**
- **选片累计 / 播放累计**（会话级桶快照）
- **粘性**（粘）— 沿用上一 tick 匹配记忆

页脚说明（`emby.js`）：

> Lucky 按 ConnsStatistics 采集各连接上传增量；按 AcceptTime 聚波并与 Emby 外网会话统一裁决，同 IP 多会话按码率权重分摊；选片与播放分账，选片段由开播/断线边沿触发结算落库。

---

## 14. 与 Docker 模式的差异（简述）

| 维度 | Lucky 模式 | Docker 模式 |
|------|-----------|-------------|
| 流量来源 | Lucky 连接级 / IP 级 | 容器 net tx 总量 |
| 归属方式 | 裁决算法 + 连接绑定 | 码率权重分摊到 WAN 会话 |
| 选片流量 | 独立选片桶 + 边沿结算 | 不支持选片分账 |
| 精度 | 连接级，按用户/设备/session 键 | 实例级总量，按会话比例 |

Docker 模式下的突发窗口（新会话/seek）与 Lucky 无关；Lucky 模式使用 **开播前突发** 参数处理 connected/viewing 阶段的推流缓冲。

---

## 15. 重要常量速查

### `emby_lucky_verdict.py`

| 常量 | 值 | 含义 |
|------|-----|------|
| `_SATELLITE_ACCEPT_SECONDS` | 2.0 | 波次聚类 / 卫星窗口 |
| `_CONTROL_MAX_BYTES` | 8 KB | 保活上限 |
| `_BROWSE_MIN_BYTES` | 64 KB | 浏览流量下限 |
| `_STREAM_TICK_BYTES` | 200 KB | 推流 tick 阈值 |
| `_STREAM_CUMULATIVE_BYTES` | 512 KB | 推流累计阈值 |
| `_ACCEPT_TIME_MATCH_SECONDS` | 120 | 建连-播放活动匹配窗 |
| `_WAVE_ASSIGN_MIN_SCORE` | 80 | 波次整体对齐门槛 |
| `_AMBIGUOUS_SCORE_GAP` | 50 | 模糊匹配分差 |
| `_STICKY_BINDING_BONUS` | 120 | 粘性加分 |
| `_WAVE_ALIGN_BONUS` | 40 | 波次对齐加分 |

### `browse_upload_settler.py`

| 常量 | 值 |
|------|-----|
| `BROWSE_CONN_END_GRACE_SECONDS` | 5 |
| `ORPHAN_BUCKET_MIN_AGE_SECONDS` | 30 |
| `OFFLINE_TIMEOUT_SECONDS` | 300 |

### `emby_continuous_playback.py`

| 常量 | 值 |
|------|-----|
| `CONTINUOUS_PLAYBACK_MEMORY_SECONDS` | 30 |
| `EPISODE_SWITCH_GAP_MAX_SECONDS` | 6 |

### `playback_record_store.py`

| 常量 | 值 |
|------|-----|
| `STOP_GRACE_SECONDS` | 5 |
| `FLAP_POSITION_TOLERANCE_SECONDS` | 3 |
| `OFFLINE_TIMEOUT_SECONDS` | 300 |

---

## 16. 设计原则摘要

1. **连接级优先、IP 级对账**：精确归属 + 短连接不漏计、不多猜
2. **每 tick 单主推流**：同会话同 tick 仅一条连接 `credited`，防播放双计
3. **选片/播放分账**：不同 persist key、不同结算边沿、不同事实表
4. **边沿结算**：选片在状态变化时落库，不在每 tick 写库
5. **播放段先于分摊**：换集/结案 checkpoint 先于 Lucky 增量，边界误差 ≤ 1 tick
6. **首见零增量、回退零增量**：防历史累计尖峰与 GB 级虚增
7. **低置信不写绑定**：ambiguous / primary_conflict 不固化错误归属
8. **连播/异常断开/暂停抖动/账户切换**：均有独立边界模块，避免误计与脏卡片

---

*文档版本：与代码库 v1.71 同步。若模块行为变更，请同步更新本文档与 `md/` 下对应 changelog。*
