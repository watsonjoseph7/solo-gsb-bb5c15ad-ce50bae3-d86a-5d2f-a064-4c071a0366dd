# 实验室传感器时间线归并 + 发布后阈值告警 API（FastAPI + SQLite）

多个传感器来源（source）按各自的序列号（seq）**乱序批量上报**带事件时间（`observed_at`）的读数。
服务以“**最慢来源的事件时间 − 允许迟到量**”维护水位线（watermark），只发布越过水位线、
且此前尚未发布的读数，并按 `(observed_at, source_id, seq)` 输出**确定顺序**的归并流。
越过**已发布**水位线才到达的迟到读数不会改写历史，而是进入**隔离区**供查询。

在此之上，本服务还提供两种发布后告警：

1. **单来源阈值告警**：为会话内某个来源创建规则后，系统在**每一条确定发布的读数**上
   驱动该来源独立的两态状态机（`normal` → `active` → `normal`），用**进入/恢复双阈值
   构成的滞回带**加**连续次数**消除边界抖动，并支持当前告警查询与进入/恢复事件的历史分页。
2. **跨来源复合告警**：一条规则声明 2～4 个**不同来源**的条件（各自的方向、阈值、
   可选 `value_key` 与 `window_seconds`）。系统按归并顺序为每个条件维护“最近一次命中”，
   当所有条件的命中都落在各自的事件时间窗口内时产生**一次** `correlated_entered` 事件；
   任一条件超窗即静默回到等待状态，可再次触发。

- 语言/框架：Python 3.11 · FastAPI · Uvicorn
- 存储：SQLite（WAL 模式，单文件，零外部依赖）
- 并发：写操作在 `BEGIN IMMEDIATE` 事务内串行化，水位线推进单调、发布只发生一次，
  告警状态与发布在同一事务内推进

## 单条命令启动

```bash
docker compose up --build
```

启动后：

- 服务地址：`http://localhost:8000`
- 交互文档（Swagger UI）：`http://localhost:8000/docs`
- OpenAPI JSON：`http://localhost:8000/openapi.json`
- 健康检查：`http://localhost:8000/health`

数据库文件在容器内 `/data/sensors.db`，由 compose 命名卷 `sensor-data` 持久化；
建库与建表在应用启动时（FastAPI lifespan）自动完成，无需手动初始化。
停止并清理：`docker compose down`（加 `-v` 同时删除数据卷）。

## 必要配置

通过环境变量配置（compose 中已给默认值，可按需覆盖）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SENSOR_HOST` | `0.0.0.0` | uvicorn 绑定地址 |
| `SENSOR_PORT` | `8000` | 监听端口（容器内固定 `EXPOSE 8000`，改宿主机端口改 compose 的 `ports`） |
| `SENSOR_DB_PATH` | `/data/sensors.db` | SQLite 文件路径，父目录不存在会自动创建 |

## 本地开发（不用 Docker）

```bash
python3 -m pip install -r requirements.txt
SENSOR_DB_PATH=./sensors.db uvicorn app.main:app --reload --port 8000
pytest -q        # 运行 84 个端到端行为测试（需 pytest、httpx）
```

## 时间线语义

1. **创建会话固定来源列表与迟到容忍**：`source_ids` 之后不可更改；上报未知 `source_id` 返回 400。
2. **首报门槛**：所有来源都至少上报一次后水位线才有定义；此前读数全部缓冲、不发布、不隔离。
3. **水位线**：`WM = min(每个来源已知最大 observed_at) − allowed_lateness_seconds`，单调不减
   （来源时钟回拨不会使已发布水位线倒退）。
4. **发布规则**：仅当 `observed_at < WM`（严格小于，落在边界上的读数继续等待）时发布；
   每次上报只发布此前未发布的缓冲读数。发布顺序固定为 `observed_at, source_id, seq`。
5. **幂等**：重复提交 `(source_id, seq)` 且 `value` 相同 → 计为 `duplicates` 的空操作；
   同 key 不同 payload → 409，且该批次整体回滚（不会半写）。
6. **隔离（quarantine）**：读数到达时若 `observed_at < 已发布水位线`，它永远无法再进入已发布流，
   原样写入隔离区（记录当时的水位线），已发布历史不发生任何变化。
7. 时间统一按 UTC 处理：接受 `...Z`、带偏移量（`+02:00`）或不带时区（按 UTC）的 ISO 8601 字符串。

## 告警状态机语义

每条规则绑定一个会话中的一个来源，方向为 `above`（高于阈值告警）或 `below`（低于阈值告警），
状态机**只消费该来源状态为“已发布”的读数**，按发布顺序 `(observed_at, seq)` 逐条推进：

- **normal（正常）**：连续出现 `enter_consecutive` 条“越限”读数后进入 `active` 并产生
  `entered` 事件。`above` 下越限指 `value > enter_threshold`；`below` 下指 `value < enter_threshold`。
- **active（告警中）**：连续出现 `recover_consecutive` 条“恢复”读数后回到 `normal` 并产生
  `recovered` 事件。`above` 下恢复指 `value < recover_threshold`；`below` 下指 `value > recover_threshold`。
- **滞回防抖**：`above` 要求 `recover_threshold < enter_threshold`，`below` 要求
  `recover_threshold > enter_threshold`；落在两个阈值之间（滞回带内）的读数不越限也不恢复，
  只会把当前连续计数清零。阈值比较是**严格**的，恰好等于阈值的读数不计入。
- 非数值（字符串、布尔、NaN/Infinity）、缺失值或指定了 `value_key` 但对象里没有该键时，
  该读数只会重置连续计数，不产生事件、不改变状态。
- **只有确定发布的读数才推进状态**：缓冲中的读数不参与；因迟到进入隔离区的读数状态永远是
  `quarantined`，不可能被状态机看到，因此**绝不会改变告警状态或补造事件**。
- **按来源独立**：不同来源（以及同一来源上的多条规则）各自维护独立的状态与连续计数。
- **晚建规则等价回放**：状态机是已发布流的纯函数。规则创建时会立即按顺序回放该来源此前全部
  已发布读数（不包括隔离读数），因此无论规则何时创建，其当前状态与历史事件都与“一开始就建好”
  完全一致。
- 数值来源：读数 `value` 本身是数字时直接比较；创建规则时给 `value_key`（如 `"temp"`）则从
  对象型读数的该顶层键取值。

## 跨来源复合告警语义

每条复合规则声明 **2～4 个条件**，条件按声明顺序（`position`）保存；每个条件绑定会话中
一个**互不相同**的来源，并自带方向（`above`/`below`）、阈值、可选 `value_key` 与窗口
`window_seconds`。规则消费的是**整个会话的已发布归并流**（顺序 `(observed_at, source_id,
seq)`），而不仅是条件来源的读数：

- 每处理一条确定发布的读数，若它属于某个条件且**严格越限**（`above`：`value > threshold`；
  `below`：`value < threshold`，相等不算），就刷新该条件的“最近命中”（记录 `seq`、
  `observed_at`、`value`）。非数值、布尔、NaN/Infinity、缺值或缺键的读数不会命中。
- 每次处理后（也包括不属于任何条件的来源读数——它们只推进事件时间）检查：每个条件都存在
  命中，且命中时间与当前读数事件时间之差 **≤ 各自的 `window_seconds`**（窗口边界**含等号**，
  恰好在界上仍然有效）。
- 规则为 `waiting` 且全部条件满足 → 转为 `correlated`，产生**一次** `correlated_entered`
  事件，事件锚定在完成本次复合的那条读数（`completed_by`）上，并快照各条件当时的命中
  （`conditions` 列表，按 `position`）。
- 已处于 `correlated` 时继续满足**不会重复事件**；只有当后续某条读数使任一条件超出自己的
  窗口（或尚无命中）时，规则才**静默**回到 `waiting`（不产生事件），此后才能再次复合。
- 每个条件的窗口互相独立：例如 A 的窗口 30s、B 的窗口 1s，只要 B 的命中超窗就回到等待，
  哪怕 A 仍新鲜。
- **只消费确定发布的读数**：缓冲读数不参与；迟到进入隔离区（`quarantined`）的读数永远不会
  成为已发布读数，因此既不能制造复合，也不会让命中“过期”。重复上报相同读数是空操作，
  同样不可能重复触发。
- **晚建规则等价回放**：复合结果是已发布归并流的纯函数。规则创建时会按全局归并顺序回放该会话
  此前全部已发布读数（不含隔离读数），其当前状态与历史事件与“一开始就建好”完全一致。

## 接口

### 1. 创建会话

`POST /sessions`

```bash
curl -s -X POST localhost:8000/sessions -H 'Content-Type: application/json' -d '{
  "session_id": "lab-1",
  "source_ids": ["thermometer-a", "thermometer-b"],
  "allowed_lateness_seconds": 10
}'
```

`session_id` 可省略（自动生成 UUID hex）。响应 201。

### 2. 批量上报读数（乱序、迟到均可）

`POST /sessions/{session_id}/readings`

请求体支持 `{"readings": [...]}`，也接受裸 JSON 数组：

```bash
curl -s -X POST localhost:8000/sessions/lab-1/readings \
  -H 'Content-Type: application/json' -d '{
  "readings": [
    {"source_id": "thermometer-b", "seq": 2, "observed_at": "2026-09-21T10:00:20Z", "value": 21.4},
    {"source_id": "thermometer-a", "seq": 1, "observed_at": "2026-09-21T10:00:05Z", "value": 22.1}
  ]
}'
```

响应汇总本批处理结果：

```json
{
  "session_id": "lab-1",
  "received": 2,
  "duplicates": 0,
  "quarantined": 0,
  "published_now": 0,
  "published_total": 0,
  "buffered": 2,
  "all_sources_reported": true,
  "published_watermark": null
}
```

字段：`received` 批内条数；`duplicates` 幂等重复条数；`quarantined` 本批被隔离条数；
`published_now` 本批触发新发布的条数；`published_total` 会话累计已发布；`buffered` 仍在等待的条数。
本批一旦有新读数发布，相关告警规则会在**同一事务**内推进并可能生成事件。

错误码：未知来源/非法时间/空批次 → 400；字段结构错误（缺字段、`seq<0` 等）→ 422；
同 key 不同 payload → 409；会话不存在 → 404。

### 3. 拉取已发布流

`GET /sessions/{session_id}/stream?limit=100&cursor=...`

- 只返回已发布读数，严格按 `(observed_at, source_id, seq)` 排序；
- keyset 分页：响应中的 `next_cursor` 原样回传即可翻页，为 `null` 表示取完；
- 每条读数带发布时的 `published_watermark`，响应体也带当前水位线。

```bash
curl -s 'localhost:8000/sessions/lab-1/stream?limit=50'
```

### 4. 查看隔离项

`GET /sessions/{session_id}/quarantine?limit=100&cursor=...`

按到达时间排列的迟到读数，含 `observed_at`、原始 `value` 与被拦截时的
`quarantined_watermark`（`reason` 固定为 `behind_published_watermark`）。同样支持游标分页。

```bash
curl -s 'localhost:8000/sessions/lab-1/quarantine'
```

### 5. 会话状态（辅助接口）

`GET /sessions/{session_id}`：返回来源列表、迟到容忍、是否已全部首报、
已发布水位线与当前计算出的有效水位线。

### 6. 创建告警规则

`POST /sessions/{session_id}/alert-rules`

为一个**会话内已声明的来源**创建阈值规则。请求字段：

| 字段 | 说明 |
| --- | --- |
| `source_id` | 会话 `source_ids` 之一；未知来源 → 400 |
| `direction` | `above`（高于进入阈值告警）或 `below`（低于进入阈值告警） |
| `enter_threshold` | 进入阈值（有限数值；比较为严格大于/小于） |
| `recover_threshold` | 恢复阈值；`above` 时必须严格小于进入阈值，`below` 时必须严格大于，否则 400 |
| `enter_consecutive` | 进入所需连续越限读数数（≥1） |
| `recover_consecutive` | 恢复所需连续恢复读数数（≥1） |
| `value_key` | 可选；从对象型读数中取该顶层键比较，省略则直接比较读数 `value` |
| `rule_id` | 可选自定义 id；重复 → 409 |

```bash
curl -s -X POST localhost:8000/sessions/lab-1/alert-rules \
  -H 'Content-Type: application/json' -d '{
  "rule_id": "temp-high",
  "source_id": "thermometer-a",
  "direction": "above",
  "enter_threshold": 30.0,
  "recover_threshold": 25.0,
  "enter_consecutive": 3,
  "recover_consecutive": 2
}'
```

响应 201，含规则当前的 `state`（`normal`/`active`）与 `streak`。若该来源此前已有已发布读数，
创建时立即回放并可能直接返回 `active` 且生成历史事件。

### 7. 查询规则

- `GET /sessions/{session_id}/alert-rules`：列出会话全部规则（含当前状态）。
- `GET /sessions/{session_id}/alert-rules/{rule_id}`：单条规则；不存在 → 404。

### 8. 查询当前告警

`GET /sessions/{session_id}/alerts/current?source_id=...`

只返回处于 `active` 的规则，按进入时间排序。每条含进入时的触发读数详情
（`seq`、`observed_at`、`value`、当时所用 `threshold`、服务端 `triggered_at`）
以及规则最近处理到的读数位置。可用 `source_id` 限定来源；未知来源 → 400。

```bash
curl -s 'localhost:8000/sessions/lab-1/alerts/current'
```

### 9. 查询告警历史（分页）

`GET /sessions/{session_id}/alerts/history?limit=100&cursor=...&source_id=...&rule_id=...&event_type=...`

- 返回 `entered` / `recovered` 事件，**按事件 id 倒序（最新在前）**；
- `next_cursor` 不透明游标，原样回传翻页；损坏游标 → 400；
- 可选过滤：`source_id`（未知来源 → 400）、`rule_id`（不存在 → 404）、
  `event_type`（仅允许 `entered`/`recovered`，否则 400）。

```bash
curl -s 'localhost:8000/sessions/lab-1/alerts/history?limit=20&event_type=entered'
```

### 10. 创建跨来源复合告警规则

`POST /sessions/{session_id}/correlated-rules`

请求体含 `conditions`（2～4 项）与可选 `rule_id`；每个条件字段：

| 字段 | 说明 |
| --- | --- |
| `source_id` | 会话 `source_ids` 之一，条件之间不可重复；未知来源 → 400 |
| `direction` | `above`（严格高于阈值命中）或 `below`（严格低于阈值命中） |
| `threshold` | 命中阈值（有限数值，严格比较，相等不计） |
| `window_seconds` | 该条件命中的有效时长（秒，> 0，边界含等号） |
| `value_key` | 可选；从对象型读数中取该顶层键比较，省略则直接比较读数 `value` |

```bash
curl -s -X POST localhost:8000/sessions/lab-1/correlated-rules \
  -H 'Content-Type: application/json' -d '{
  "rule_id": "heatwave",
  "conditions": [
    {"source_id": "thermometer-a", "direction": "above", "threshold": 30.0, "window_seconds": 15},
    {"source_id": "thermometer-b", "direction": "above", "threshold": 28.0,
     "window_seconds": 10, "value_key": "temp"}
  ]
}'
```

响应 201，含规则当前 `state`（`waiting`/`correlated`）、各条件的当前 `last_hit`、
复合锚点 `correlated_since` 以及归并流处理位置（`last_observed_at`/`last_source_id`/
`last_seq`）。创建时立即回放，已有的已发布读数可能直接让规则返回 `correlated`。
结构错误（条件数不在 2～4、来源重复、窗口非正、NaN 阈值等）→ 422；重复 `rule_id` → 409。

### 11. 查询复合规则

- `GET /sessions/{session_id}/correlated-rules`：列出会话全部复合规则（含各条件最近命中）。
- `GET /sessions/{session_id}/correlated-rules/{rule_id}`：单条规则；不存在 → 404。

### 12. 查询当前复合命中

`GET /sessions/{session_id}/correlated-alerts/current`

只返回处于 `correlated` 的规则，按复合完成时间排序。每条含 `correlated_entered`：
完成读数 `completed_by`（`source_id`/`seq`/`observed_at`）、完成时刻各条件命中快照
`conditions`（含位置、方向、阈值、窗口、命中读数与取值）以及服务端 `triggered_at`。

```bash
curl -s 'localhost:8000/sessions/lab-1/correlated-alerts/current'
```

### 13. 查询复合事件历史（分页）

`GET /sessions/{session_id}/correlated-alerts/history?limit=100&cursor=...&rule_id=...`

- 返回 `correlated_entered` 事件，**按事件 id 倒序（最新在前）**；
- `next_cursor` 不透明游标，原样回传翻页；损坏游标 → 400；
- 可选 `rule_id` 过滤（规则不存在 → 404）；会话不存在 → 404。

```bash
curl -s 'localhost:8000/sessions/lab-1/correlated-alerts/history?limit=20'
```

## 数据库迁移

建表采用“**幂等基础 schema + 版本化迁移**”：应用启动（FastAPI lifespan）时自动创建
基础表（`CREATE TABLE IF NOT EXISTS`），并按 `schema_migrations` 表记录的版本号依次执行
尚未应用的迁移；每个迁移与其版本登记在同一个 `BEGIN IMMEDIATE` 事务内提交。
因此老版本数据库会在下次启动时**原地升级、不丢数据**，新数据库则直接建到最新版本，
无需手工初始化。当前迁移版本：v1（跨来源复合告警的三张表）。

## 端到端示例（滞回 + 连续次数）

规则：`above`，进入阈值 30、恢复阈值 25，连续进入 3 次、连续恢复 2 次。
温度计 A 的读数只有在被水位线确定发布后才被消费：

| 已发布读数 | 计数/状态 |
| --- | --- |
| 31, 31 | 越限计数 2，仍 normal |
| 28（滞回带内：25 < 28 ≤ 30） | 越限计数清零，仍 normal |
| 31, 31, 31 | 第 3 次连续越限 → **entered**，转 active |
| 24 | 恢复计数 1 |
| 27 | 仍在滞回带内，恢复计数清零，保持 active |
| 24, 23 | 连续 2 次严格低于 25 → **recovered**，转 normal |

期间任何因迟到而被隔离的高温读数都不会触发或延长告警。

## 端到端示例（跨来源同时越限）

复合规则：两个来源都 `above`，A 阈值 30/窗口 15s，B 阈值 28/窗口 10s。
读数在被水位线确定发布后按归并顺序逐条驱动规则：

| 已发布归并流（事件时间） | 规则状态 |
| --- | --- |
| A@:00 = 31（命中 A） | `waiting`（B 尚无命中） |
| B@:05 = 29（命中 B；A 命中年龄 5s ≤ 15s，B 新鲜） | → **correlated_entered**，锚定 B@:05 |
| A@:12 = 31、B@:13 = 29 | 仍 `correlated`，**不重复事件** |
| C@:20 = 0（C 不参与规则，只推进事件时间） | B 命中年龄 15s > 10s → 静默回 `waiting` |
| A@:21 = 31、B@:21 = 29 | 两个条件同时在窗内 → 再次 **correlated_entered** |

期间任何因迟到而被隔离的读数都不会参与复合或使命中过期。

## 项目结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由、异常映射
│   ├── service.py       # 水位线计算 / 发布 / 隔离 / 幂等等核心逻辑
│   ├── alerts.py        # 单来源告警状态机、规则创建（回放）、当前/历史查询
│   ├── correlated.py    # 跨来源复合规则、窗口命中维护、回放、当前/历史查询
│   ├── errors.py        # 400/404/409 业务异常
│   ├── cursor.py        # 不透明 keyset 分页游标
│   ├── database.py      # SQLite schema、版本化迁移、连接、BEGIN IMMEDIATE 事务
│   ├── models.py        # Pydantic 请求/响应模型
│   ├── time_utils.py    # ISO 8601 解析与 UTC 规范化
│   └── config.py        # 环境变量配置
├── tests/
│   ├── test_api.py        # 时间线/隔离/幂等等端到端行为测试（22 个）
│   ├── test_alerts.py     # 单来源告警状态机/滞回/连续次数/隔离/回放/分页测试（27 个）
│   └── test_correlated.py # 跨来源复合/窗口/过期/再触发/隔离/回放/迁移测试（35 个）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
