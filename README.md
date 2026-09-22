# 实验室传感器时间线归并 + 发布后阈值告警 API（FastAPI + SQLite）

多个传感器来源（source）按各自的序列号（seq）**乱序批量上报**带事件时间（`observed_at`）的读数。
服务以“**最慢来源的事件时间 − 允许迟到量**”维护水位线（watermark），只发布越过水位线、
且此前尚未发布的读数，并按 `(observed_at, source_id, seq)` 输出**确定顺序**的归并流。
越过**已发布**水位线才到达的迟到读数不会改写历史，而是进入**隔离区**供查询。

在此之上，本服务还提供**发布后阈值告警**：为会话内某个来源创建规则后，系统在**每一条
确定发布的读数**上驱动该来源独立的两态状态机（`normal` → `active` → `normal`），
用**进入/恢复双阈值构成的滞回带**加**连续次数**消除边界抖动，并支持当前告警查询与
进入/恢复事件的历史分页。

此外，本服务支持**跨来源复合告警**：创建规则时声明 2～4 个来源及各自的方向、阈值、
可选 `value_key` 和 `window_seconds`；系统在归并后的已发布流上按合并顺序维护每个
条件的**最近命中**，只有当所有条件的最近命中都落在同一个事件时间窗口内时才产生一次
`correlated_entered` 事件，任一条件超窗即回到等待状态。

- 语言/框架：Python 3.11 · FastAPI · Uvicorn
- 存储：SQLite（WAL 模式，单文件，零外部依赖）
- 并发：写操作在 `BEGIN IMMEDIATE` 事务内串行化，水位线推进单调、发布只发生一次，
  告警状态与发布在同一事务内推进
- 迁移：建库与版本化迁移（`PRAGMA user_version`）在应用启动时自动完成，
  旧版本数据库启动即自动升级、数据保留

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
建库、建表与版本化迁移在应用启动时（FastAPI lifespan）自动完成，无需手动初始化。
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
pytest -q        # 运行 75 个端到端行为测试（需 pytest、httpx）
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

## 复合告警语义（跨来源相关）

每条复合规则绑定会话内 **2～4 个不同来源**的条件（各自独立的方向/阈值/可选 `value_key`），
并声明事件时间窗口 `window_seconds`。状态机消费**归并后的已发布流**，按确定顺序
`(observed_at, source_id, seq)` 逐条推进两个状态（`waiting` ⇄ `correlated`）：

- **命中（hit）**：某来源的已发布读数严格越过其条件阈值（`above` 为 `value > threshold`，
  `below` 为 `value < threshold`，等于阈值不算）时，该条件的“最近命中”更新为这条读数；
  未越限的读数不会清除已有命中，命中只通过时间窗口过期。非数值、缺失键的读数不构成命中。
- **进入**：每当处理完一条已发布读数后检查——若**所有**条件都有最近命中，且最早与最晚命中
  之差 `max − min ≤ window_seconds`（含边界），则从 `waiting` 进入 `correlated` 并产生
  **恰好一次** `correlated_entered` 事件，事件快照记录各条件当时的命中读数与窗口边界。
- **超窗回退**：处于 `correlated` 时，新命中使最早/最晚命中跨度超过窗口即**静默**回到
  `waiting`（不产生事件）；之后各条件重新对齐会再产生一次新的 `correlated_entered`。
- **恰好一次**：每条读数只发布一次、每条规则按归并流位置只消费一次，重复上报（幂等空操作）
  不会重复产生事件；数据库层另有 `UNIQUE(rule_id, window_end)` 兜底。
- **只有确定发布的读数才参与**：缓冲中的读数不参与；迟到被隔离的读数状态永远是
  `quarantined`，绝不会构成命中或补造事件。
- **晚建规则等价回放**：创建规则时立即按归并顺序回放相关来源此前全部已发布读数，
  因此无论规则何时创建，其当前命中、状态与历史事件都与“一开始就建好”完全一致。

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

### 10. 创建复合告警规则

`POST /sessions/{session_id}/correlation-rules`

为会话内 **2～4 个不同来源**创建跨来源复合规则。请求字段：

| 字段 | 说明 |
| --- | --- |
| `conditions` | 2～4 个条件（少于 2 或多于 4 → 422），来源必须互不相同（重复 → 422） |
| `conditions[].source_id` | 会话 `source_ids` 之一；未知来源 → 400 |
| `conditions[].direction` | `above`（高于阈值越限）或 `below`（低于阈值越限），严格比较 |
| `conditions[].threshold` | 越限阈值（有限数值） |
| `conditions[].value_key` | 可选；从对象型读数中取该顶层键比较，省略则直接比较读数 `value` |
| `window_seconds` | 事件时间窗口（> 0 的有限数值）；所有条件的最近命中须落在同一窗口内 |
| `rule_id` | 可选自定义 id；重复 → 409 |

```bash
curl -s -X POST localhost:8000/sessions/lab-1/correlation-rules \
  -H 'Content-Type: application/json' -d '{
  "rule_id": "heat-and-dry",
  "window_seconds": 30,
  "conditions": [
    {"source_id": "thermometer-a", "direction": "above", "threshold": 30.0},
    {"source_id": "thermometer-b", "direction": "below", "threshold": 5.0, "value_key": "humidity"}
  ]
}'
```

响应 201，含规则定义与当前 `state`（`waiting`/`correlated`）。若相关来源此前已有
已发布读数，创建时立即回放，可能直接返回 `correlated` 并生成历史事件。

### 11. 查询复合规则

- `GET /sessions/{session_id}/correlation-rules`：列出会话全部复合规则（含当前状态）。
- `GET /sessions/{session_id}/correlation-rules/{rule_id}`：单条规则；不存在 → 404。

### 12. 查询当前命中

`GET /sessions/{session_id}/correlation-rules/{rule_id}/hits`

返回每个条件的当前最近命中（`last_hit`，未命中过为 `null`）、整体是否满足
（`satisfied`：所有条件都有命中且跨度在窗口内）、状态机状态 `state`、当前窗口边界
`window_start`/`window_end`（有全部命中时给出）以及规则在归并流上消费到的位置。

```bash
curl -s 'localhost:8000/sessions/lab-1/correlation-rules/heat-and-dry/hits'
```

```json
{
  "rule_id": "heat-and-dry",
  "session_id": "lab-1",
  "state": "correlated",
  "window_seconds": 30.0,
  "satisfied": true,
  "window_start": "2026-09-21T10:00:05Z",
  "window_end": "2026-09-21T10:00:20Z",
  "conditions": [
    {"source_id": "thermometer-a", "direction": "above", "threshold": 30.0,
     "value_key": null,
     "last_hit": {"source_id": "thermometer-a", "seq": 1,
                  "observed_at": "2026-09-21T10:00:05Z", "value": 31.5}},
    {"source_id": "thermometer-b", "direction": "below", "threshold": 5.0,
     "value_key": "humidity",
     "last_hit": {"source_id": "thermometer-b", "seq": 2,
                  "observed_at": "2026-09-21T10:00:20Z", "value": 3.1}}
  ],
  "last_observed_at": "2026-09-21T10:00:20Z",
  "last_source_id": "thermometer-b",
  "last_seq": 2
}
```

### 13. 查询复合事件历史（分页）

`GET /sessions/{session_id}/correlation-alerts/history?limit=100&cursor=...&rule_id=...`

- 返回 `correlated_entered` 事件，**按事件 id 倒序（最新在前）**；
- 每个事件含触发时的窗口边界与全部命中读数快照（`hits`）；
- `next_cursor` 不透明游标，原样回传翻页；损坏游标 → 400；
- 可选过滤：`rule_id`（不存在 → 404）。

```bash
curl -s 'localhost:8000/sessions/lab-1/correlation-alerts/history?limit=20'
```

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

## 端到端示例（跨来源复合）

规则：温度计 A `above 30`、湿度计 B `below 5`（取 `value_key="humidity"`），窗口 30 秒。
两个来源的读数同样只有在被水位线确定发布后才被消费：

| 已发布读数（归并序） | 命中/状态 |
| --- | --- |
| A@10:00:05 = 31.5 | A 最近命中 = 10:00:05；B 未命中，仍 waiting |
| B@10:00:20 humidity = 3.1 | B 最近命中 = 10:00:20；跨度 15s ≤ 30s → **correlated_entered**，转 correlated |
| A@10:01:10 = 32.0 | A 命中更新为 10:01:10；与 B 跨度 50s > 30s → 静默回到 waiting |
| B@10:01:25 humidity = 2.8 | B 命中更新为 10:01:25；跨度 15s ≤ 30s → 第二次 **correlated_entered** |

若 A 的高温读数因迟到被隔离，它不会构成命中；只有 A 再发布一条窗口内的越限读数，
规则才会再次进入 correlated。

## 项目结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由、异常映射
│   ├── service.py       # 水位线计算 / 发布 / 隔离 / 幂等等核心逻辑
│   ├── alerts.py        # 单来源告警状态机、规则创建（回放）、当前/历史查询
│   ├── correlations.py  # 跨来源复合规则、命中维护、correlated_entered 事件
│   ├── errors.py        # 400/404/409 业务异常
│   ├── cursor.py        # 不透明 keyset 分页游标
│   ├── database.py      # SQLite schema、版本化迁移、BEGIN IMMEDIATE 事务
│   ├── models.py        # Pydantic 请求/响应模型
│   ├── time_utils.py    # ISO 8601 解析与 UTC 规范化
│   └── config.py        # 环境变量配置
├── tests/
│   ├── test_api.py          # 时间线/隔离/幂等等端到端行为测试（22 个）
│   ├── test_alerts.py       # 单来源告警状态机/滞回/连续次数/隔离/回放/分页（27 个）
│   └── test_correlations.py # 复合规则/窗口对齐/超窗回退/隔离/回放/分页（26 个）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
