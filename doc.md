# Rift Realtime Protocol / 1.0

状态：Draft
协议代号：Rift/1
定位：面向实时应用的双向事件、状态同步与可靠消息协议
目标实现语言：Rust / TypeScript / Swift / Kotlin / C / C++
不推荐实现语言：Java 技术栈

---

# 1. 设计目标

Rift/1 是一个面向现代实时业务的应用层协议，目标是替代传统的“裸 WebSocket + 自定义事件字符串”模型，并吸收 Socket.IO 的易用性，同时修正其在协议语义、可靠性、重连恢复、背压、观测和分布式路由上的弱点。

Rift/1 的核心目标是：

1. 提供明确的消息语义，而不是只提供 `event + payload`。
2. 支持可靠、有序、可恢复的事件流。
3. 支持可丢弃、只保留最新值的高频状态同步。
4. 支持 topic/room 级别的权限、保留策略、限流策略和 fanout 策略。
5. 支持跨节点部署，不依赖单机内存会话。
6. 支持弱网重连后的 offset replay。
7. 支持 schema-first 的事件定义。
8. 支持二进制优先，同时保留调试友好的 JSON 模式。
9. 支持显式背压、慢消费者处理和消息降级。
10. 支持完整可观测性，包括 trace、ack、drop、retry、replay 和 fanout 指标。

---

# 2. 非目标

Rift/1 不试图成为通用消息队列，不替代 Kafka、NATS、Redis Streams 或 Pulsar。

Rift/1 不保证所有消息永久存储。消息是否持久化由 topic 策略和 message class 决定。

Rift/1 不强制唯一传输协议。WebSocket、WebTransport、TCP、Unix Socket 都可以作为 transport binding。

Rift/1 不将“连接在线”视为业务状态的唯一事实来源。在线状态必须通过 presence topic 和 heartbeat policy 建模。

Rift/1 不默认提供 exactly-once。协议只提供幂等、去重和 offset 机制，业务语义上的 exactly-once 需要上层状态机配合。

---

# 3. 分层模型

Rift/1 分为四层：

## 3.1 Transport Layer

负责承载双向字节流或数据报。

推荐传输：

| 传输                 | 用途     | 说明                             |
| ------------------ | ------ | ------------------------------ |
| WebSocket          | 默认兼容传输 | 适合大多数 Web 和桌面场景                |
| WebTransport       | 高级传输   | 支持 stream 与 datagram，更适合高频实时状态 |
| Native TCP/TLS     | 原生客户端  | 适合游戏、桌面、嵌入式、内网服务               |
| Unix Domain Socket | 本地进程通信 | 适合 sidecar、agent、本地网关          |

## 3.2 Session Layer

负责身份、连接、恢复、心跳、流控、能力协商。

## 3.3 Message Layer

负责消息类型、可靠性、有序性、ack、重放、去重、优先级和过期策略。

## 3.4 Topic Layer

负责 topic/room 的订阅、发布、权限、分片、fanout、保留和状态快照。

---

# 4. 基本术语

| 术语           | 定义                             |
| ------------ | ------------------------------ |
| Client       | 发起连接的一端，通常是浏览器、移动端、桌面端或服务端 SDK |
| Gateway      | 接收客户端连接的边缘节点                   |
| Broker       | 负责 topic 路由、持久化、fanout 的内部节点   |
| Session      | 一个客户端的逻辑会话，可跨连接恢复              |
| Connection   | 一条实际网络连接，断线后会变化                |
| Client ID    | 客户端长期身份标识                      |
| Session ID   | 服务端为一次逻辑会话分配的标识                |
| Epoch        | 会话世代号，用于区分旧连接和新连接              |
| Topic        | 可订阅、可发布、可路由的消息空间               |
| Room         | Topic 的业务别名；协议层统一称为 Topic      |
| Stream       | 同一 topic 下具备顺序语义的消息序列          |
| Offset       | Stream 内单调递增的位置                |
| Event        | 一条结构化业务消息                      |
| Command      | 需要响应的请求型消息                     |
| State        | 只关心最新值的状态消息                    |
| Datagram     | 可丢弃、低延迟、不保证到达的消息               |
| Ack          | 对消息处理结果的确认                     |
| Snapshot     | 某个 topic 或业务状态的完整快照            |
| Replay       | 按 offset 补发历史消息                |
| Backpressure | 接收方或服务端无法继续承载当前发送速率时的控制机制      |

---

# 5. 连接生命周期

Rift/1 连接生命周期包括以下阶段：

1. Open
2. Hello
3. Authenticate
4. Resume 或 Start
5. Ready
6. Active
7. Draining
8. Closed

## 5.1 Open

客户端建立底层 transport 连接。

连接建立后，客户端必须发送 `hello` 控制帧。

## 5.2 Hello

客户端声明协议版本、客户端能力、编码格式、恢复信息和期望的传输特性。

客户端 hello 必须包含：

| 字段           | 必填 | 说明                   |
| ------------ | -- | -------------------- |
| protocol     | 是  | 固定为 `rift`           |
| version      | 是  | 协议版本，例如 `1.0`        |
| client_id    | 否  | 已知客户端身份              |
| session_id   | 否  | 尝试恢复时提供              |
| epoch        | 否  | 上一次会话世代号             |
| codecs       | 是  | 支持的编码列表              |
| transports   | 是  | 当前 transport 能力      |
| compression  | 否  | 支持的压缩算法              |
| auth_modes   | 是  | 支持的认证方式              |
| last_offsets | 否  | 客户端已处理的 topic offset |
| client_clock | 否  | 客户端时间，用于粗略偏移校验       |
| sdk          | 否  | SDK 名称和版本            |
| features     | 否  | 客户端特性开关              |

## 5.3 Authenticate

认证可以在 hello 中携带，也可以由服务端发起 challenge。

支持的认证方式：

| 模式               | 说明                |
| ---------------- | ----------------- |
| Bearer Token     | 适合 Web、移动端、普通业务系统 |
| Cookie Session   | 适合浏览器同域系统         |
| mTLS             | 适合内部服务、网关、边缘节点    |
| Signed Challenge | 适合设备身份、游戏客户端、IoT  |
| Anonymous        | 适合游客、公开订阅、临时会话    |

认证成功后，服务端返回 `welcome` 控制帧。

## 5.4 Resume

如果客户端携带 `session_id`、`epoch` 和 `last_offsets`，服务端应尝试恢复会话。

恢复结果分为：

| 结果       | 说明                                    |
| -------- | ------------------------------------- |
| resumed  | 会话恢复成功，topic 订阅、socket data 和可重放消息已恢复 |
| partial  | 会话恢复部分成功，部分 topic 需要 snapshot         |
| rejected | 会话无法恢复，客户端必须重新 start                  |
| expired  | 恢复窗口过期，客户端必须重新拉取 snapshot             |
| conflict | 存在更新 epoch 的连接，旧连接被拒绝                 |

## 5.5 Ready

服务端完成认证、能力协商、恢复判定后发送 `ready`。

收到 `ready` 后，客户端才可以发送业务消息。

## 5.6 Active

连接进入正常工作状态。

Active 状态下允许：

1. 发布消息。
2. 订阅 topic。
3. 取消订阅 topic。
4. 发送 command。
5. 接收 event/state/datagram。
6. 发送 ack。
7. 心跳保活。
8. 流控协商。

## 5.7 Draining

服务端准备关闭连接或迁移连接时进入 Draining。

服务端必须说明原因：

| 原因            | 说明     |
| ------------- | ------ |
| deploy        | 服务升级   |
| rebalance     | 连接重平衡  |
| overload      | 节点过载   |
| auth_expiring | 认证即将过期 |
| policy        | 策略要求   |
| idle          | 空闲超时   |

Draining 状态下，客户端应停止发送非关键消息，并在指定时间内重新连接。

## 5.8 Closed

连接关闭。

关闭必须携带 close code 和 reason。

---

# 6. Frame Envelope

所有 Rift/1 消息都封装为 frame。

Frame 分为 control frame、data frame、ack frame、flow frame、error frame 五类。

## 6.1 逻辑 Envelope

每个 frame 必须包含以下逻辑字段：

| 字段             | 类型     | 必填 | 说明                                  |
| -------------- | ------ | -- | ----------------------------------- |
| version        | u16    | 是  | 协议版本                                |
| frame_id       | u64    | 是  | 当前连接内单调递增                           |
| frame_type     | enum   | 是  | control / data / ack / flow / error |
| flags          | bitset | 是  | 压缩、加密、分片、trace 等标记                  |
| codec          | enum   | 是  | payload 编码                          |
| session_id     | string | 否  | 当前逻辑会话                              |
| stream_id      | string | 否  | 流标识                                 |
| topic          | string | 否  | topic 名称                            |
| event          | string | 否  | 事件名称                                |
| message_id     | string | 否  | 全局消息 ID                             |
| correlation_id | string | 否  | 请求响应关联 ID                           |
| trace_id       | string | 否  | 链路追踪 ID                             |
| timestamp      | i64    | 是  | 服务端或客户端生成时间                         |
| ttl_ms         | u32    | 否  | 消息生存时间                              |
| priority       | enum   | 否  | 消息优先级                               |
| payload_len    | u32    | 是  | payload 长度                          |
| payload        | bytes  | 否  | 业务或控制数据                             |

## 6.2 frame_type

| 类型      | 用途                                                           |
| ------- | ------------------------------------------------------------ |
| control | hello、welcome、ready、ping、pong、subscribe、unsubscribe、resume 等 |
| data    | event、state、command、reply、datagram 等                         |
| ack     | 确认消息处理结果                                                     |
| flow    | 流控、限速、窗口更新、降级通知                                              |
| error   | 协议错误、权限错误、业务错误、系统错误                                          |

## 6.3 flags

| 标记             | 说明            |
| -------------- | ------------- |
| compressed     | payload 已压缩   |
| encrypted      | payload 应用层加密 |
| fragmented     | frame 分片      |
| final_fragment | 最后一个分片        |
| requires_ack   | 需要 ack        |
| replayed       | 此消息来自 replay  |
| snapshot       | 此消息为 snapshot |
| degraded       | 消息经过降级处理      |
| duplicate      | 服务端检测为重复消息    |
| trace          | 携带 trace 信息   |

---

# 7. 编码格式

Rift/1 必须支持至少一种二进制编码和一种调试编码。

推荐编码：

| 编码          | 用途                |
| ----------- | ----------------- |
| CBOR        | 默认二进制编码           |
| MessagePack | 可选二进制编码           |
| Protobuf    | 强 schema 场景       |
| JSON        | 调试、开发、抓包          |
| Raw Bytes   | 文件片段、音视频片段、自定义二进制 |

生产环境默认推荐 CBOR 或 Protobuf。

JSON 只建议用于开发环境、调试环境、控制台工具和协议测试。

---

# 8. 消息类别

Rift/1 不允许所有业务消息都退化成普通 event。

每条业务消息必须声明 message class。

## 8.1 message class

| 类别       | 语义         | 到达保证               | 有序性        | 是否可重放      | 典型用途              |
| -------- | ---------- | ------------------ | ---------- | ---------- | ----------------- |
| event    | 普通事件       | 可配置                | 可配置        | 可配置        | 聊天、通知、业务事件        |
| command  | 请求指令       | 至少需要 reply 或 error | 单请求内有序     | 不默认重放      | RPC、操作请求          |
| reply    | command 响应 | 跟随 command 策略      | 跟随 command | 否          | 请求结果              |
| state    | 最新状态       | 可丢弃旧值              | 只保留最新      | 可 snapshot | 在线状态、光标、房间人数      |
| datagram | 高频低延迟消息    | 不保证                | 不保证        | 否          | 游戏位置、遥测、typing    |
| stream   | 连续数据流      | 可配置                | stream 内有序 | 可配置        | AI token、文件、音视频片段 |
| snapshot | 状态快照       | 可靠                 | 单点状态       | 是          | 重连恢复、初始化状态        |
| system   | 系统消息       | 可靠                 | 连接内有序      | 否          | 限流、迁移、维护通知        |

## 8.2 delivery mode

每条消息必须声明 delivery mode。

| 模式                  | 说明                         |
| ------------------- | -------------------------- |
| at_most_once        | 最多一次，允许丢失，不重试              |
| at_least_once       | 至少一次，可能重复，需要 message_id 去重 |
| exactly_once_effect | 协议提供幂等键和去重窗口，业务层保证效果唯一     |
| latest_only         | 只保留最新值，旧值可覆盖               |
| best_effort         | 尽力发送，无确认、无重放               |
| durable_ordered     | 持久化、有序、可按 offset 重放        |

默认规则：

| message class | 默认 delivery mode                                  |
| ------------- | ------------------------------------------------- |
| event         | at_least_once                                     |
| command       | at_least_once                                     |
| reply         | at_least_once                                     |
| state         | latest_only                                       |
| datagram      | best_effort                                       |
| stream        | durable_ordered 或 best_effort，由 stream profile 决定 |
| snapshot      | at_least_once                                     |
| system        | at_least_once                                     |

---

# 9. Topic 模型

Topic 是 Rift/1 的核心路由单位。

Topic 替代传统 Socket.IO 的 room，但语义更强。

## 9.1 Topic 命名

Topic 名称必须满足：

1. 使用 UTF-8。
2. 最大长度 256 字节。
3. 推荐使用 `/` 分层。
4. 不允许空 topic。
5. 不允许包含控制字符。
6. 不允许以 `$` 开头，`$` 前缀保留给系统 topic。

推荐格式：

| 类型             | 示例                        |
| -------------- | ------------------------- |
| 用户私有 topic     | `user/{user_id}`          |
| 会话 topic       | `session/{session_id}`    |
| 房间 topic       | `room/{room_id}`          |
| 文档协作 topic     | `doc/{doc_id}`            |
| 组织 topic       | `org/{org_id}`            |
| 系统 topic       | `$system/notice`          |
| presence topic | `presence/room/{room_id}` |

## 9.2 Topic Profile

每个 topic 必须绑定 topic profile。

Topic profile 定义：

| 字段              | 说明         |
| --------------- | ---------- |
| name            | profile 名称 |
| retention       | 消息保留策略     |
| ordering        | 有序策略       |
| auth_policy     | 权限策略       |
| fanout_policy   | 广播策略       |
| rate_limit      | 限流策略       |
| backpressure    | 背压策略       |
| replay          | 重放策略       |
| snapshot        | 快照策略       |
| max_subscribers | 最大订阅数      |
| max_publishers  | 最大发布者数     |
| region_policy   | 区域策略       |

## 9.3 Retention Policy

| 策略      | 说明           |
| ------- | ------------ |
| none    | 不保留消息        |
| ttl     | 按时间保留        |
| count   | 按数量保留        |
| size    | 按空间保留        |
| durable | 持久保留，由外部存储控制 |
| latest  | 只保留最新值       |

## 9.4 Ordering Policy

| 策略         | 说明                      |
| ---------- | ----------------------- |
| none       | 不保证顺序                   |
| connection | 单连接内有序                  |
| publisher  | 单发布者内有序                 |
| topic      | topic 内全局有序             |
| key        | 按 ordering_key 有序       |
| causal     | 因果有序，需要 vector metadata |

默认推荐：

| 场景       | ordering          |
| -------- | ----------------- |
| 聊天房间     | topic             |
| 私信       | topic             |
| 在线状态     | key               |
| 光标同步     | key               |
| 游戏移动     | none              |
| 协作文档操作日志 | topic 或 causal    |
| 通知       | publisher 或 topic |

---

# 10. Subscribe

客户端通过 subscribe 控制帧订阅 topic。

订阅请求必须包含：

| 字段               | 必填 | 说明            |
| ---------------- | -- | ------------- |
| topic            | 是  | 目标 topic      |
| mode             | 是  | 订阅模式          |
| from_offset      | 否  | 从指定 offset 开始 |
| last_seen_offset | 否  | 客户端已处理 offset |
| snapshot_policy  | 否  | 是否需要 snapshot |
| filter           | 否  | 服务端过滤条件       |
| qos              | 否  | 期望服务质量        |
| auth_context     | 否  | 附加授权上下文       |

## 10.1 订阅模式

| 模式                 | 说明                   |
| ------------------ | -------------------- |
| live               | 只接收订阅后的新消息           |
| replay             | 从 offset 重放          |
| snapshot_then_live | 先发快照，再进入 live        |
| latest             | 只接收最新状态              |
| passive            | 只接收系统通知，不参与 presence |
| ephemeral          | 临时订阅，断线自动清理          |

## 10.2 Subscribe Ack

服务端必须返回订阅结果。

| 结果                | 说明              |
| ----------------- | --------------- |
| accepted          | 订阅成功            |
| denied            | 权限不足            |
| not_found         | topic 不存在       |
| gone              | topic 已关闭       |
| replay_required   | 必须从指定 offset 重放 |
| snapshot_required | 必须先获取 snapshot  |
| rate_limited      | 订阅过于频繁          |
| overloaded        | 服务端过载           |
| invalid_filter    | 过滤条件非法          |

---

# 11. Publish

客户端或服务端通过 data frame 发布消息。

发布消息必须包含：

| 字段           | 必填 | 说明           |
| ------------ | -- | ------------ |
| topic        | 是  | 目标 topic     |
| event        | 是  | 事件名称         |
| class        | 是  | 消息类别         |
| delivery     | 是  | 到达模式         |
| message_id   | 是  | 消息唯一 ID      |
| payload      | 是  | 业务载荷         |
| schema       | 是  | schema 名称和版本 |
| ordering_key | 否  | 有序键          |
| dedupe_key   | 否  | 幂等去重键        |
| ttl_ms       | 否  | 生存时间         |
| priority     | 否  | 优先级          |
| requires_ack | 否  | 是否需要 ack     |
| causality    | 否  | 因果依赖信息       |

## 11.1 message_id

message_id 必须在去重窗口内唯一。

推荐格式：

1. ULID。
2. UUIDv7。
3. Snowflake-like ID。
4. 服务端分配的 monotonic ID。

不推荐使用随机 UUIDv4 作为高吞吐 topic 的排序依据。

## 11.2 dedupe_key

dedupe_key 用于业务幂等。

例如：

| 场景   | dedupe_key                |
| ---- | ------------------------- |
| 发消息  | `client_message_id`       |
| 支付确认 | `payment_intent_id`       |
| 文档操作 | `operation_id`            |
| 通知投递 | `notification_id:user_id` |

服务端必须在 topic profile 指定的 dedupe window 内拒绝重复效果。

---

# 12. Ack 机制

Rift/1 区分传输确认、服务端接收确认、业务处理确认。

## 12.1 Ack 类型

| 类型        | 说明             |
| --------- | -------------- |
| received  | 对端已收到 frame    |
| accepted  | 服务端接受消息并进入处理流程 |
| persisted | 消息已持久化         |
| delivered | 消息已投递到目标连接或订阅者 |
| processed | 业务处理完成         |
| rejected  | 消息被拒绝          |
| expired   | 消息过期           |
| duplicate | 重复消息，未再次执行     |
| failed    | 处理失败           |

## 12.2 Ack 必须包含

| 字段             | 必填 | 说明           |
| -------------- | -- | ------------ |
| ack_id         | 是  | ack 消息 ID    |
| message_id     | 是  | 被确认的消息       |
| status         | 是  | ack 状态       |
| offset         | 否  | 服务端分配 offset |
| reason         | 否  | 失败原因         |
| error_code     | 否  | 错误码          |
| retry_after_ms | 否  | 建议重试时间       |
| server_time    | 是  | 服务端时间        |

## 12.3 Ack 策略

| 策略          | 说明            |
| ----------- | ------------- |
| none        | 不需要 ack       |
| server      | 服务端接收即 ack    |
| persisted   | 持久化后 ack      |
| quorum      | 多副本确认后 ack    |
| subscriber  | 指定订阅方处理后 ack  |
| application | 业务逻辑明确确认后 ack |

---

# 13. Offset 与 Replay

Rift/1 的可靠恢复基于 topic offset，而不是单纯基于连接状态。

## 13.1 Offset 规则

1. 每个 durable topic 必须有独立 offset。
2. offset 必须单调递增。
3. offset 不要求连续暴露给客户端，但客户端看到的 offset 必须可比较。
4. replay 必须按 offset 顺序发送。
5. replay 消息必须带 `replayed` flag。
6. 如果 offset 已过期，服务端必须返回 `snapshot_required`。

## 13.2 客户端恢复信息

客户端重连时必须提交：

| 字段               | 说明                    |
| ---------------- | --------------------- |
| session_id       | 旧会话 ID                |
| epoch            | 旧会话世代                 |
| last_offsets     | 每个 topic 的最后处理 offset |
| pending_commands | 未完成 command 列表        |
| client_time      | 客户端时间                 |
| sdk_state_hash   | 可选，用于状态校验             |

## 13.3 恢复结果

| 状态                | 说明        |
| ----------------- | --------- |
| full_resume       | 完整恢复      |
| partial_resume    | 部分恢复      |
| replaying         | 正在补发      |
| snapshot_required | 必须重新拉快照   |
| cold_start        | 无法恢复，重新开始 |
| rejected          | 恢复请求被拒绝   |

## 13.4 Snapshot 规则

当服务端无法通过 offset replay 恢复状态时，必须要求客户端拉取 snapshot。

Snapshot 必须包含：

| 字段          | 说明          |
| ----------- | ----------- |
| topic       | topic       |
| snapshot_id | 快照 ID       |
| base_offset | 快照对应 offset |
| schema      | 快照 schema   |
| payload     | 状态数据        |
| created_at  | 快照生成时间      |
| expires_at  | 快照过期时间      |
| checksum    | 可选校验值       |

客户端应用 snapshot 后，必须从 `base_offset + 1` 继续接收 live 或 replay 消息。

---

# 14. State 消息

State 消息用于同步“只关心最新值”的状态。

典型场景：

1. 用户在线状态。
2. 输入中状态。
3. 鼠标位置。
4. 光标位置。
5. 房间人数。
6. 音视频状态。
7. 设备遥测。

## 14.1 State Key

State 消息必须包含 state_key。

同一 topic 下，`state_key` 相同的旧 state 可以被新 state 覆盖。

## 14.2 State 策略

| 策略              | 说明            |
| --------------- | ------------- |
| latest_only     | 只保留最新         |
| latest_per_key  | 每个 key 保留最新   |
| ttl_state       | 状态在 ttl 后自动失效 |
| heartbeat_state | 必须定期刷新，否则失效   |
| tombstone       | 使用删除标记清理状态    |

## 14.3 Presence

Presence 是特殊 state。

Presence 必须包含：

| 字段            | 说明                             |
| ------------- | ------------------------------ |
| subject       | 用户、设备或连接                       |
| status        | online / away / busy / offline |
| session_id    | 会话 ID                          |
| connection_id | 当前连接 ID                        |
| ttl_ms        | presence 有效期                   |
| metadata      | 业务元数据                          |
| updated_at    | 更新时间                           |

Presence 不应仅依赖 TCP 连接关闭判断。

---

# 15. Command / Reply

Command 用于请求响应模型。

Command 必须包含：

| 字段              | 必填 | 说明         |
| --------------- | -- | ---------- |
| command         | 是  | command 名称 |
| correlation_id  | 是  | 请求响应 ID    |
| timeout_ms      | 是  | 超时时间       |
| idempotency_key | 否  | 幂等键        |
| payload         | 是  | 请求数据       |
| schema          | 是  | schema 版本  |

Reply 必须包含：

| 字段             | 必填 | 说明                              |
| -------------- | -- | ------------------------------- |
| correlation_id | 是  | 对应 command                      |
| status         | 是  | ok / error / timeout / rejected |
| payload        | 否  | 响应数据                            |
| error          | 否  | 错误对象                            |
| server_time    | 是  | 服务端时间                           |

Command 不应该替代所有 event。

Command 用于明确需要响应的操作；event 用于事实广播；state 用于状态覆盖；datagram 用于可丢弃高频数据。

---

# 16. Schema-first 约束

Rift/1 要求所有业务消息必须声明 schema。

Schema ID 格式：

`{domain}.{name}@{major}.{minor}`

示例：

| Schema                      | 说明     |
| --------------------------- | ------ |
| `chat.message.created@1.0`  | 聊天消息创建 |
| `chat.message.deleted@1.0`  | 聊天消息删除 |
| `doc.operation.applied@2.1` | 文档操作   |
| `presence.user.updated@1.0` | 用户在线状态 |
| `room.member.joined@1.0`    | 房间成员加入 |

## 16.1 Schema 兼容规则

| 变更      | minor | major    |
| ------- | ----- | -------- |
| 新增可选字段  | 允许    | 不需要      |
| 新增必填字段  | 不允许   | 需要       |
| 删除字段    | 不允许   | 需要       |
| 修改字段类型  | 不允许   | 需要       |
| 扩展 enum | 谨慎允许  | 推荐 major |
| 收紧约束    | 不允许   | 需要       |
| 放宽约束    | 允许    | 不需要      |

## 16.2 Unknown Field

客户端必须忽略未知可选字段。

服务端不得依赖旧客户端无法理解的新必填字段。

## 16.3 Event Registry

服务端必须维护事件注册表。

注册表至少包含：

| 字段          | 说明        |
| ----------- | --------- |
| event       | 事件名       |
| schema      | schema ID |
| class       | 消息类别      |
| delivery    | 默认投递语义    |
| auth        | 权限策略      |
| rate_limit  | 限速        |
| retention   | 保留策略      |
| deprecated  | 是否废弃      |
| replacement | 替代事件      |
| owner       | 事件负责人或模块  |

---

# 17. 权限模型

Rift/1 权限按动作判断，而不是只在连接时判断一次。

## 17.1 动作

| 动作          | 说明          |
| ----------- | ----------- |
| connect     | 建立连接        |
| resume      | 恢复会话        |
| subscribe   | 订阅 topic    |
| unsubscribe | 取消订阅        |
| publish     | 发布消息        |
| command     | 执行 command  |
| replay      | 请求重放        |
| snapshot    | 请求快照        |
| presence    | 发布 presence |
| admin       | 管理 topic    |

## 17.2 授权上下文

授权判断应包含：

| 字段         | 说明           |
| ---------- | ------------ |
| subject    | 用户或服务身份      |
| client_id  | 客户端 ID       |
| session_id | 会话 ID        |
| topic      | 目标 topic     |
| action     | 动作           |
| event      | 事件名          |
| claims     | token claims |
| region     | 连接区域         |
| device     | 设备信息         |
| risk       | 风险评分         |
| time       | 当前时间         |

## 17.3 权限缓存

权限可以缓存，但必须支持失效。

缓存失效触发条件：

1. 用户登出。
2. token 撤销。
3. 角色变化。
4. topic 权限变化。
5. 风控状态变化。
6. 组织成员关系变化。
7. 服务端主动踢出。

---

# 18. 背压与限流

Rift/1 必须显式处理背压。

## 18.1 连接级背压

每条连接必须维护发送窗口。

当客户端处理过慢时，服务端可以：

| 策略             | 说明                  |
| -------------- | ------------------- |
| pause          | 暂停投递                |
| drop_volatile  | 丢弃可丢消息              |
| coalesce_state | 合并 state，只保留最新      |
| downgrade      | 降低消息频率              |
| disconnect     | 断开慢消费者              |
| snapshot_later | 停止增量，要求重新拉 snapshot |

## 18.2 Topic 级背压

Topic 可以定义 fanout budget。

当广播压力过高时，服务端可以：

1. 降低 state 频率。
2. 合并相同 state_key 的状态。
3. 丢弃过期 datagram。
4. 将大 topic 切换到分层 fanout。
5. 拒绝低优先级 publish。
6. 对订阅者下发 overload 通知。
7. 要求客户端降级到 snapshot polling。

## 18.3 优先级

| 优先级        | 说明            |
| ---------- | ------------- |
| critical   | 认证、系统、关闭、权限变更 |
| high       | 用户可见的关键业务消息   |
| normal     | 默认业务消息        |
| low        | 非关键通知         |
| volatile   | 可丢弃状态         |
| background | 后台同步          |

丢弃顺序必须从低优先级开始。

---

# 19. 错误模型

所有错误必须使用结构化 error。

Error 必须包含：

| 字段             | 说明         |
| -------------- | ---------- |
| code           | 稳定错误码      |
| message        | 面向开发者的简短说明 |
| reason         | 机器可读原因     |
| retryable      | 是否可重试      |
| retry_after_ms | 建议重试时间     |
| correlation_id | 关联请求       |
| details        | 附加信息       |
| server_time    | 服务端时间      |

## 19.1 错误码

### 协议错误

| 错误码                                  | 说明         |
| ------------------------------------ | ---------- |
| RIFT_PROTOCOL_VERSION_UNSUPPORTED    | 协议版本不支持    |
| RIFT_PROTOCOL_FRAME_INVALID          | frame 格式非法 |
| RIFT_PROTOCOL_CODEC_UNSUPPORTED      | 编码不支持      |
| RIFT_PROTOCOL_PAYLOAD_TOO_LARGE      | payload 过大 |
| RIFT_PROTOCOL_REQUIRED_FIELD_MISSING | 缺少必填字段     |
| RIFT_PROTOCOL_SCHEMA_MISMATCH        | schema 不匹配 |
| RIFT_PROTOCOL_ORDER_VIOLATION        | 顺序约束被破坏    |

### 认证与权限

| 错误码                    | 说明         |
| ---------------------- | ---------- |
| RIFT_AUTH_REQUIRED     | 需要认证       |
| RIFT_AUTH_INVALID      | 认证无效       |
| RIFT_AUTH_EXPIRED      | 认证过期       |
| RIFT_AUTH_REVOKED      | 认证已撤销      |
| RIFT_PERMISSION_DENIED | 权限不足       |
| RIFT_TOPIC_FORBIDDEN   | 无权访问 topic |

### 会话与恢复

| 错误码                        | 说明          |
| -------------------------- | ----------- |
| RIFT_SESSION_NOT_FOUND     | 会话不存在       |
| RIFT_SESSION_EXPIRED       | 会话过期        |
| RIFT_SESSION_CONFLICT      | 会话 epoch 冲突 |
| RIFT_RESUME_REJECTED       | 恢复被拒绝       |
| RIFT_REPLAY_OFFSET_EXPIRED | offset 已过期  |
| RIFT_SNAPSHOT_REQUIRED     | 必须重新拉快照     |

### Topic

| 错误码                         | 说明        |
| --------------------------- | --------- |
| RIFT_TOPIC_NOT_FOUND        | topic 不存在 |
| RIFT_TOPIC_CLOSED           | topic 已关闭 |
| RIFT_TOPIC_OVERLOADED       | topic 过载  |
| RIFT_TOPIC_SUBSCRIBER_LIMIT | 订阅人数超限    |
| RIFT_TOPIC_PUBLISHER_LIMIT  | 发布者超限     |
| RIFT_TOPIC_RATE_LIMITED     | topic 限流  |

### 消息

| 错误码                          | 说明     |
| ---------------------------- | ------ |
| RIFT_MESSAGE_DUPLICATE       | 重复消息   |
| RIFT_MESSAGE_EXPIRED         | 消息已过期  |
| RIFT_MESSAGE_REJECTED        | 消息被拒绝  |
| RIFT_MESSAGE_TOO_LARGE       | 消息过大   |
| RIFT_MESSAGE_ACK_TIMEOUT     | ack 超时 |
| RIFT_MESSAGE_DELIVERY_FAILED | 投递失败   |

### 系统

| 错误码                            | 说明              |
| ------------------------------ | --------------- |
| RIFT_SYSTEM_OVERLOADED         | 系统过载            |
| RIFT_SYSTEM_MAINTENANCE        | 系统维护            |
| RIFT_SYSTEM_SHARD_MOVED        | topic shard 已迁移 |
| RIFT_SYSTEM_REGION_UNAVAILABLE | 区域不可用           |
| RIFT_SYSTEM_INTERNAL           | 内部错误            |

---

# 20. 关闭码

| Close Code | 名称                      | 说明      |
| ---------- | ----------------------- | ------- |
| 1000       | normal                  | 正常关闭    |
| 1001       | draining                | 服务端排空   |
| 1002       | protocol_error          | 协议错误    |
| 1003       | unsupported_codec       | 编码不支持   |
| 1004       | auth_failed             | 认证失败    |
| 1005       | auth_expired            | 认证过期    |
| 1006       | permission_revoked      | 权限撤销    |
| 1007       | session_conflict        | 会话冲突    |
| 1008       | rate_limited            | 限流      |
| 1009       | payload_too_large       | 消息过大    |
| 1010       | slow_consumer           | 慢消费者    |
| 1011       | server_overloaded       | 服务端过载   |
| 1012       | shard_moved             | 分片迁移    |
| 1013       | idle_timeout            | 空闲超时    |
| 1014       | client_upgrade_required | 客户端必须升级 |
| 1015       | policy_violation        | 策略违规    |

---

# 21. 心跳

Rift/1 使用 ping/pong 保活。

心跳策略由服务端在 ready 阶段下发。

| 字段               | 说明        |
| ---------------- | --------- |
| ping_interval_ms | ping 间隔   |
| pong_timeout_ms  | pong 超时   |
| max_missed_pongs | 最大连续丢失次数  |
| idle_timeout_ms  | 空闲连接超时    |
| jitter_ms        | 客户端心跳抖动范围 |

客户端不应在所有连接上同一时间发送心跳，必须加入 jitter。

服务端不得仅以一次 ping 失败判断连接死亡。

---

# 22. 分布式路由

Rift/1 设计上不假设 sticky session 必然存在。

## 22.1 Gateway

Gateway 负责：

1. 维护客户端连接。
2. 执行初步认证。
3. 执行连接级限流。
4. 转发 publish/subscribe。
5. 承接下行 fanout。
6. 处理连接级背压。
7. 上报连接状态。

## 22.2 Broker

Broker 负责：

1. topic 元数据。
2. topic 分片。
3. topic offset。
4. 消息持久化。
5. replay。
6. snapshot 协调。
7. fanout 路由。
8. dedupe。
9. topic 级限流。

## 22.3 Topic Sharding

Topic shard key 默认由 topic name 决定。

特殊场景可以使用 routing key：

| 场景   | routing key              |
| ---- | ------------------------ |
| 用户私信 | user_id                  |
| 群聊   | room_id                  |
| 文档协作 | doc_id                   |
| 游戏房间 | match_id                 |
| 组织通知 | org_id                   |
| 全站广播 | region 或 broadcast_group |

## 22.4 Fanout 策略

| 策略              | 说明                |
| --------------- | ----------------- |
| direct          | 小 topic，直接广播      |
| broker_fanout   | broker 统一 fanout  |
| gateway_fanout  | gateway 本地 fanout |
| tree_fanout     | 大 topic 分层 fanout |
| regional_fanout | 多区域 fanout        |
| edge_cache      | 边缘缓存最新状态          |
| pull_snapshot   | 超大 topic 改为快照拉取   |

---

# 23. 观测

Rift/1 必须内建协议级观测字段。

## 23.1 Trace

每条关键消息应携带 trace_id。

跨系统调用必须保留：

| 字段             | 说明      |
| -------------- | ------- |
| trace_id       | 链路 ID   |
| span_id        | 当前 span |
| parent_span_id | 父 span  |
| sampled        | 是否采样    |
| baggage        | 附加上下文   |

## 23.2 指标

实现必须暴露以下指标：

### 连接指标

| 指标                      | 说明    |
| ----------------------- | ----- |
| active_connections      | 当前连接数 |
| connection_open_total   | 连接创建数 |
| connection_close_total  | 连接关闭数 |
| reconnect_total         | 重连次数  |
| resume_success_total    | 恢复成功数 |
| resume_failed_total     | 恢复失败数 |
| heartbeat_timeout_total | 心跳超时数 |

### 消息指标

| 指标                      | 说明         |
| ----------------------- | ---------- |
| messages_in_total       | 入站消息数      |
| messages_out_total      | 出站消息数      |
| messages_dropped_total  | 丢弃消息数      |
| messages_replayed_total | replay 消息数 |
| messages_expired_total  | 过期消息数      |
| ack_timeout_total       | ack 超时数    |
| duplicate_total         | 重复消息数      |

### 延迟指标

| 指标                  | 说明          |
| ------------------- | ----------- |
| publish_latency_ms  | 发布延迟        |
| fanout_latency_ms   | fanout 延迟   |
| ack_latency_ms      | ack 延迟      |
| replay_latency_ms   | replay 延迟   |
| snapshot_latency_ms | snapshot 延迟 |
| queue_wait_ms       | 队列等待时间      |

### 背压指标

| 指标                   | 说明      |
| -------------------- | ------- |
| send_queue_depth     | 下行队列深度  |
| recv_queue_depth     | 上行队列深度  |
| slow_consumer_total  | 慢消费者数量  |
| flow_pause_total     | 暂停次数    |
| flow_resume_total    | 恢复次数    |
| volatile_drop_total  | 可丢消息丢弃数 |
| state_coalesce_total | 状态合并次数  |

---

# 24. 安全要求

Rift/1 生产环境必须运行在加密传输上。

## 24.1 基线要求

1. 必须使用 TLS 或等价加密通道。
2. token 不得出现在 URL query 中，除非是短期一次性 token。
3. 服务端必须限制 payload 大小。
4. 服务端必须限制 topic 订阅数量。
5. 服务端必须限制每连接发布速率。
6. 服务端必须限制认证失败次数。
7. 服务端必须防止 topic 枚举。
8. 服务端必须支持权限撤销。
9. 服务端必须校验 schema。
10. 服务端必须对高风险 command 做幂等保护。

## 24.2 防重放

对于敏感 command，客户端必须提供：

| 字段              | 说明     |
| --------------- | ------ |
| nonce           | 一次性随机数 |
| timestamp       | 客户端时间  |
| idempotency_key | 幂等键    |
| signature       | 可选签名   |

服务端必须拒绝过期 timestamp 和重复 nonce。

## 24.3 多端冲突

同一 client_id 多连接时，服务端必须有明确策略：

| 策略             | 说明              |
| -------------- | --------------- |
| allow_multi    | 允许多端同时在线        |
| replace_old    | 新连接踢掉旧连接        |
| reject_new     | 拒绝新连接           |
| device_scoped  | 按 device_id 区分  |
| session_scoped | 按 session_id 区分 |

默认推荐 `device_scoped`。

---

# 25. 兼容性与版本

Rift/1 使用 major/minor 版本。

## 25.1 协议版本

| 版本变化  | 说明     |
| ----- | ------ |
| major | 不兼容变更  |
| minor | 向后兼容变更 |

客户端 hello 必须声明支持的版本范围。

服务端必须选择双方都支持的最高兼容版本。

## 25.2 Feature Negotiation

功能必须通过 feature negotiation 开启。

常见 feature：

| Feature          | 说明         |
| ---------------- | ---------- |
| resume           | 会话恢复       |
| replay           | offset 重放  |
| snapshot         | 状态快照       |
| compression      | 压缩         |
| datagram         | 不可靠数据报     |
| multiplex        | 多 stream   |
| server_time_sync | 服务端时间同步    |
| binary_schema    | 二进制 schema |
| trace            | 链路追踪       |
| flow_control     | 显式流控       |

---

# 26. 实现约束

## 26.1 服务端实现

服务端实现必须满足：

1. 连接读写分离。
2. 每连接独立发送队列。
3. 每 topic 独立限流。
4. 所有业务消息必须 schema 校验。
5. 所有 publish 必须经过权限判断。
6. durable topic 必须分配 offset。
7. 支持 dedupe window。
8. 支持 graceful draining。
9. 支持慢消费者检测。
10. 支持观测指标导出。

## 26.2 客户端实现

客户端实现必须满足：

1. 维护连接状态机。
2. 支持指数退避重连。
3. 支持 jitter，避免重连风暴。
4. 持久化 last_offsets。
5. 处理 duplicate 消息。
6. 处理 replay 消息。
7. 处理 snapshot_required。
8. 尊重服务端 flow control。
9. 本地发送队列必须有上限。
10. UI 状态不得假设消息一定按业务期望到达。

## 26.3 SDK 约束

SDK 不得暴露无约束的字符串事件 API 作为主入口。

SDK 应暴露类型化接口：

1. 类型化 publish。
2. 类型化 subscribe。
3. 类型化 command。
4. 类型化 state。
5. 类型化 error。
6. 类型化 ack。
7. 类型化 topic handle。

允许提供 raw API，但必须标记为 unsafe 或 low-level。

---

# 27. 推荐默认配置

## 27.1 普通 Web 应用

| 参数                        | 默认值       |
| ------------------------- | --------- |
| transport                 | WebSocket |
| codec                     | CBOR      |
| fallback codec            | JSON      |
| ping_interval_ms          | 25000     |
| pong_timeout_ms           | 10000     |
| max_payload_bytes         | 65536     |
| max_topics_per_connection | 128       |
| max_send_queue_bytes      | 1048576   |
| reconnect_base_ms         | 500       |
| reconnect_max_ms          | 15000     |
| replay_window_sec         | 300       |

## 27.2 聊天应用

| 参数            | 默认值                |
| ------------- | ------------------ |
| message class | event              |
| delivery      | durable_ordered    |
| ordering      | topic              |
| ack           | persisted          |
| retention     | ttl 或 durable      |
| replay        | enabled            |
| snapshot      | room metadata only |
| dedupe        | client_message_id  |

## 27.3 协作编辑

| 参数            | 默认值             |
| ------------- | --------------- |
| operation log | durable_ordered |
| cursor        | latest_per_key  |
| presence      | heartbeat_state |
| ordering      | causal 或 topic  |
| snapshot      | required        |
| replay        | required        |
| dedupe        | operation_id    |
| ack           | persisted       |

## 27.4 游戏房间

| 参数                  | 默认值                            |
| ------------------- | ------------------------------ |
| player input        | datagram                       |
| authoritative state | state                          |
| match event         | event                          |
| delivery            | best_effort / at_least_once 混合 |
| ordering            | key                            |
| tick state          | latest_only                    |
| replay              | disabled                       |
| snapshot            | latest state                   |

---

# 28. 与 Socket.IO 的设计差异

| 维度     | Socket.IO        | Rift/1                   |
| ------ | ---------------- | ------------------------ |
| 协议模型   | 事件驱动             | 事件、命令、状态、流、数据报分离         |
| 可靠性    | 默认 at most once  | 每条消息显式声明                 |
| 重连恢复   | session recovery | offset replay + snapshot |
| room   | 内存房间抽象           | 可路由 topic                |
| schema | 非强制              | 强制 schema-first          |
| ack    | API 层 ack        | 协议级 ack 类型               |
| 背压     | 不够显式             | 协议级 flow control         |
| 分布式    | adapter 扩展       | topic shard 原生模型         |
| 观测     | 依赖外部             | 协议级 trace/metrics        |
| 高频状态   | event 模拟         | state/datagram 原生支持      |
| 类型安全   | 依赖应用             | 协议要求 schema registry     |

---

# 29. 最小合规实现

一个最小 Rift/1 实现必须支持：

1. WebSocket transport。
2. hello / welcome / ready。
3. token 认证。
4. topic subscribe / unsubscribe。
5. event publish。
6. message_id。
7. at_least_once delivery。
8. server ack。
9. topic offset。
10. reconnect resume。
11. replay。
12. structured error。
13. heartbeat。
14. payload size limit。
15. per-connection send queue limit。
16. JSON debug codec。
17. 至少一种二进制 codec。

不支持 schema registry、topic profile、snapshot、flow control 的实现只能称为 Rift/1 Lite，不能称为完整 Rift/1。

---

# 30. 推荐模块划分

服务端推荐模块：

| 模块              | 职责                            |
| --------------- | ----------------------------- |
| Transport       | WebSocket/WebTransport/TCP 接入 |
| Session         | 会话、认证、恢复                      |
| Codec           | 编解码、压缩                        |
| Schema Registry | schema 注册与校验                  |
| Router          | topic 路由                      |
| Broker Client   | 内部 broker 通信                  |
| Topic Store     | topic 元数据                     |
| Offset Store    | offset 与 replay               |
| Snapshot Store  | 快照                            |
| Dedupe Store    | 幂等去重                          |
| Fanout Engine   | 广播                            |
| Flow Controller | 背压                            |
| Authz Engine    | 权限                            |
| Metrics         | 指标                            |
| Trace           | 链路追踪                          |
| Admin API       | 管理接口                          |

客户端推荐模块：

| 模块               | 职责            |
| ---------------- | ------------- |
| Connection       | 底层连接          |
| Session          | hello、认证、恢复   |
| Codec            | 编解码           |
| Registry         | 本地 schema     |
| Topic Client     | 订阅发布          |
| Offset Tracker   | offset 持久化    |
| Replay Handler   | replay 处理     |
| Snapshot Handler | 快照处理          |
| Command Client   | command/reply |
| State Store      | 本地状态合并        |
| Flow Handler     | 背压处理          |
| Retry Policy     | 重试与退避         |

---

# 31. 协议设计原则

Rift/1 的实现必须遵守以下原则：

1. 任何消息语义都必须显式声明。
2. 任何可恢复状态都必须绑定 offset 或 snapshot。
3. 任何业务事件都必须绑定 schema。
4. 任何权限都不能只在连接时校验一次。
5. 任何队列都必须有上限。
6. 任何重试都必须有退避。
7. 任何重复消息都必须可识别。
8. 任何过期消息都必须可丢弃。
9. 任何 topic 都必须有 profile。
10. 任何大规模 fanout 都不能依赖单机内存。
11. 任何断线恢复都不能假设服务端仍保留连接对象。
12. 任何 SDK 都不能鼓励无类型字符串事件泛滥。

---

# 32. 结论

Rift/1 的核心不是“比 WebSocket 多一个事件封装”，而是把实时系统中最容易腐烂的部分变成协议约束：

1. 消息语义。
2. 可靠性。
3. 重连恢复。
4. topic 路由。
5. 状态快照。
6. 背压。
7. 权限。
8. schema。
9. 观测。
10. 分布式部署。

Socket.IO 的价值在于易用；Rift/1 的目标是在保留易用性的同时，让协议边界更硬、状态恢复更可靠、分布式模型更干净、工程实现更适合长期维护。

