# 灾害预警插件

多数据源灾害预警插件，支持地震预警、地震速报、海啸预警和气象预警实时推送。

## 功能

- 🌍 **多数据源**：FAN Studio、P2P Earthquake、Wolfx、Global Quake
- 🧭 **多区域地震**：中国大陆、台湾、日本、全球地震信息
- 🌊 **海啸预警**：中国和日本海啸预警消息
- ⛈️ **气象预警**：中国气象局预警，支持颜色级别过滤
- 🔌 **WebSocket 实时推送**：低延迟接收灾害信息，支持自动重连
- 📊 **智能过滤**：震级、烈度、震度、关键词黑白名单和推送频率控制
- 📍 **本地烈度监控**：按本地坐标和烈度阈值进行关注区域过滤
- 💬 **微信推送**：支持推送到指定群聊或好友

## 数据源

| 数据源 | 默认状态 | 主要内容 |
|--------|----------|----------|
| `fans_studio` | 启用 | 中国地震预警、台湾 CWA、日本 JMA、USGS、中国气象预警、海啸预警 |
| `p2p_earthquake` | 启用 | 日本 JMA 地震预警、地震情报、海啸预警 |
| `wolfx` | 启用 | 日本 JMA、中国 CENC、台湾 CWA 地震预警和速报 |
| `global_quake` | 启用 | Global Quake Protobuf 全球地震监测 |

## 命令

群聊中消息需要包含 `灾害预警` 或 `disaster` 触发词；私聊可直接发送命令。

| 命令 | 权限 | 说明 |
|------|------|------|
| `/灾害预警` | 所有人 | 显示帮助信息 |
| `/灾害预警状态` | 所有人 | 查看运行状态、连接数、事件数和推送目标数量 |
| `/灾害预警统计` | 所有人 | 查看简要统计 |
| `/灾害预警重连` | 管理员 | 强制重连所有数据源 |
| `/灾害预警推送开关` | 管理员 | 临时开启或关闭推送，不写回配置文件 |
| `/灾害预警模拟 <纬度> <经度> <震级> [深度]` | 管理员 | 模拟地震事件并走完整过滤、格式化和推送流程 |

管理员优先读取 `[DisasterWarning].admin_wxids`；未配置时会尝试读取 `main_config.toml` 中的 `[XYBot].admins`。

## 配置

编辑 `plugins/DisasterWarning/config.toml`：

```toml
[DisasterWarning]
enabled = true
# admin_wxids = ["wxid_xxx"]
push_targets = ["群聊或好友 wxid"]

[fans_studio]
enabled = true
url = "wss://ws.fanstudio.tech/all"
backup_url = "wss://ws.fanstudio.hk/all"

[p2p_earthquake]
enabled = true
url = "wss://api.p2pquake.net/v2/ws"

[wolfx]
enabled = true
url = "wss://ws-api.wolfx.jp/all_eew"

[global_quake]
enabled = true
url = "wss://gqm.aloys23.link/ws"

[local_monitoring]
enabled = true
latitude = 30.5728
longitude = 104.0668
place_name = "成都市"
strict_mode = false
intensity_threshold = 1.0

[earthquake_filters]
usgs_min_magnitude = 1.5
domestic_min_magnitude = 1.0
overseas_min_magnitude = 4.0
min_intensity = 1.0
min_scale = 1.0
blacklist_keywords = []
whitelist_keywords = []

[push_frequency]
cea_cwa_report_n = 1
jma_report_n = 1
gq_report_n = 1
final_report_always_push = true

[weather]
min_color_level = "白色"
keywords = []
max_description_length = 512
enable_icon = true
```

### 推送目标

- `push_targets` 非空：只推送到配置的 wxid 列表。
- `push_targets` 为空：不主动发送微信消息，仅记录日志。

### 过滤规则

- `domestic_min_magnitude`：国内地震最低震级。
- `overseas_min_magnitude`：海外地震最低震级。
- `usgs_min_magnitude`：USGS 等全球地震源最低震级。
- `min_intensity`：最低烈度。
- `min_scale`：最低日本震度。
- `blacklist_keywords`：命中任一关键词则过滤。
- `whitelist_keywords`：非空时，仅推送命中关键词的地震。

### 气象预警

`min_color_level` 支持：`白色` < `蓝色` < `黄色` < `橙色` < `红色`。

## 安装与启用

1. 安装依赖：`pip install aiohttp pydantic protobuf`
2. 按需修改 `plugins/DisasterWarning/config.toml`
3. 重启 AllBot，或在管理后台启用 `DisasterWarning` 插件
4. 发送 `/灾害预警状态` 确认数据源连接状态

## 调试

- 使用 `/灾害预警模拟 30.5728 104.0668 4.5 10` 验证推送链路。
- 设置 `[debug].startup_silence_seconds` 可在启动后短时间静默，避免历史消息触发推送。
- 设置 `[debug].enable_raw_logging = true` 可按实现记录原始调试日志。
