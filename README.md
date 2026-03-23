# Market Intelligence Monitor

实时市场情报监控系统，自动采集全球财经新闻，AI 翻译总结后推送到 Telegram。

专注领域：黄金/原油价格、加密货币、Trump 政策动态、地缘政治、央行货币政策。

## 功能

- 14 个 RSS 源（Bloomberg、WSJ、CNBC、CoinDesk、OilPrice、Google News 等）
- AI 智能分级（替代关键词匹配，精准过滤无关新闻）
- AI 语义去重（跨源识别同一事件，合并后只推送一条）
- AI 翻译 + 总结（英文新闻自动翻译为中文摘要）
- 价格异动监控（黄金、原油 WTI、BTC）
- Telegram 实时推送（带重试和失败队列）
- 每日简报（每天 08:00 自动发送）
- 健康检查（每小时检测 RSS 源可用性）

## 架构

- Python asyncio 异步并发
- feedparser RSS 解析
- aiohttp HTTP 客户端（Session 复用）
- scikit-learn TF-IDF 基础去重
- SQLite 数据存储
- OpenAI 兼容 API（AI 分级/去重/翻译）
- systemd 服务守护

## 部署

```bash
# 安装依赖
pip3 install feedparser aiohttp scikit-learn

# 配置
cp config.example.json config.json
# 编辑 config.json 填入：
#   ai.api_key — OpenAI 兼容 API 密钥
#   notification.telegram_bot_token — Telegram Bot Token
#   notification.telegram_chat_id — Telegram Chat ID

# 创建目录
mkdir -p logs data

# 运行
python3 monitor.py

# systemd 服务（可选）
sudo cp market-intel.service /etc/systemd/system/
sudo systemctl enable market-intel.service
sudo systemctl start market-intel.service
```

## 配置示例

见 `config.example.json`

## License

MIT
