# Market Intelligence Monitor

实时市场情报监控系统，自动拉取、翻译、分析并推送重要财经新闻。

## 功能

- 📰 7 个权威 RSS 源（Bloomberg、WSJ、CNBC、FT 等）
- 🤖 AI 自动翻译 + 总结（Claude Opus 4.6）
- 🎯 智能分级（紧急/重要/普通）
- 📱 Telegram 实时推送
- 💾 SQLite 数据库存档

## 架构

- Python asyncio 异步并发
- feedparser RSS 解析
- aiohttp HTTP 客户端
- SQLite 数据存储
- systemd 服务守护

## 部署

```bash
# 安装依赖
pip3 install feedparser aiohttp

# 配置
cp config.example.json config.json
# 编辑 config.json 填入 API Key

# 运行
python3 monitor.py

# systemd 服务
sudo cp market-intel.service /etc/systemd/system/
sudo systemctl enable market-intel.service
sudo systemctl start market-intel.service
```

## 配置示例

见 `config.example.json`

## License

MIT
