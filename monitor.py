#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import sqlite3
from typing import Dict, List
import aiohttp
import feedparser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MarketIntelMonitor:
    def __init__(self, config_path: str = "config.json"):
        with open(config_path) as f:
            self.config = json.load(f)
        self.db_path = "data/intel.db"
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                url TEXT,
                published_at TEXT,
                priority TEXT,
                analyzed BOOLEAN DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, url)
            )
        """)
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化: {self.db_path}")

    async def fetch_rss(self, feed: Dict) -> List[Dict]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(feed["url"], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    content = await resp.text()

            parsed = feedparser.parse(content)
            items = []

            for entry in parsed.entries[:10]:
                item = {
                    "source": feed["name"],
                    "title": entry.get("title", ""),
                    "content": entry.get("summary", ""),
                    "url": entry.get("link", ""),
                    "published_at": entry.get("published", "")
                }
                items.append(item)

            logger.info(f"[RSS] {feed['name']}: {len(items)} 条")
            return items

        except Exception as e:
            logger.error(f"[RSS] {feed['name']} 失败: {e}")
            return []

    def classify_priority(self, title: str, content: str) -> str:
        text = (title + " " + content).lower()

        for keyword in self.config["priority_levels"]["urgent"]:
            if keyword in text:
                return "urgent"

        for keyword in self.config["priority_levels"]["important"]:
            if keyword in text:
                return "important"

        return "normal"

    def save_news(self, items: List[Dict]):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        new_count = 0
        for item in items:
            priority = self.classify_priority(item["title"], item.get("content", ""))
            try:
                c.execute("""
                    INSERT INTO news (source, title, content, url, published_at, priority)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    item["source"],
                    item["title"],
                    item.get("content", ""),
                    item.get("url", ""),
                    item.get("published_at", ""),
                    priority
                ))
                new_count += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()
        conn.close()

        if new_count > 0:
            logger.info(f"新增 {new_count} 条新闻")

        return new_count

    async def send_telegram(self, message: str, priority: str = "normal"):
        bot_token = self.config["notification"]["telegram_bot_token"]
        chat_id = self.config["notification"]["telegram_chat_id"]

        emoji = {"urgent": "🔴", "important": "🟡", "normal": "🟢"}
        text = f"{emoji.get(priority, )} {message}"

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info(f"[TG] 推送: {priority}")
                    else:
                        logger.error(f"[TG] 失败: {await resp.text()}")
        except Exception as e:
            logger.error(f"[TG] 异常: {e}")

    async def ai_analyze(self, news_item):
        """AI 分析新闻"""
        prompt = f"""你是市场情报分析师。请用中文总结以下新闻（如果是英文请先翻译）：

标题：{news_item["title"]}
内容：{news_item.get("content", "")[:500]}

要求：
1. 用1-2句话总结核心内容（中文）
2. 说明对市场的影响（正面/负面/中性）
3. 影响哪些资产（股票/债券/黄金/加密货币）

直接输出中文总结，不超过80字。"""
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": self.config["ai"]["model"],
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.3
                }
                headers = {
                    "Authorization": f"Bearer {self.config['ai']['api_key']}",
                    "Content-Type": "application/json"
                }
                
                async with session.post(
                    f"{self.config['ai']['base_url']}/chat/completions",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data["choices"][0]["message"]["content"].strip()
                    else:
                        logger.error(f"[AI] 分析失败: {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"[AI] 异常: {e}")
            return None



    async def analyze_and_notify(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            SELECT id, source, title, content, url, priority
            FROM news
            WHERE analyzed = 0 AND priority IN ('urgent', 'important')
            ORDER BY created_at DESC
            LIMIT 5
        """)

        rows = c.fetchall()

        for row in rows:
            news_id, source, title, content, url, priority = row

            # 所有重要/紧急新闻都翻译总结
            analysis = await self.ai_analyze({'title': title, 'content': content})
            if analysis:
                message = f"<b>[{source}]</b>\n\n💡 {analysis}\n\n原标题: {title}\n{url}"
            else:
                message = f"<b>[{source}]</b> {title}\n{url}"
            await self.send_telegram(message, priority)

            c.execute("UPDATE news SET analyzed = 1 WHERE id = ?", (news_id,))

        conn.commit()
        conn.close()

    async def run(self):
        logger.info("市场情报监控启动")
        logger.info(f"RSS 源: {len(self.config['rss_feeds'])} 个")
        logger.info(f"轮询: {self.config['polling_interval']}s")

        while True:
            try:
                tasks = [self.fetch_rss(feed) for feed in self.config["rss_feeds"]]
                results = await asyncio.gather(*tasks)

                all_items = []
                for items in results:
                    all_items.extend(items)

                new_count = self.save_news(all_items)

                if new_count > 0:
                    await self.analyze_and_notify()

                await asyncio.sleep(self.config["polling_interval"])

            except Exception as e:
                logger.error(f"主循环异常: {e}")
                await asyncio.sleep(60)

if __name__ == "__main__":
    monitor = MarketIntelMonitor()
    asyncio.run(monitor.run())
    async def ai_analyze(self, news_item: Dict) -> Optional[str]:
        """AI 分析新闻"""
        prompt = f"""你是市场情报分析师。请用中文总结以下新闻（如果是英文请先翻译）：

标题：{news_item["title"]}
内容：{news_item.get("content", "")[:500]}

要求：
1. 用1-2句话总结核心内容（中文）
2. 说明对市场的影响（正面/负面/中性）
3. 影响哪些资产（股票/债券/黄金/加密货币）

直接输出中文总结，不超过80字。"""
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": self.config["ai"]["model"],
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.3
                }
                headers = {
                    "Authorization": f"Bearer {self.config['ai']['api_key']}",
                    "Content-Type": "application/json"
                }
                
                async with session.post(
                    f"{self.config['ai']['base_url']}/chat/completions",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data["choices"][0]["message"]["content"].strip()
                    else:
                        logger.error(f"[AI] 分析失败: {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"[AI] 异常: {e}")
            return None
