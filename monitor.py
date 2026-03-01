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
            # 去重检查
            if self.check_duplicate(item["title"], item.get("content", "")):
                continue
            
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
            
            await self.send_telegram_with_retry(message, priority)

            c.execute("UPDATE news SET analyzed = 1 WHERE id = ?", (news_id,))

        conn.commit()
        conn.close()

    async def run(self):
        logger.info("市场情报监控启动")
        logger.info(f"RSS 源: {len(self.config['rss_feeds'])} 个")
        logger.info(f"轮询: {self.config['polling_interval']}s")

        while True:
            loop_count = 0
            try:
                tasks = [self.fetch_rss(feed) for feed in self.config["rss_feeds"]]
                results = await asyncio.gather(*tasks)

                all_items = []
                for items in results:
                    all_items.extend(items)

                new_count = self.save_news(all_items)

                if new_count > 0:
                    await self.analyze_and_notify()

                loop_count += 1
                
                # 每小时健康检查（12次循环 = 1小时）
                if loop_count % 12 == 0:
                    await self.health_check()
                
                # 每30分钟价格监控（6次循环）
                if loop_count % 6 == 0:
                    await self.check_price_alerts()
                
                # 每天早上8点发送简报
                from datetime import datetime
                if datetime.now().hour == 8 and datetime.now().minute < 5:
                    await self.daily_digest()
                
                await asyncio.sleep(self.config["polling_interval"])

            except Exception as e:
                logger.error(f"主循环异常: {e}")
                await asyncio.sleep(60)

    def check_duplicate(self, title: str, content: str) -> bool:
        """检查新闻是否与最近24h的新闻重复（语义相似度）"""
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute("""
            SELECT title, content FROM news
            WHERE created_at > datetime('now', '-24 hours')
            ORDER BY created_at DESC
            LIMIT 50
        """)
        
        recent_news = c.fetchall()
        conn.close()
        
        if not recent_news:
            return False
        
        current_text = f"{title} {content[:200]}"
        history_texts = [f"{t} {c[:200]}" for t, c in recent_news]
        
        try:
            vectorizer = TfidfVectorizer(max_features=100)
            all_texts = [current_text] + history_texts
            tfidf_matrix = vectorizer.fit_transform(all_texts)
            similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()
            max_similarity = similarities.max() if len(similarities) > 0 else 0
            
            if max_similarity > 0.7:
                logger.info(f"[去重] 相似度 {max_similarity:.2f}: {title[:30]}...")
                return True
        except Exception as e:
            logger.error(f"[去重] 异常: {e}")
            
        return False

    def is_quiet_hours(self) -> bool:
        """检查是否在静音时段（23:00-08:00）"""
        from datetime import datetime
        hour = datetime.now().hour
        return hour >= 23 or hour < 8

    async def health_check(self):
        """健康检查：RSS源可用性"""
        failed_feeds = []
        
        for feed in self.config["rss_feeds"]:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(feed["url"], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status != 200:
                            failed_feeds.append(feed["name"])
            except Exception as e:
                failed_feeds.append(feed["name"])
                logger.error(f"[健康检查] {feed[name]} 失败: {e}")
        
        if failed_feeds:
            alert = f"⚠️ <b>RSS源异常</b>\n\n以下源无法访问：\n" + "\n".join([f"• {name}" for name in failed_feeds])
            await self.send_telegram(alert, "urgent")
            logger.warning(f"[健康检查] {len(failed_feeds)} 个源失败")
        else:
            logger.info("[健康检查] 所有RSS源正常")

    async def check_price_alerts(self):
        """价格异动监控（黄金/BTC）"""
        alerts = []
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1d&range=5d") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                        current = prices[-1]
                        prev = prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        
                        if abs(change_pct) > 2:
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🟡 黄金{direction} {abs(change_pct):.2f}%，当前 ${current:.2f}/盎司")
                
                async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?interval=1d&range=5d") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                        current = prices[-1]
                        prev = prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        
                        if abs(change_pct) > 5:
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🔴 BTC{direction} {abs(change_pct):.2f}%，当前 ${current:,.0f}")
        
        except Exception as e:
            logger.error(f"[价格监控] 异常: {e}")
        
        for alert in alerts:
            await self.send_telegram(alert, "urgent")
            logger.info(f"[价格监控] {alert}")


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

    def check_duplicate(self, title: str, content: str) -> bool:
        """检查新闻是否与最近24h的新闻重复（语义相似度）"""
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # 获取最近24h的新闻
        c.execute("""
            SELECT title, content FROM news
            WHERE created_at > datetime('now', '-24 hours')
            ORDER BY created_at DESC
            LIMIT 50
        """)
        
        recent_news = c.fetchall()
        conn.close()
        
        if not recent_news:
            return False
        
        # 当前新闻文本
        current_text = f"{title} {content[:200]}"
        
        # 历史新闻文本
        history_texts = [f"{t} {c[:200]}" for t, c in recent_news]
        
        # TF-IDF 向量化
        try:
            vectorizer = TfidfVectorizer(max_features=100)
            all_texts = [current_text] + history_texts
            tfidf_matrix = vectorizer.fit_transform(all_texts)
            
            # 计算相似度
            similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()
            
            # 相似度阈值 0.7（70%以上认为重复）
            max_similarity = similarities.max() if len(similarities) > 0 else 0
            
            if max_similarity > 0.7:
                logger.info(f"[去重] 相似度 {max_similarity:.2f}: {title[:30]}...")
                return True
                
        except Exception as e:
            logger.error(f"[去重] 异常: {e}")
            
        return False

    def is_quiet_hours(self) -> bool:
        """检查是否在静音时段（23:00-08:00）"""
        from datetime import datetime
        hour = datetime.now().hour
        return hour >= 23 or hour < 8

    async def daily_digest(self):
        """生成每日简报"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # 获取今天的重要新闻
        c.execute("""
            SELECT source, title, priority, COUNT(*) as cnt
            FROM news
            WHERE created_at > date(now)
            AND priority IN (urgent, important)
            GROUP BY priority
            ORDER BY 
                CASE priority 
                    WHEN urgent THEN 1 
                    WHEN important THEN 2 
                END
        """)
        
        stats = c.fetchall()
        
        # 获取具体新闻
        c.execute("""
            SELECT source, title, url, priority
            FROM news
            WHERE created_at > date(now)
            AND priority IN (urgent, important)
            ORDER BY created_at DESC
            LIMIT 10
        """)
        
        news_list = c.fetchall()
        conn.close()
        
        if not news_list:
            return
        
        # 组装简报
        digest = "📊 <b>今日市场简报</b>\n\n"
        
        for priority, count in [(p, sum(1 for x in stats if x[2] == p)) for p in [urgent, important]]:
            if count > 0:
                emoji = "🔴" if priority == "urgent" else "🟡"
                digest += f"{emoji} {priority.upper()}: {count} 条\n"
        
        digest += "\n<b>重点新闻：</b>\n"
        for i, (source, title, url, priority) in enumerate(news_list[:5], 1):
            emoji = "🔴" if priority == "urgent" else "🟡"
            digest += f"{i}. {emoji} [{source}] {title[:50]}...\n"
        
        await self.send_telegram(digest, "important")
        logger.info("[简报] 每日简报已发送")

    async def health_check(self):
        """健康检查：RSS源可用性"""
        failed_feeds = []
        
        for feed in self.config["rss_feeds"]:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(feed["url"], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status != 200:
                            failed_feeds.append(feed["name"])
            except Exception as e:
                failed_feeds.append(feed["name"])
                logger.error(f"[健康检查] {feed[name]} 失败: {e}")
        
        if failed_feeds:
            alert = f"⚠️ <b>RSS源异常</b>\n\n以下源无法访问：\n" + "\n".join([f"• {name}" for name in failed_feeds])
            await self.send_telegram(alert, "urgent")
            logger.warning(f"[健康检查] {len(failed_feeds)} 个源失败")
        else:
            logger.info("[健康检查] 所有RSS源正常")

    async def send_telegram_with_retry(self, message: str, priority: str = "normal", max_retries: int = 3):
        """带重试的Telegram推送"""
        for attempt in range(max_retries):
            try:
                await self.send_telegram_with_retry(message, priority)
                return True
            except Exception as e:
                logger.error(f"[推送] 第{attempt+1}次失败: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5 * (attempt + 1))  # 指数退避
        
        # 所有重试失败，记录到失败队列
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS failed_pushes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                priority TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("INSERT INTO failed_pushes (message, priority) VALUES (?, ?)", (message, priority))
        conn.commit()
        conn.close()
        logger.error(f"[推送] 失败队列已保存")
        return False

    async def check_price_alerts(self):
        """价格异动监控（黄金/BTC）"""
        alerts = []
        
        try:
            async with aiohttp.ClientSession() as session:
                # 黄金价格（GC=F）
                async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1d&range=5d") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                        current = prices[-1]
                        prev = prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        
                        if abs(change_pct) > 2:  # 涨跌超过2%
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🟡 黄金{direction} {abs(change_pct):.2f}%，当前 ${current:.2f}/盎司")
                
                # BTC价格
                async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?interval=1d&range=5d") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                        current = prices[-1]
                        prev = prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        
                        if abs(change_pct) > 5:  # 涨跌超过5%
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🔴 BTC{direction} {abs(change_pct):.2f}%，当前 ${current:,.0f}")
        
        except Exception as e:
            logger.error(f"[价格监控] 异常: {e}")
        
        # 推送异动告警
        for alert in alerts:
            await self.send_telegram(alert, "urgent")
            logger.info(f"[价格监控] {alert}")
