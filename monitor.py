#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import sqlite3
import signal
from datetime import datetime
from typing import Dict, List, Optional

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
        self.session: Optional[aiohttp.ClientSession] = None
        self._running = True
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
                ai_summary TEXT,
                event_group TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, url)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS failed_pushes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                priority TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化: {self.db_path}")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # ── RSS 抓取 ──────────────────────────────────────────────

    async def fetch_rss(self, feed: Dict) -> List[Dict]:
        try:
            session = await self._get_session()
            async with session.get(feed["url"], timeout=aiohttp.ClientTimeout(total=15)) as resp:
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

    # ── 去重：TF-IDF 基础去重（入库阶段） ─────────────────────

    def check_duplicate(self, title: str, content: str) -> bool:
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

    # ── AI 智能分级 ───────────────────────────────────────────

    async def ai_classify_priority(self, news_items: List[Dict]) -> List[Dict]:
        """用 AI 批量判断新闻优先级和相关性"""
        if not news_items:
            return []

        titles_text = "\n".join(
            f"{i+1}. [{item['source']}] {item['title']}"
            for i, item in enumerate(news_items)
        )

        prompt = f"""你是市场情报过滤器。用户只关心以下领域的新闻：
1. 黄金/贵金属价格走势及影响因素
2. 原油/能源价格走势及影响因素
3. 加密货币（BTC/ETH等）价格走势及影响因素
4. Trump 政策动态（关税、贸易战、制裁等影响市场的决策）
5. 地缘政治冲突（战争、制裁、军事行动）
6. 央行货币政策（美联储/ECB/BOJ 利率决议、QE/QT）
7. 重大经济数据（非农、CPI、GDP）

严格过滤规则：
- urgent: 正在发生或即将发生的、会直接冲击上述资产价格的事件
- important: 与上述领域相关但影响较间接的事件
- normal: 与上述7个领域无关的新闻，一律标为 normal

必须标为 normal 的类型：
- 每日汇总/回顾类（"what happened today"、"daily roundup"、"weekly recap"）— 无增量信息
- 行业社会新闻（公司人事变动、高管绑架/诉讼/丑闻）— 不影响价格
- 娱乐、体育、科技产品发布
- 单个小币种/NFT/DeFi 项目动态（除非涉及重大监管或系统性风险）

核心原则：只推送会让交易者立刻采取行动的新闻。宁可漏推也不要误推。

新闻列表：
{titles_text}

请严格按以下 JSON 格式返回，不要添加任何其他文字：
[{{"index": 1, "priority": "urgent"}}, {{"index": 2, "priority": "normal"}}]"""

        try:
            result = await self._call_ai(prompt, max_tokens=300)
            if not result:
                return news_items

            import re
            json_match = re.search(r'\[.*\]', result, re.DOTALL)
            if not json_match:
                return news_items

            priorities = json.loads(json_match.group())
            priority_map = {p["index"]: p["priority"] for p in priorities}

            for i, item in enumerate(news_items):
                ai_priority = priority_map.get(i + 1, "normal")
                if ai_priority in ("urgent", "important", "normal"):
                    item["priority"] = ai_priority

        except Exception as e:
            logger.error(f"[AI分级] 异常: {e}")

        return news_items

    # ── AI 语义去重（推送阶段） ───────────────────────────────

    async def ai_deduplicate(self, news_rows: list) -> list:
        """用 AI 识别同一事件的不同报道，合并后返回去重结果"""
        if len(news_rows) <= 1:
            return news_rows

        titles_text = "\n".join(
            f"{i+1}. [{row[1]}] {row[2]}"  # row: id, source, title, content, url, priority
            for i, row in enumerate(news_rows)
        )

        prompt = f"""以下新闻来自不同来源，请将报道同一事件或同一主题的新闻分为一组。

分组标准（宽松合并）：
- 同一具体事件的不同报道 → 合并（如：多条关于"Trump暂停打击伊朗"的报道）
- 同一主题不同角度 → 合并（如："沙特减产对亚洲影响" 和 "伊朗战争影响原油出口" 都是"中东局势影响原油供应"主题）
- 完全不同的事件 → 分开

新闻列表：
{titles_text}

请严格按以下 JSON 格式返回，每个 group 是报道同一事件的新闻编号列表：
[{{"group": [1, 3], "best": 1}}, {{"group": [2], "best": 2}}, {{"group": [4, 5], "best": 4}}]

其中 best 是该组中信息最完整的那条编号。不要添加其他文字。"""

        try:
            result = await self._call_ai(prompt, max_tokens=300)
            if not result:
                return news_rows

            import re
            json_match = re.search(r'\[.*\]', result, re.DOTALL)
            if not json_match:
                return news_rows

            groups = json.loads(json_match.group())
            deduped = []
            seen_indices = set()

            for g in groups:
                best_idx = g.get("best", g["group"][0])
                if best_idx <= len(news_rows) and best_idx not in seen_indices:
                    row = news_rows[best_idx - 1]
                    # 标注其他来源
                    other_sources = []
                    for idx in g["group"]:
                        seen_indices.add(idx)
                        if idx != best_idx and idx <= len(news_rows):
                            other_sources.append(news_rows[idx - 1][1])  # source name
                    deduped.append((*row, other_sources))

            # 处理未被分组的新闻
            for i, row in enumerate(news_rows):
                if (i + 1) not in seen_indices:
                    deduped.append((*row, []))

            logger.info(f"[AI去重] {len(news_rows)} 条 -> {len(deduped)} 条")
            return deduped

        except Exception as e:
            logger.error(f"[AI去重] 异常: {e}")
            return [(*row, []) for row in news_rows]

    # ── AI 分析 ───────────────────────────────────────────────

    async def _call_ai(self, prompt: str, max_tokens: int = 200) -> Optional[str]:
        try:
            session = await self._get_session()
            payload = {
                "model": self.config["ai"]["model"],
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
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
                    logger.error(f"[AI] 请求失败: {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"[AI] 异常: {e}")
            return None

    async def ai_analyze(self, news_item: Dict) -> Optional[str]:
        prompt = f"""你是市场情报分析师。请用中文总结以下新闻（如果是英文请先翻译）：

标题：{news_item["title"]}
内容：{news_item.get("content", "")[:500]}

要求：
1. 用1-2句话总结核心内容（中文）
2. 说明对市场的影响（正面/负面/中性）
3. 影响哪些资产（股票/债券/黄金/原油/加密货币）

直接输出中文总结，不超过80字。"""
        return await self._call_ai(prompt)

    # ── 关键词预过滤（快速筛选，减少 AI 调用） ────────────────

    def classify_priority(self, title: str, content: str) -> str:
        """关键词预过滤，作为 AI 分级前的快速筛选"""
        text = (title + " " + content).lower()

        for keyword in self.config["priority_levels"]["urgent"]:
            if keyword.lower() in text:
                return "urgent"

        for keyword in self.config["priority_levels"]["important"]:
            if keyword.lower() in text:
                return "important"

        return "normal"

    # ── 入库 ──────────────────────────────────────────────────

    def save_news(self, items: List[Dict]) -> int:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        new_count = 0
        for item in items:
            if self.check_duplicate(item["title"], item.get("content", "")):
                continue

            priority = item.get("priority", self.classify_priority(item["title"], item.get("content", "")))
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

    # ── Telegram 推送 ─────────────────────────────────────────

    async def send_telegram(self, message: str, priority: str = "normal"):
        bot_token = self.config["notification"]["telegram_bot_token"]
        chat_id = self.config["notification"]["telegram_chat_id"]

        emoji = {"urgent": "🔴", "important": "🟡", "normal": "🟢"}
        text = f"{emoji.get(priority, '🟢')} {message}"

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    logger.info(f"[TG] 推送: {priority}")
                else:
                    logger.error(f"[TG] 失败: {await resp.text()}")
        except Exception as e:
            logger.error(f"[TG] 异常: {e}")

    async def send_telegram_with_retry(self, message: str, priority: str = "normal", max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                await self.send_telegram(message, priority)
                return True
            except Exception as e:
                logger.error(f"[推送] 第{attempt+1}次失败: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5 * (attempt + 1))

        # 所有重试失败，记录到失败队列
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("INSERT INTO failed_pushes (message, priority) VALUES (?, ?)", (message, priority))
        conn.commit()
        conn.close()
        logger.error("[推送] 失败队列已保存")
        return False

    # ── 分析 + 去重 + 推送 ────────────────────────────────────

    async def analyze_and_notify(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            SELECT id, source, title, content, url, priority
            FROM news
            WHERE analyzed = 0 AND priority IN ('urgent', 'important')
            ORDER BY created_at DESC
            LIMIT 10
        """)

        rows = c.fetchall()
        if not rows:
            conn.close()
            return

        # AI 语义去重：识别同一事件的不同报道
        deduped = await self.ai_deduplicate(rows)

        for item in deduped:
            # item: (id, source, title, content, url, priority, other_sources)
            news_id, source, title, content, url, priority = item[0], item[1], item[2], item[3], item[4], item[5]
            other_sources = item[6] if len(item) > 6 else []

            analysis = await self.ai_analyze({'title': title, 'content': content})

            source_tag = f"<b>[{source}]</b>"
            if other_sources:
                source_tag += f" (综合 {', '.join(other_sources)})"

            if analysis:
                message = f"{source_tag}\n\n💡 {analysis}\n\n原标题: {title}\n{url}"
            else:
                message = f"{source_tag} {title}\n{url}"

            await self.send_telegram_with_retry(message, priority)

        # 标记所有原始行为已分析
        for row in rows:
            c.execute("UPDATE news SET analyzed = 1 WHERE id = ?", (row[0],))

        conn.commit()
        conn.close()

    # ── 健康检查 ──────────────────────────────────────────────

    async def health_check(self):
        failed_feeds = []

        session = await self._get_session()
        for feed in self.config["rss_feeds"]:
            try:
                async with session.get(feed["url"], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        failed_feeds.append(feed["name"])
            except Exception as e:
                failed_feeds.append(feed["name"])
                logger.error(f"[健康检查] {feed['name']} 失败: {e}")

        if failed_feeds:
            alert = f"⚠️ <b>RSS源异常</b>\n\n以下源无法访问：\n" + "\n".join([f"• {name}" for name in failed_feeds])
            await self.send_telegram(alert, "urgent")
            logger.warning(f"[健康检查] {len(failed_feeds)} 个源失败")
        else:
            logger.info("[健康检查] 所有RSS源正常")

    # ── 价格监控 ──────────────────────────────────────────────

    async def check_price_alerts(self):
        alerts = []
        session = await self._get_session()

        try:
            # 黄金
            async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1d&range=5d") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                    prices = [p for p in prices if p is not None]
                    if len(prices) >= 2:
                        current, prev = prices[-1], prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        if abs(change_pct) > 2:
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🟡 黄金{direction} {abs(change_pct):.2f}%，当前 ${current:.2f}/盎司")

            # BTC
            async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?interval=1d&range=5d") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                    prices = [p for p in prices if p is not None]
                    if len(prices) >= 2:
                        current, prev = prices[-1], prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        if abs(change_pct) > 5:
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🔴 BTC{direction} {abs(change_pct):.2f}%，当前 ${current:,.0f}")

            # 原油 (WTI)
            async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/CL=F?interval=1d&range=5d") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    prices = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                    prices = [p for p in prices if p is not None]
                    if len(prices) >= 2:
                        current, prev = prices[-1], prices[-2]
                        change_pct = ((current - prev) / prev) * 100
                        if abs(change_pct) > 3:
                            direction = "暴涨" if change_pct > 0 else "暴跌"
                            alerts.append(f"🛢️ 原油{direction} {abs(change_pct):.2f}%，当前 ${current:.2f}/桶")

        except Exception as e:
            logger.error(f"[价格监控] 异常: {e}")

        for alert in alerts:
            await self.send_telegram(alert, "urgent")
            logger.info(f"[价格监控] {alert}")

    # ── 每日简报 ──────────────────────────────────────────────

    async def daily_digest(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            SELECT source, title, url, priority
            FROM news
            WHERE created_at > date('now')
            AND priority IN ('urgent', 'important')
            ORDER BY
                CASE priority
                    WHEN 'urgent' THEN 1
                    WHEN 'important' THEN 2
                END,
                created_at DESC
            LIMIT 10
        """)

        news_list = c.fetchall()
        conn.close()

        if not news_list:
            return

        urgent_count = sum(1 for x in news_list if x[3] == "urgent")
        important_count = sum(1 for x in news_list if x[3] == "important")

        digest = "📊 <b>今日市场简报</b>\n\n"

        if urgent_count > 0:
            digest += f"🔴 URGENT: {urgent_count} 条\n"
        if important_count > 0:
            digest += f"🟡 IMPORTANT: {important_count} 条\n"

        digest += "\n<b>重点新闻：</b>\n"
        for i, (source, title, url, priority) in enumerate(news_list[:5], 1):
            emoji = "🔴" if priority == "urgent" else "🟡"
            digest += f"{i}. {emoji} [{source}] {title[:50]}...\n"

        await self.send_telegram(digest, "important")
        logger.info("[简报] 每日简报已发送")

    # ── 主循环 ────────────────────────────────────────────────

    async def run(self):
        logger.info("市场情报监控启动")
        logger.info(f"RSS 源: {len(self.config['rss_feeds'])} 个")
        logger.info(f"轮询: {self.config['polling_interval']}s")

        loop_count = 0

        while self._running:
            try:
                # 1. 并发抓取所有 RSS 源
                tasks = [self.fetch_rss(feed) for feed in self.config["rss_feeds"]]
                results = await asyncio.gather(*tasks)

                all_items = []
                for items in results:
                    all_items.extend(items)

                # 2. AI 智能分级（批量，每 3 轮调用一次 AI 分级，其余用关键词）
                if loop_count % 3 == 0 and all_items:
                    all_items = await self.ai_classify_priority(all_items)

                # 3. 入库（含 TF-IDF 基础去重）
                new_count = self.save_news(all_items)

                # 4. AI 语义去重 + 分析 + 推送
                if new_count > 0:
                    await self.analyze_and_notify()

                loop_count += 1

                # 每 12 轮（~1h）健康检查
                if loop_count % 12 == 0:
                    await self.health_check()

                # 每 6 轮（~30m）价格监控
                if loop_count % 6 == 0:
                    await self.check_price_alerts()

                # 每天早上 8 点发简报
                if datetime.now().hour == 8 and datetime.now().minute < 5:
                    await self.daily_digest()

                await asyncio.sleep(self.config["polling_interval"])

            except Exception as e:
                logger.error(f"主循环异常: {e}")
                await asyncio.sleep(60)

        await self._close_session()
        logger.info("监控已停止")

    def stop(self):
        self._running = False


if __name__ == "__main__":
    monitor = MarketIntelMonitor()

    def handle_signal(sig, frame):
        logger.info(f"收到信号 {sig}，正在停止...")
        monitor.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(monitor.run())
