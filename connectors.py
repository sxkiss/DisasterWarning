"""
WebSocket 数据源连接器。
负责与各上游数据源建立 WebSocket 连接、收发消息、自动重连。
支持 TEXT 和 BINARY 消息（Protobuf）。
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Coroutine, Optional

from loguru import logger

try:
    import aiohttp
except ImportError:
    aiohttp = None


class WSConnector:
    """通用 WebSocket 连接器，支持自动重连。"""

    def __init__(
        self,
        url: str,
        backup_url: str = "",
        name: str = "ws",
        heartbeat_interval: int = 120,
        reconnect_interval: int = 10,
        max_reconnect_retries: int = 3,
        connection_timeout: int = 15,
    ):
        self.url = url
        self.backup_url = backup_url
        self.name = name
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_retries = max_reconnect_retries
        self.connection_timeout = connection_timeout

        self._on_message: Optional[Callable[[Any, bytes | None], Coroutine]] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._retry_count = 0

    def set_handler(self, handler: Callable[[Any, bytes | None], Coroutine]) -> None:
        self._on_message = handler

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._retry_count = 0
        logger.info(f"[灾害预警] 启动连接器: {self.name} ({self.url})")
        self._task = asyncio.create_task(self._connect_loop())

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._close_ws()

    async def _connect_loop(self) -> None:
        """主连接循环，支持故障转移。"""
        urls = [self.url]
        if self.backup_url:
            urls.append(self.backup_url)

        while self._running:
            connected = False
            for url in urls:
                if not self._running:
                    break
                try:
                    await self._do_connect(url)
                    connected = True
                    self._retry_count = 0
                    break
                except Exception as e:
                    logger.warning(f"[灾害预警] {self.name} 连接 {url} 失败: {e}")
                    continue

            if not connected and self._running:
                self._retry_count += 1
                if self._retry_count > self.max_reconnect_retries and self.max_reconnect_retries > 0:
                    logger.error(f"[灾害预警] {self.name} 达到最大重连次数 ({self.max_reconnect_retries})，停止重连")
                    await asyncio.sleep(60)  # 长时间冷却
                else:
                    wait = min(self.reconnect_interval * (2 ** (self._retry_count - 1)), 120)
                    logger.info(f"[灾害预警] {self.name} {wait}s 后重连 (第 {self._retry_count} 次)")
                    await asyncio.sleep(wait)

    async def _do_connect(self, url: str) -> None:
        if aiohttp is None:
            logger.error("[灾害预警] aiohttp 未安装，无法连接 WebSocket")
            return

        timeout = aiohttp.ClientTimeout(total=self.connection_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)

        try:
            self._ws = await self._session.ws_connect(url, heartbeat=self.heartbeat_interval)
            logger.info(f"[灾害预警] {self.name} 已连接: {url}")
            await self._receive_loop()
        except Exception as e:
            logger.warning(f"[灾害预警] {self.name} 连接断开: {e}")
            raise
        finally:
            await self._close_ws()

    async def _receive_loop(self) -> None:
        """接收消息循环。同时处理 TEXT 和 BINARY 消息。"""
        assert self._ws is not None
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    if self._on_message:
                        await self._on_message(data, None)
                except json.JSONDecodeError:
                    logger.debug(f"[灾害预警] {self.name} 收到非 JSON 文本消息: {msg.data[:200]}")
            elif msg.type == aiohttp.WSMsgType.BINARY:
                # 处理二进制消息（如 Global Quake 的 Protobuf 数据）
                if self._on_message:
                    await self._on_message(None, msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                break
            elif msg.type in (aiohttp.WSMsgType.PING, aiohttp.WSMsgType.PONG):
                # 心跳帧，忽略
                pass

    async def _close_ws(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
