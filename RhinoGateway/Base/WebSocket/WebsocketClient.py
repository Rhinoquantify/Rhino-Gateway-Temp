import asyncio
import json
import time
import traceback
from typing import NoReturn, Dict, List, Union

import aiohttp
from RhinoObject.Rhino.RhinoEnum import RhinoDataType
from RhinoObject.Rhino.RhinoObject import SymbolInfos, WebsocketData, CallableMethods

from RhinoGateway.Base.BaseGateway.BaseGateway import BaseGateway


class WebsocketClient:

    def __init__(self, gateway: BaseGateway) -> NoReturn:
        self._ws = None
        self.rhino_websocket = None
        self.subscribe_data = None  # 订阅信息
        self.unsubscribe_data = None  # 取消订阅
        self.on_transfer = None
        self.on_transfers = None
        self.on_heart = None
        self.websocket_time = time.time()  # websocket pong 时间
        self.gateway = gateway

    def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        self.on_transfers = callable_methods.on_transfers
        self.on_transfer = self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer

    async def unsubscribe(self):
        if self._ws is None:
            session = aiohttp.ClientSession()
            try:
                self._ws = await session.ws_connect(self.rhino_websocket.url, proxy=self.rhino_websocket.proxy)
            except aiohttp.ClientConnectorError:
                self.gateway.logger.error(
                    f"connect to Websocket server aiohttp.ClientConnectorError! url: {self.rhino_websocket.url}")
                return
            except Exception as e:
                self.gateway.logger.error(f"connect to Websocket server error! url: {self.rhino_websocket.url}")
                self.gateway.logger.error(traceback.format_exc())
                return

        if self.unsubscribe_data is not None:
            self.gateway.logger.info(f"{self.gateway.exchange_sub} unsubscribe")
            await self.send(self.unsubscribe_data)

    @property
    def ws(self):
        return self._ws

    async def close(self):
        try:
            if self._ws is None:
                return
            await self._ws.close()
            self._ws = None
        except Exception as e:
            self.gateway.logger.error(traceback.format_exc())
        await asyncio.sleep(5)
        await self.unsubscribe()

    async def ping(self, message: Union[str, bytes] = b"") -> NoReturn:
        await self._ws.ping(message)

    async def pong(self, message: Union[str, bytes] = b"") -> NoReturn:
        self.gateway.logger.info(f"{self.gateway.exchange_sub} pong")
        await self._ws.pong(message)

    async def connect(self) -> NoReturn:
        self.gateway.logger.info(f"{self.rhino_websocket.url} 开始连接")
        proxy = self.rhino_websocket.proxy
        session = aiohttp.ClientSession()
        try:
            if self._ws is None:
                self._ws = await session.ws_connect(self.rhino_websocket.url, proxy=proxy)
        except aiohttp.ClientConnectorError:
            self.gateway.logger.error(
                f"connect to Websocket server aiohttp.ClientConnectorError! url: {self.rhino_websocket.url}")
            return
        except Exception as e:
            self.gateway.logger.error(f"connect to Websocket server error! url: {self.rhino_websocket.url}")
            self.gateway.logger.error(traceback.format_exc())
            return
        if self.rhino_websocket.on_connected:
            await self.rhino_websocket.on_connected()
        loop = asyncio.get_event_loop()
        coro_receive = loop.create_task(self.receive())
        await coro_receive

    async def on_connected(self):
        self.gateway.logger.info(f"{self.gateway.exchange_sub} 进入 on_connected")
        self.gateway.logger.info(f"{self.gateway.exchange_sub} {self.subscribe_data}")
        websocket_data = WebsocketData(
            key=RhinoDataType.WEBSOCKETSTART.value,
            data_type=RhinoDataType.WEBSOCKETSTART.value,
            exchange_sub=self.gateway.exchange_sub,
        )
        await self.on_transfer(websocket_data)
        await self.send(self.subscribe_data)

    async def reconnect(self) -> NoReturn:
        """Re-connect to Websocket server."""
        self.gateway.logger.info("reconnecting to Websocket server right now!")
        # 取消订阅
        await self.close()
        await self.connect()

    async def receive(self):
        self.gateway.logger.info("websocket 开始接收消息")
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if self.rhino_websocket.on_receive:
                    try:
                        if len(msg.data) == 0:
                            continue
                        data = json.loads(msg.data)
                    except:
                        data = msg.data
                    await self.rhino_websocket.on_receive(data)
            elif msg.type == aiohttp.WSMsgType.PING:
                self.gateway.logger.warn("receive event PING")
            elif msg.type == aiohttp.WSMsgType.PONG:
                self.gateway.logger.warn("receive event PONG")
            elif msg.type == aiohttp.WSMsgType.BINARY:
                pass
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                self.gateway.logger.warn("receive event CLOSED")
                await self.reconnect()
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.gateway.logger.error("receive event ERROR")
            else:
                self.gateway.logger.error("receive is unknow")

            # await asyncio.sleep(0)

    async def send(self, data: Union[Dict, List, str]) -> bool:
        if not self.ws:
            self.gateway.logger.info("Websocket connection not connected yet!")
            return False
        if isinstance(data, dict):
            await self.ws.send_json(data)
        elif isinstance(data, str):
            await self.ws.send_str(data)
        elif isinstance(data, list):
            for d in data:
                if isinstance(d, str):
                    await self.ws.send_str(d)
                elif isinstance(d, dict):
                    await self.ws.send_json(d)
                else:
                    self.gateway.logger.error("send message failed")
                    return False
        else:
            self.gateway.logger.error("send message failed")
            return False
        self.gateway.logger.debug("send message")
        return True
