import hashlib
import hmac
import time
import traceback
import urllib.parse
from typing import NoReturn, Union, Dict, List, Callable, Any

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import CexOrderType
from RhinoObject.Base.BaseEnum import Exchange, ExchangeSub, DataGetType, OrderDirection, KLineType, SymbolType, \
    TickerType
from RhinoObject.Rhino.RhinoEnum import MethodEnum, RhinoDataType
from RhinoObject.Rhino.RhinoObject import MixInfo, SymbolInfo, RhinoDepth, RhinoTrade, RhinoConfig, SymbolInfos, \
    WebsocketListen, RhinoLeverage, RhinoOrder, CallableMethods, RhinoAccount, RhinoPosition, RhinoFundingRate, \
    RhinoBalance, RhinoKline, RhinoTickers, RhinoTicker
from RhinoObject.RhinoRequest.RhinoRequest import RhinoRequest
from RhinoObject.RhinoRequest.RhinoWebsocket import RhinoWebsocket
from RhinoObject.RhinoRequest.RhunoRequestEnum import Method

from RhinoGateway.Base.BaseGateway.BaseGateway import BaseGateway
from RhinoGateway.Base.RestFul.RestClient import RestClient
from RhinoGateway.Base.WebSocket.WebsocketClient import WebsocketClient
from RhinoGateway.Util.Util import get_RhinoDepth_from_MixInfo

rest_api = "https://api.binance.com"
websocket_url = "wss://stream.binance.com:9443/stream?streams="
websocket_pong = 700  # 多少秒发一次 pong 进行 websocket 保活


class BinanceSpotGateway(BaseGateway):
    def __init__(self, logger: RhinoLogger, rhino_collect_config: RhinoConfig, loop):
        super().__init__(logger, rhino_collect_config, loop)
        self.exchange = Exchange.BINANCE.value
        self.exchange_sub = ExchangeSub.BINANCESPOT.value

        self.rest = BinanceSpotRestGateway(self)
        self.websocket = BinanceSpotWebsocketGateway(self)

    async def get_time(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_time(symbol_info, callable_methods)

    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_exchange_infos(symbol_info, callable_methods)

    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_depths(symbol_info, callable_methods)

    async def get_trades(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_public_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_public_trades(rhino_trade, callable_methods)

    async def submit_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.submit_order(rhino_order, callable_methods)

    async def batch_submit_orders(self, rhino_orders: List[RhinoOrder],
                                  callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.batch_submit_orders(rhino_orders, callable_methods)

    async def cancel_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.cancel_order(rhino_order, callable_methods)

    async def close_position(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.close_position(rhino_order, callable_methods)

    async def cancel_batch_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        # 现货没有批量下单
        await self.rest.cancel_batch_orders(rhino_order, callable_methods)

    async def cancel_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        # 现货没有批量下单
        await self.rest.cancel_orders(rhino_order, callable_methods)

    async def close_positions(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.cancel_order(rhino_order, callable_methods)

    async def get_account(self, rhino_account: RhinoAccount, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_account(rhino_account, callable_methods)

    async def update_leverage(self, rhino_leverage: RhinoLeverage,
                              callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.update_leverage(rhino_leverage, callable_methods)

    async def withdraw(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def withdraw_status(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_position(self, rhino_position: RhinoPosition, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_position(rhino_position, callable_methods)

    async def get_positions(self, rhino_position: RhinoPosition, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_positions(rhino_position, callable_methods)

    async def get_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_order(rhino_order, callable_methods)

    async def get_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        """
        查询所有订单，包括：当前挂单、已成交、取消等
        """
        await self.rest.get_orders(rhino_order, callable_methods)

    async def get_open_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_funding_rate(self, rhino_funding_rate: RhinoFundingRate,
                               callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_funding_rates(self, rhino_funding_rates: List[RhinoFundingRate],
                                callable_methods: CallableMethods = None):
        pass

    async def get_klines(self, rhino_kline: RhinoKline,
                         callable_methods: CallableMethods = None):
        pass

    async def get_pending(self, symbol_info: SymbolInfo,
                          callable_methods: CallableMethods = None):
        pass

    def get_kline_type(self, line_type):
        k_line_type = ""
        if line_type == KLineType.K_1M.value:
            k_line_type = "1m"
        elif line_type == KLineType.K_5M.value:
            k_line_type = "5m"
        elif line_type == KLineType.K_15M.value:
            k_line_type = "15m"
        return k_line_type

    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        await super().subscribe(symbol_infos, callable_methods)


class BinanceSpotRestGateway(RestClient):

    def __init__(self, gateway: BinanceSpotGateway):
        super().__init__(gateway)

    async def sign(self, request: RhinoRequest) -> NoReturn:
        try:
            cex_key = request.extra.get("key")
            cex_secret = request.extra.get("secret")
            encode_str = ""
            if request.method == "GET" or request.method == "DELETE":
                if len(request.params) > 0:
                    for key, value in request.params.items():
                        encode_str += str(key) + "=" + str(value) + "&"
            elif request.method == "POST":
                if len(request.data) > 0:
                    encode_str = urllib.parse.urlencode(request.data) + "&"
                    # for key, value in request.data.items():
                    #     encode_str += str(key) + "=" + str(value) + "&"
            encode_str = bytes(encode_str[:-1], encoding="utf8")
            signature = hmac.new(cex_secret.encode('utf-8'), encode_str, hashlib.sha256).hexdigest()
            if request.method == "GET" or request.method == "DELETE":
                request.params["signature"] = signature
            elif request.method == "POST":
                request.data["signature"] = signature
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "X-MBX-APIKEY": cex_key
            }
            request.headers = headers
            return request
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} sign is error")
            self.gateway.logger.error(traceback.format_exc())

    async def get_time(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/time",
            params=None,
            data=None,
            headers=None,
            callback=self.on_get_time,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=symbol_info,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=symbol_info.proxy,
            is_sign=False
        )
        await self.fetch(rhino_request)

    async def on_get_time(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                          on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        await on_transfer(
            int(data.get("serverTime")), on_transfer_extra_data
        )

    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/exchangeInfo",
            params=None,
            data=None,
            headers=None,
            callback=self.on_get_exchange_infos,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=symbol_info,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=symbol_info.proxy,
            is_sign=False
        )
        await self.fetch(rhino_request)

    async def on_get_exchange_infos(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                                    on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        symbol_infos: List[SymbolInfo] = []
        for d in data.get("symbols"):
            symbol_info = SymbolInfo(
                real_pair=d.get("symbol").upper(),
                symbol=d.get("baseAsset").upper(),
                base=d.get("quoteAsset").upper(),
                base_price_precision=int(0),
                base_amount_precision=int(d.get("baseAssetPrecision")),
                symbol_price_precision=int(d.get("")),
                symbol_amount_precision=int(d.get("")),
            )
            symbol_infos.append(symbol_info)
        await on_transfer(
            symbol_infos, on_transfer_extra_data
        )

    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        real_pair = symbol_info.real_pair
        depth_limit = symbol_info.depth_limit
        params = {
            "symbol": real_pair,
            "limit": depth_limit,
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/depth",
            params=params,
            data=None,
            headers=None,
            callback=self.on_get_depths,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=symbol_info,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            proxy=symbol_info.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_depths(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                            on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        depth_limit = request.params.get("limit", 0)
        if depth_limit == 0:
            pass
        rhino_depth = get_RhinoDepth_from_MixInfo(extra)
        rhino_depth.gateway_send_time = data.get("lastUpdateId") * 1000
        rhino_depth.rhino_get_time = int(time.time() * 1000)

        bids = data.get("bids")
        bids_len = len(bids)
        asks = data.get("asks")
        asks_len = len(asks)
        for i in range(1, depth_limit + 1):
            if i <= asks_len:
                bid = bids[i - 1]
                setattr(rhino_depth, f"buy_price{i}", float(bid[0]))
                setattr(rhino_depth, f"buy_amount{i}", float(bid[1]))
            if i <= bids_len:
                ask = asks[i - 1]
                setattr(rhino_depth, f"sell_price{i}", float(ask[0]))
                setattr(rhino_depth, f"sell_amount{i}", float(ask[1]))
        # self.gateway.logger.debug(
        #     f"{self.gateway.exchange_sub} depth {extra.__str__()} 获取数据消耗时间 {int(time.time() * 1000) - extra.start_time}")
        await on_transfer(rhino_depth, on_transfer_extra_data)

    async def get_public_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_trade.sign_key,
            "secret": rhino_trade.sign_secret,
            "proxy": rhino_trade.proxy,
        }

        real_pair = rhino_trade.real_pair
        limit = rhino_trade.limit
        params = {
            "symbol": real_pair.upper(),
            "limit": limit,
            "fromId": 1,
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/historicalTrades",
            params=params,
            data=None,
            headers=None,
            callback=self.on_get_public_trades,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_trade.time_out,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=rhino_trade.proxy,
            is_sign=True
        )
        await self.fetch(rhino_request)

    async def on_get_public_trades(self, request: RhinoRequest, data, code: int, extra: Dict,
                                   on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        print(1)

    async def get_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "direction": rhino_order.direction,
            "proxy": rhino_order.proxy,
            "rhino_order": rhino_order,
            "real_pair": rhino_order.real_pair.upper(),
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "timestamp": int(time.time() * 1000),
        }

        if len(rhino_order.order_id) > 0:
            data["orderId"] = rhino_order.order_id

        if len(rhino_order.client_order_id) > 0:
            data["origClientOrderId"] = rhino_order.client_order_id

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/order",
            params=data,
            data=None,
            headers=None,
            callback=self.on_get_order,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_order.time_out,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=rhino_order.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_order(self, request: RhinoRequest, data, code: int, extra: Dict,
                           on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_order: RhinoOrder = extra.get("rhino_order")
        order = RhinoOrder(
            real_pair=data.get("symbol").upper(),
            order_id=data.get("orderId"),
            price=float(data.get("price")),
            amount=float(data.get("origQty")),
            execute_amount=float(data.get("executedQty")),
            state=data.get("status"),
            # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
            direction=extra.get("direction"),
            sign_key=extra.get("key"),
            sign_secret=extra.get("secret"),
            proxy=extra.get("proxy"),
            is_close=rhino_order.is_close,
            cex_exchange_sub=self.gateway.exchange_sub,
            order_type=data.get("type"),
            OrderForceType=data.get("timeInForce"),
            client_order_id=data.get("clientOrderId"),

            pre_price=rhino_order.pre_price,
            pre_order_id=rhino_order.pre_order_id,
            pre_client_order_id=rhino_order.pre_client_order_id,

            sell_price=rhino_order.sell_price,
            sell_order_id=rhino_order.sell_order_id,
            sell_client_order_id=rhino_order.sell_client_order_id,
            buy_price=rhino_order.buy_price,
            buy_order_id=rhino_order.buy_order_id,
            buy_client_order_id=rhino_order.buy_client_order_id,

        )
        if order.price == 0 and float(data.get("executedQty")) > 0:
            order.price = float(data.get("cummulativeQuoteQty")) / float(data.get("executedQty"))
        await on_transfer(order, on_transfer_extra_data)

    async def get_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "direction": rhino_order.direction,
            "proxy": rhino_order.proxy,
            "rhino_order": rhino_order,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "limit": 30,
            "timestamp": int(time.time() * 1000),
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/allOrders",
            params=data,
            data=None,
            headers=None,
            callback=self.on_get_orders,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_order.time_out,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=rhino_order.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_orders(self, request: RhinoRequest, data, code: int, extra: Dict,
                            on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_orders = []
        # 只返回挂单、成交、部分成交
        for d in data:
            # if d.get("status") in [CexOrderType.NEW.value, CexOrderType.PARTIALLY_FILLED.value,
            #                        CexOrderType.FILLED.value]:
            rhino_order = RhinoOrder(
                real_pair=d.get("symbol").upper(),
                order_id=d.get("orderId"),
                price=float(d.get("price")),
                amount=float(d.get("origQty")),
                execute_amount=float(d.get("executedQty")),
                state=d.get("status"),
                direction=d.get("side"),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
                cex_exchange_sub=self.gateway.exchange_sub,
                order_type=d.get("type"),
                OrderForceType=d.get("timeInForce"),
                rhino_get_time=int(time.time() * 1000),
                client_order_id=d.get("clientOrderId")
            )
            if rhino_order.price == 0 and float(d.get("executedQty")) > 0:
                rhino_order.price = float(d.get("cummulativeQuoteQty")) / float(d.get("executedQty"))
            rhino_orders.append(
                rhino_order
            )
        await on_transfer(rhino_orders, on_transfer_extra_data)

    async def submit_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "direction": rhino_order.direction,
            "proxy": rhino_order.proxy,
            "rhino_order": rhino_order,
            "real_pair": rhino_order.real_pair.upper(),
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "side": rhino_order.direction,
            "type": rhino_order.order_type,
            "quantity": str(float(rhino_order.amount)),
            "timestamp": int(time.time() * 1000),
        }

        if len(rhino_order.client_order_id) > 0:
            data["newClientOrderId"] = rhino_order.client_order_id

        if rhino_order.OrderForceType is not None:
            data["timeInForce"] = rhino_order.OrderForceType

        if float(rhino_order.price) > 0:
            data["price"] = str(rhino_order.price)

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api + "/api/v3/order",
            params=None,
            data=data,
            headers=None,
            callback=self.on_submit_order,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            on_timeout=None if callable_methods.on_timeout is None else callable_methods.on_timeout,
            timeout=rhino_order.time_out,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=rhino_order.proxy
        )
        await self.fetch(rhino_request)

    async def on_submit_order(self, request: RhinoRequest, data, code: int, extra: Dict,
                              on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:

        _rhino_order: RhinoOrder = extra.get("rhino_order")

        rhino_order = RhinoOrder(
            real_pair=data.get("symbol").upper(),
            order_id=data.get("orderId"),

            pre_price=_rhino_order.pre_price,
            pre_order_id=_rhino_order.pre_order_id,
            pre_client_order_id=_rhino_order.pre_client_order_id,
            price=float(data.get("price")),
            amount=float(data.get("origQty")),
            execute_amount=float(data.get("executedQty")),
            # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
            direction=extra.get("direction"),
            sign_key=extra.get("key"),
            sign_secret=extra.get("secret"),
            proxy=extra.get("proxy"),
            state=data.get("status"),
            order_type=data.get("type"),
            OrderForceType=data.get("timeInForce"),
            is_close=False,
            cex_exchange_sub=self.gateway.exchange_sub,
            rhino_get_time=int(time.time() * 1000),
            client_order_id=data.get("clientOrderId"),

            sell_price=_rhino_order.sell_price,
            sell_order_id=_rhino_order.sell_order_id,
            sell_client_order_id=_rhino_order.sell_client_order_id,
            buy_price=_rhino_order.buy_price,
            buy_order_id=_rhino_order.buy_order_id,
            buy_client_order_id=_rhino_order.buy_client_order_id,
        )
        if rhino_order.price == 0 and float(data.get("executedQty")) > 0:
            rhino_order.price = float(data.get("cummulativeQuoteQty")) / float(data.get("executedQty"))
        await on_transfer(rhino_order, on_transfer_extra_data)

    async def batch_submit_order(self, rhino_orders: list[RhinoOrder],
                                 callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def on_batch_submit_order(self, request: RhinoRequest, data, code: int, extra: Dict,
                                    on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        pass

    async def cancel_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "real_pair": rhino_order.real_pair.upper(),
            "rhino_order": rhino_order,
            "proxy": rhino_order.proxy,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "orderId": rhino_order.order_id,
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.DELETE.value,
            url=rest_api + "/api/v3/order",
            params=data,
            data=None,
            callback=self.on_cancel_order,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_order.time_out,
            proxy=rhino_order.proxy
        )

        await self.fetch(rhino_request)

    async def on_cancel_order(self, request: RhinoRequest, data, code: int, extra: dict,
                              on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_order: RhinoOrder = extra.get("rhino_order")
        order = RhinoOrder(
            sign_key=extra.get("key"),
            sign_secret=extra.get("secret"),
            proxy=extra.get("proxy"),
            order_id=rhino_order.order_id,
            cex_exchange_sub=self.gateway.exchange_sub,
            is_close=True,
            state=CexOrderType.CANCELED.value,
            real_pair=extra.get("real_pair"),
            rhino_get_time=int(time.time() * 1000),
            amount=rhino_order.amount,
            price=rhino_order.price,
            client_order_id=data.get("clientOrderId")
        )

        await on_transfer(order, on_transfer_extra_data)

    async def cancel_batch_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def cancel_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "real_pair": rhino_order.real_pair.upper(),
            "rhino_order": rhino_order,
            "proxy": rhino_order.proxy,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.DELETE.value,
            url=rest_api + "/api/v3/openOrders",
            params=data,
            data=None,
            callback=self.on_cancel_orders,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_order.time_out,
            proxy=rhino_order.proxy
        )

        await self.fetch(rhino_request)

    async def on_cancel_orders(self, request: RhinoRequest, data, code: int, extra: dict,
                               on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_orders = []
        for d in data:
            rhino_orders.append(RhinoOrder(
                real_pair=d.get("symbol"),
                order_id=d.get("orderId"),
                price=float(d.get("price")),
                amount=float(d.get("origQty")),
                execute_amount=float(d.get("executedQty")),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
                is_close=True,
                state=CexOrderType.CANCELED.value,
                client_order_id=d.get("clientOrderId")
            ))
        await on_transfer(rhino_orders, on_transfer_extra_data)

    async def get_account(self, rhino_account: RhinoAccount, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_account.sign_key,
            "secret": rhino_account.sign_secret,
        }

        data = {
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/account",
            params=data,
            data=None,
            callback=self.on_get_account,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_account.time_out,
            proxy=rhino_account.proxy
        )

        await self.fetch(rhino_request)

    async def on_get_account(self, request: RhinoRequest, data, code: int, extra: Dict,
                             on_transfer: Callable = None, on_transfer_extra_data: Any = None):
        rhino_account = RhinoAccount(
            cex_exchange_sub=self.gateway.exchange_sub
        )
        balances = []
        data_balances = data.get("balances", [])
        for d in data_balances:
            rhino_balance = RhinoBalance(
                symbol=d.get("asset"),
                balance=float(d.get("free")) + float(d.get("locked")),
                available=float(d.get("free")),
                frozen=float(d.get("locked")),
            )
            balances.append(rhino_balance)
        rhino_account.balance_list = balances
        await on_transfer(
            rhino_account, on_transfer_extra_data
        )

    async def on_error(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass

    async def on_fail(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass


class BinanceSpotWebsocketGateway(WebsocketClient):

    def __init__(self, gateway: BinanceSpotGateway):
        super().__init__(gateway)

    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        super().subscribe(symbol_infos, callable_methods)
        symbol_infos: List[SymbolInfo] = symbol_infos.symbols.get(self.gateway.exchange_sub)
        subscribe_list = []
        all_ticker = False
        for symbol_info in symbol_infos:
            symbol_methods: List[Union[MethodEnum]] = symbol_info.symbol_methods
            for symbol_method in symbol_methods:
                if symbol_method == MethodEnum.GETDEPTHS.value:
                    depth_limit = symbol_info.rhino_depth.depth_limit
                    interval = symbol_info.rhino_depth.interval
                    subscribe_list.append(f"{symbol_info.real_pair}@depth{depth_limit}@{interval}ms")
                elif symbol_method == MethodEnum.GETTRADES.value:
                    subscribe_list.append(f"{symbol_info.real_pair}@aggTrade")
                elif symbol_method == MethodEnum.GETKLINE.value:
                    k_line_type = self.gateway.get_kline_type(symbol_info.rhino_kline.k_line_type)
                    subscribe_list.append(f"{symbol_info.real_pair}@kline_{k_line_type}")
                elif symbol_method == MethodEnum.GETTICKER.value:
                    rhino_ticker_type = symbol_info.rhino_ticker.ticker_type
                    if rhino_ticker_type == TickerType.ALL.value:
                        if not all_ticker:
                            subscribe_list.append(f"!ticker@arr")
                            all_ticker = True
                    else:
                        subscribe_list.append(f"{symbol_info.real_pair}@ticker")

        self.subscribe_data = ('{"method": "SUBSCRIBE","params":' + str(subscribe_list) + ',"id": 1}').replace('\'',
                                                                                                               '"')
        self.unsubscribe_data = ('{"method": "UNSUBSCRIBE","params":' + str(subscribe_list) + ',"id": 1}').replace('\'',
                                                                                                                   '"')
        rhino_websocket = RhinoWebsocket(
            url=websocket_url,
            on_connected=self.on_connected,
            on_receive=self.on_received,
            proxy=symbol_info.proxy
        )
        self.rhino_websocket = rhino_websocket
        await self.connect(rhino_websocket)

    async def on_received(self, data):
        try:
            channel = data.get("stream", None)
            if channel is None:
                self.gateway.logger.error(f"{self.gateway.exchange_sub} websocket channel 为 None {data}")
                return
            if "depth" in channel:
                await self.on_depths(data)
            elif "aggTrade" in channel:
                await self.on_trades(data)
            elif "kline" in channel:
                await self.on_kline(data)
            elif "ticker" in channel:
                await self.on_tickers(data)

            # 进行 ping/pong 保活
            if time.time() - self.websocket_time > websocket_pong:
                await self.pong(b"pong")
                self.websocket_time = time.time()

        except Exception as e:
            self.gateway.logger.error(f"解析 websocket receive 有错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_tickers(self, data):
        try:
            data = data.get("data")
            symbol = None
            rhino_ticker_object = None
            if isinstance(data, list):
                symbol = "ALL"
                rhino_ticker_object = RhinoTickers()
                rhino_ticker_object.key = symbol + RhinoDataType.RHINOTICKER.value
                rhino_ticker_object.ticker_list = []
                rhino_ticker_object.ticker_type = TickerType.ALL.value
                for d in data:
                    rhino_ticker_object.ticker_list.append(self.on_ticker(d))
            else:
                rhino_ticker_object = self.on_ticker(data)
                symbol = data.get("s").upper()

            self.gateway.logger.debug(
                f"websocket 推送数据成功 {self.gateway.exchange_sub} ticker")
            # await self.on_transfer(rhino_ticker_object)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOTICKER.value)(
                rhino_ticker_object)
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
                key=symbol + MethodEnum.GETKLINE.value
            )
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket ticker 数据错误")
            self.gateway.logger.error(traceback.format_exc())

    def on_ticker(self, data) -> RhinoTicker:
        symbol = data.get("s").upper()
        rhino_ticker = RhinoTicker(
            real_pair=symbol,
            cex_exchange_sub=self.gateway.exchange_sub,
            data_get_type=DataGetType.WEBSOCKET.value,
            data_type=RhinoDataType.RHINOKLINE.value,
            cex_type=SymbolType.USWAP.value,
            rhino_get_time=int(time.time() * 1000)
        )
        rhino_ticker.open_price = float(data.get("o"))
        rhino_ticker.close_price = float(data.get("c"))
        rhino_ticker.trade_price = float(data.get("c"))
        rhino_ticker.high_price = float(data.get("h"))
        rhino_ticker.low_price = float(data.get("l"))
        return rhino_ticker

    async def on_kline(self, data):
        try:
            data = data.get("data")
            symbol = data.get("s").upper()
            kline_info = data.get("k")
            rhino_kline = RhinoKline(
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
                data_type=RhinoDataType.RHINOKLINE.value,
                cex_type=SymbolType.SPOT.value,
                rhino_get_time=int(time.time() * 1000)
            )
            rhino_kline.open_price = float(kline_info.get("o"))
            rhino_kline.close_price = float(kline_info.get("c"))
            rhino_kline.high_price = float(kline_info.get("h"))
            rhino_kline.low_price = float(kline_info.get("l"))
            rhino_kline.is_end = kline_info.get("x")
            # await self.on_transfer(rhino_kline)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOKLINE.value)(rhino_kline)
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
                key=symbol + MethodEnum.GETKLINE.value
            )
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket kline 数据错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_trades(self, data):
        try:
            symbol = data.get("stream").split("@")[0].upper()
            rhino_trade = RhinoTrade(
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
                data_type=RhinoDataType.RHINOTRADE.value
            )
            rhino_trade.data_calcu_time = int(data.get("data").get("T"))
            rhino_trade.gateway_send_time = int(data.get("data").get("E"))
            rhino_trade.rhino_get_time = int(time.time() * 1000)
            rhino_trade.amount = float(data.get("data").get("q"))
            rhino_trade.price = float(data.get("data").get("p"))
            rhino_trade.direction = OrderDirection.SELL.value if data.get("data").get(
                "m") is True else OrderDirection.BUY.value
            self.gateway.logger.debug(
                f"websocket 推送数据成功 {self.gateway.exchange_sub} trade {symbol} amount {rhino_trade.amount} price {rhino_trade.price} "
                f" 推送和计算 time diff {rhino_trade.gateway_send_time - rhino_trade.data_calcu_time} 接收和推送 time diff {rhino_trade.rhino_get_time - rhino_trade.gateway_send_time}")
            # await self.on_transfer(rhino_trade)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOTRADE.value)(rhino_trade)
            self.gateway.logger.debug(f"传送完毕")
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
                key=symbol + MethodEnum.GETTRADES.value
            )
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket trade 数据错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_depths(self, data):
        try:
            symbol = data.get("stream").split("@")[0].upper()
            token = symbol.upper().replace("USDT", "").replace("BUSD", "")
            depth_limit = int(data.get("stream").split("@")[1].split("depth")[1])
            rhino_depth = RhinoDepth(
                symbol=token,
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
            )
            bids = data.get("data", {}).get("bids")
            bids_len = len(bids)
            asks = data.get("data", {}).get("asks")
            asks_len = len(asks)
            for i in range(1, depth_limit + 1):
                if i <= asks_len:
                    bid = bids[i - 1]
                    setattr(rhino_depth, f"buy_price{i}", float(bid[0]))
                    setattr(rhino_depth, f"buy_amount{i}", float(bid[1]))
                if i <= bids_len:
                    ask = asks[i - 1]
                    setattr(rhino_depth, f"sell_price{i}", float(ask[0]))
                    setattr(rhino_depth, f"sell_amount{i}", float(ask[1]))
            self.gateway.logger.debug(
                f"websocket 推送数据成功 {self.gateway.exchange_sub} depth {symbol}")
            # await self.on_transfer(rhino_depth)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINODEPTH.value)(rhino_depth)
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
                key=symbol + MethodEnum.GETDEPTHS.value
            )
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket depth 数据错误")
            self.gateway.logger.error(traceback.format_exc())
