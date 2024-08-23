import hashlib
import hmac
import json
import time
import traceback
import urllib.parse
from typing import NoReturn, Union, Dict, List, Callable, Any

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import Exchange, ExchangeSub, DataGetType, PositionDirection, SymbolType, KLineType, \
    TickerType, DepthType, OrderDirection
from RhinoObject.Rhino.RhinoEnum import MethodEnum, RhinoDataType
from RhinoObject.Rhino.RhinoObject import MixInfo, SymbolInfo, RhinoDepth, RhinoTrade, RhinoConfig, SymbolInfos, \
    WebsocketListen, RhinoOrder, RhinoLeverage, CallableMethods, RhinoAccount, RhinoBalance, RhinoPosition, \
    RhinoFundingRate, RhinoKline, RhinoTicker, RhinoTickers, RhinoTrades
from RhinoObject.RhinoRequest.RhinoRequest import RhinoRequest
from RhinoObject.RhinoRequest.RhinoWebsocket import RhinoWebsocket
from RhinoObject.RhinoRequest.RhunoRequestEnum import Method

from RhinoGateway.Base.BaseGateway.BaseGateway import BaseGateway
from RhinoGateway.Base.RestFul.RestClient import RestClient
from RhinoGateway.Base.WebSocket.WebsocketClient import WebsocketClient
from RhinoGateway.Util.Util import get_RhinoDepth_from_MixInfo

rest_api = "https://fapi.binance.com"
websocket_url = "wss://fstream.binance.com/ws"
websocket_pong = 700  # 多少秒发一次 pong 进行 websocket 保活


class BinanceUSwapGateway(BaseGateway):
    def __init__(self, logger: RhinoLogger, rhino_collect_config: RhinoConfig, loop):
        super().__init__(logger, rhino_collect_config, loop)
        self.exchange = Exchange.BINANCE.value
        self.exchange_sub = ExchangeSub.BINANCEUSWAP.value

        self.rest = BinanceUSwapRestGateway(self)
        self.websocket = BinanceUSwapWebsocketGateway(self)

    async def get_time(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_exchange_infos(symbol_info, callable_methods)

    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_depths(symbol_info, callable_methods)

    async def get_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_trades(rhino_trade, callable_methods)

    async def get_public_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        pass

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
        await self.rest.cancel_orders(rhino_order, callable_methods)

    async def cancel_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
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
        await self.rest.get_orders(rhino_order, callable_methods)

    async def get_open_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_open_orders(rhino_order, callable_methods)

    async def get_funding_rate(self, rhino_funding_rate: RhinoFundingRate,
                               callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_funding_rates(self, rhino_funding_rates: List[RhinoFundingRate],
                                callable_methods: CallableMethods = None):
        await self.rest.get_funding_rates(rhino_funding_rates, callable_methods)

    async def get_klines(self, rhino_kline: RhinoKline,
                         callable_methods: CallableMethods = None):
        await self.rest.get_klines(rhino_kline, callable_methods)

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


class BinanceUSwapRestGateway(RestClient):

    def __init__(self, gateway: BinanceUSwapGateway):
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

    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/exchangeInfo",
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
            if d.get("status") != "TRADING":
                continue
            symbol_info = SymbolInfo(
                real_pair=d.get("symbol").upper(),
                symbol=d.get("baseAsset").upper(),
                base=d.get("quoteAsset").upper(),
                base_price_precision=int(0),
                base_amount_precision=int(0),
                symbol_price_precision=int(d.get("pricePrecision")),
                symbol_amount_precision=int(d.get("quantityPrecision")),
            )
            symbol_infos.append(symbol_info)
        await on_transfer(
            symbol_infos, on_transfer_extra_data
        )

    async def get_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:

        params = {
            "symbol": rhino_trade.real_pair,
            "limit": rhino_trade.limit,
        }

        if rhino_trade.trade_start_time > 0:
            params["startTime"] = rhino_trade.trade_start_time

        if rhino_trade.trade_end_time > 0:
            params["endTime"] = rhino_trade.trade_end_time

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/aggTrades",
            params=params,
            data=None,
            headers=None,
            callback=self.on_get_trades,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_trade.time_out,
            extra=rhino_trade,
            on_transfer_extra_data=callable_methods.extra_data,
            proxy=rhino_trade.proxy,
            is_sign=False
        )
        await self.fetch(rhino_request)

    async def on_get_trades(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                            on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_trades = RhinoTrades(
            trade_list=[]
        )
        for d in data:
            rhino_trade = RhinoTrade(
                real_pair=extra.real_pair.upper(),
                price=float(d.get("p")),
                amount=float(d.get("q")),
                data_calcu_time=int(d.get("T")),
                direction=OrderDirection.SELL.value if d.get("m") else OrderDirection.BUY.value
            )
            rhino_trades.trade_list.append(rhino_trade)
        await on_transfer(
            rhino_trades, on_transfer_extra_data
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
            url=rest_api + "/fapi/v1/depth",
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
        rhino_depth.data_calcu_time = data.get("T")
        rhino_depth.gateway_send_time = data.get("E")
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
        self.gateway.logger.debug(
            f"{self.gateway.exchange_sub} depth {extra.__str__()} 获取数据消耗时间 {int(time.time() * 1000) - extra.start_time}")
        await on_transfer(rhino_depth, on_transfer_extra_data)

    async def get_klines(self, rhino_kline: RhinoKline,
                         callable_methods: CallableMethods = None):
        k_line_type = self.gateway.get_kline_type(rhino_kline.k_line_type)
        params = {
            "symbol": rhino_kline.real_pair.upper(),
            "interval": k_line_type,
            "limit": rhino_kline.line_limit,
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/klines",
            params=params,
            data=None,
            headers=None,
            callback=self.on_get_klines,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_kline.time_out,
            extra=rhino_kline,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            proxy=rhino_kline.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_klines(self, request: RhinoRequest, data, code: int, extra: RhinoKline,
                            on_transfer: Callable = None, on_transfer_extra_data: Any = None):
        rhino_klines = []
        for d in data:
            rhino_klines.append(RhinoKline(
                real_pair=extra.real_pair,
                open_time=d[0],
                open_price=d[1],
                high_price=d[2],
                low_price=d[3],
                close_price=d[4],
                trade_amount=d[5],
                close_time=d[6],
                trade_count=d[7],
                rhino_get_time=int(time.time() * 1000)
            ))
        await on_transfer(
            rhino_klines, on_transfer_extra_data
        )

    async def get_funding_rates(self, rhino_funding_rates: List[RhinoFundingRate],
                                callable_methods: CallableMethods = None):

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/premiumIndex",
            params=None,
            data=None,
            headers=None,
            callback=self.on_get_funding_rates,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_funding_rates[0].time_out,
            extra=rhino_funding_rates,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            proxy=rhino_funding_rates[0].proxy
        )
        await self.fetch(rhino_request)

    async def on_get_funding_rates(self, request: RhinoRequest, data, code: int, extra: Dict,
                                   on_transfer: Callable = None, on_transfer_extra_data: Any = None):
        print(1)

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
            url=rest_api + "/fapi/v2/balance",
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
        for d in data:
            rhino_balance = RhinoBalance(
                symbol=d.get("asset"),
                balance=float(d.get("balance")),
                available=float(d.get("availableBalance")),
                frozen=float(d.get("balance")) - float(d.get("availableBalance")),
            )
            balances.append(rhino_balance)
        rhino_account.balance_list = balances
        await on_transfer(
            rhino_account, on_transfer_extra_data
        )

    async def submit_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "direction": rhino_order.direction,
            "proxy": rhino_order.proxy,
            "rhino_order": rhino_order,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "side": rhino_order.direction,  # OrderDirection
            "type": rhino_order.order_type,
            "quantity": rhino_order.amount,
            "timestamp": int(time.time() * 1000),
        }

        if rhino_order.OrderForceType is not None:
            data["timeInForce"] = rhino_order.OrderForceType

        if float(rhino_order.stop_price) > 0:
            data["stopPrice"] = rhino_order.stop_price

        if float(rhino_order.price) > 0:
            data["price"] = str(rhino_order.price)

        if len(rhino_order.client_order_id) > 0:
            data["newClientOrderId"] = rhino_order.client_order_id

        if rhino_order.position_direction is not None:
            """
            双向持仓
            """
            data["positionSide"] = rhino_order.position_direction

        _rest_api = rest_api

        if len(rhino_order.rest_url) > 0:
            _rest_api = rhino_order.rest_url

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=_rest_api + "/fapi/v1/order",
            params=None,
            data=data,
            callback=self.on_submit_order,
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

    async def on_submit_order(self, request: RhinoRequest, data, code: int, extra: Dict,
                              on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        _rhino_order: RhinoOrder = extra.get("rhino_order")
        await on_transfer(
            RhinoOrder(
                pre_price=_rhino_order.pre_price,
                real_pair=data.get("symbol").upper(),
                order_id=data.get("orderId"),
                price=float(data.get("price")),
                amount=float(data.get("origQty")),
                # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
                direction=extra.get("direction"),
                is_close=False,
                cex_exchange_sub=self.gateway.exchange_sub,
                rhino_get_time=int(time.time() * 1000),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),

                client_order_id=data.get("clientOrderId"),
                pre_client_order_id=_rhino_order.pre_client_order_id,
                pre_order_id=_rhino_order.pre_order_id,

                sell_price=_rhino_order.sell_price,
                sell_order_id=_rhino_order.sell_order_id,
                sell_client_order_id=_rhino_order.sell_client_order_id,
                buy_price=_rhino_order.buy_price,
                buy_order_id=_rhino_order.buy_order_id,
                buy_client_order_id=_rhino_order.buy_client_order_id,

            ), on_transfer_extra_data
        )

    async def batch_submit_orders(self, rhino_orders: List[RhinoOrder],
                                  callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_orders[0].sign_key,
            "secret": rhino_orders[0].sign_secret,
            "direction": [rhino_order.direction for rhino_order in rhino_orders],
            "proxy": rhino_orders[0].proxy,
        }

        orders = "["

        for rhino_order in rhino_orders:
            data = {
                "symbol": rhino_order.real_pair.upper(),
                "side": rhino_order.direction,
                "type": rhino_order.order_type,
                "quantity": str(float(rhino_order.amount)),
            }

            if rhino_order.OrderForceType is not None:
                data["timeInForce"] = rhino_order.OrderForceType

            if float(rhino_order.stop_price) > 0:
                data["stopPrice"] = str(rhino_order.stop_price)

            if float(rhino_order.price) > 0:
                data["price"] = str(rhino_order.price)

            if len(rhino_order.client_order_id) > 0:
                data["newClientOrderId"] = rhino_order.client_order_id

            if rhino_order.position_direction is not None:
                """
                双向持仓
                """
                data["positionSide"] = rhino_order.position_direction

            if len(rhino_order.client_order_id) > 0:
                data["newClientOrderId"] = rhino_order.client_order_id

            orders += json.dumps(data)
            orders += ","

        orders = orders[:-1] + "]"

        data = {
            "batchOrders": orders,
            "timestamp": int(time.time() * 1000),
        }

        self.gateway.logger.info(data)

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api + "/fapi/v1/batchOrders",
            params=None,
            data=data,
            callback=self.on_batch_submit_orders,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_orders[0].time_out,
            proxy=rhino_orders[0].proxy,
        )

        await self.fetch(rhino_request)

    async def on_batch_submit_orders(self, request: RhinoRequest, data, code: int, extra: Dict,
                                     on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_orders = []
        directions = extra.get("direction")
        for d in data:
            try:
                index = 0
                if "code" in d.keys() and d.get("code") != 200:
                    continue
                rhino_orders.append(RhinoOrder(
                    real_pair=d.get("symbol").upper(),
                    order_id=d.get("orderId"),
                    price=float(d.get("price")),
                    amount=float(d.get("origQty")),
                    # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
                    direction=directions[index],
                    is_close=False,
                    cex_exchange_sub=self.gateway.exchange_sub,
                    rhino_get_time=int(time.time() * 1000),
                    sign_key=extra.get("key"),
                    sign_secret=extra.get("secret"),
                    proxy=extra.get("proxy"),
                    client_order_id=d.get("clientOrderId")
                ))
                index += 1
            except Exception as e:
                self.gateway.logger.error("解析批量订单错误")
                self.gateway.logger.error(traceback.format_exc())
        await on_transfer(
            rhino_orders, on_transfer_extra_data
        )

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
            "timestamp": int(time.time() * 1000)
        }

        if len(rhino_order.order_id) > 0:
            data["orderId"] = rhino_order.order_id

        if len(rhino_order.client_order_id) > 0:
            data["origClientOrderId"] = rhino_order.client_order_id

        rhino_request = RhinoRequest(
            method=Method.DELETE.value,
            url=rest_api + "/fapi/v1/order",
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

    async def on_cancel_order(self, request: RhinoRequest, data, code: int, extra: Dict,
                              on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_order: RhinoOrder = extra.get("rhino_order")
        await on_transfer(
            RhinoOrder(
                order_id=rhino_order.order_id,
                cex_exchange_sub=self.gateway.exchange_sub,
                real_pair=extra.get("real_pair"),
                rhino_get_time=int(time.time() * 1000),
                is_close=True,
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
                client_order_id=data.get("clientOrderId"),
                state=data.get("status"),
                direction=data.get("positionSide"),
                execute_amount=data.get("executedQty"),
                amount=data.get("origQty"),
                price=data.get("price"),
            ), on_transfer_extra_data
        )

    async def cancel_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "real_pair": rhino_order.real_pair.upper(),
            "proxy": rhino_order.proxy,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "timestamp": int(time.time() * 1000),
        }

        rhino_request = RhinoRequest(
            method=Method.DELETE.value,
            url=rest_api + "/fapi/v1/allOpenOrders",
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

    async def on_cancel_orders(self, request: RhinoRequest, data, code: int, extra: Dict,
                               on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        await on_transfer(
            RhinoOrder(
                cex_exchange_sub=self.gateway.exchange_sub,
                real_pair=extra.get("real_pair"),
                rhino_get_time=int(time.time() * 1000),
                is_close=True,
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
            ), on_transfer_extra_data
        )

    async def close_position(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "proxy": rhino_order.proxy,
            "exchange_sub": self.gateway.exchange_sub,
            "real_pair": rhino_order.real_pair.upper(),
            "extra_data": callable_methods.extra_data,
            "rhino_order": rhino_order,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "type": rhino_order.order_type,
            "side": rhino_order.direction,
            "reduceOnly": "true",
            "quantity": rhino_order.amount,
            "timestamp": int(time.time() * 1000),
        }

        if len(rhino_order.client_order_id) > 0:
            data["newClientOrderId"] = rhino_order.client_order_id

        if rhino_order.position_direction is not None:
            """
            目前只支持双向持仓的全部平仓
            """
            data["positionSide"] = rhino_order.position_direction
            data.pop("reduceOnly")

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api + "/fapi/v1/order",
            params=None,
            data=data,
            callback=self.on_close_position,
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

    async def on_close_position(self, request: RhinoRequest, data, code: int, extra: Dict,
                                on_transfer: Callable = None, on_transfer_extra_data: Any = None):
        # 虽然是 close position 但是，本质上这个就是下单而已，所以，第一次返回的状态是 NEW
        _rhino_order = extra.get("rhino_order")
        await on_transfer(
            RhinoPosition(
                cex_exchange_sub=self.gateway.exchange_sub,
                real_pair=data.get("symbol"),
                rhino_get_time=int(time.time() * 1000),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
                client_order_id=str(data.get("clientOrderId")),
                state=data.get("status"),
                direction=PositionDirection.SHORT.value if data.get(
                    "side") == OrderDirection.SELL.value else PositionDirection.LONG.value,
                order_id=str(data.get("orderId")),
                pre_price=_rhino_order.pre_price,
                pre_order_id=_rhino_order.pre_order_id,
                pre_client_order_id=_rhino_order.pre_client_order_id,
            ), on_transfer_extra_data
        )

    async def update_leverage(self, rhino_leverage: RhinoLeverage,
                              callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_leverage.sign_key,
            "secret": rhino_leverage.sign_secret,
        }

        data = {
            "symbol": rhino_leverage.real_pair.upper(),
            "leverage": rhino_leverage.leverage,
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api + "/fapi/v1/leverage",
            params=None,
            data=data,
            callback=self.on_update_leverage,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_leverage.time_out,
            proxy=rhino_leverage.proxy
        )

        await self.fetch(rhino_request)

    async def on_update_leverage(self, request: RhinoRequest, data, code: int, extra: Dict,
                                 on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        await on_transfer(
            RhinoLeverage(
                cex_exchange_sub=self.gateway.exchange_sub,
                leverage=data.get("leverage"),
                real_pair=data.get("symbol"),
                rhino_get_time=int(time.time() * 1000)
            ), on_transfer_extra_data
        )

    async def get_position(self, rhino_position: RhinoPosition, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_position.sign_key,
            "secret": rhino_position.sign_secret,
            "proxy": rhino_position.proxy,
        }

        data = {
            "symbol": rhino_position.real_pair.upper(),
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v2/positionRisk",
            params=data,
            data=None,
            callback=self.on_get_position,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_position.time_out,
            proxy=rhino_position.proxy
        )

        await self.fetch(rhino_request)

    async def on_get_position(self, request: RhinoRequest, data, code: int, extra: Dict,
                              on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_positions = []
        for d in data:
            rhino_position = RhinoPosition(
                cex_exchange_sub=self.gateway.exchange_sub,
                real_pair=d.get("symbol").upper(),
                estimate_profit=float(d.get("unRealizedProfit")),
                direction=d.get("positionSide").upper(),
                Liquidation_Price=float(d.get("liquidationPrice")),
                price=float(d.get("entryPrice")),
                mark_price=float(d.get("markPrice")),
                amount=abs(float(d.get("positionAmt"))),
                rhino_get_time=int(time.time() * 1000),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
            )
            if float(d.get("positionAmt")) < 0:
                rhino_position.direction = PositionDirection.SHORT.value
            elif float(d.get("positionAmt")) > 0:
                rhino_position.direction = PositionDirection.LONG.value
            else:
                # 说明没有仓位
                rhino_position.direction = PositionDirection.BOTH.value
            rhino_positions.append(rhino_position)
        await on_transfer(rhino_positions, on_transfer_extra_data)

    async def get_positions(self, rhino_position: RhinoPosition, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_position.sign_key,
            "secret": rhino_position.sign_secret,
            "proxy": rhino_position.proxy,
        }

        data = {
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v2/positionRisk",
            params=data,
            data=None,
            callback=self.on_get_positions,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=rhino_position.time_out,
            proxy=rhino_position.proxy
        )

        await self.fetch(rhino_request)

    async def on_get_positions(self, request: RhinoRequest, data, code: int, extra: Dict,
                               on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_positions = []
        for d in data:
            # 不能做取消，否则，手动平单子，或者单子意外平了，会导致数据错误
            # if float(d.get("entryPrice")) == 0 or float(d.get("liquidationPrice")) == 0:
            #     continue
            rhino_position = RhinoPosition(
                cex_exchange_sub=self.gateway.exchange_sub,
                real_pair=d.get("symbol").upper(),
                estimate_profit=float(d.get("unRealizedProfit")),
                direction=d.get("positionSide").upper(),
                Liquidation_Price=float(d.get("liquidationPrice")),
                price=float(d.get("entryPrice")),
                amount=abs(float(d.get("positionAmt"))),
                mark_price=float(d.get("markPrice")),
                rhino_get_time=int(time.time() * 1000),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
            )
            if float(d.get("positionAmt")) < 0:
                rhino_position.direction = PositionDirection.SHORT.value
            elif float(d.get("positionAmt")) > 0:
                rhino_position.direction = PositionDirection.LONG.value
            else:
                # 证明没有仓位
                rhino_position.direction = PositionDirection.BOTH.value
            rhino_positions.append(rhino_position)
        await on_transfer(rhino_positions, on_transfer_extra_data)

    async def get_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "proxy": rhino_order.proxy,
            "rhino_order": rhino_order,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "timestamp": int(time.time() * 1000)
        }

        if len(rhino_order.order_id) > 0:
            data["orderId"] = rhino_order.order_id

        if len(rhino_order.client_order_id) > 0:
            data["origClientOrderId"] = rhino_order.client_order_id

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/order",
            params=data,
            data=None,
            callback=self.on_get_order,
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

    async def on_get_order(self, request: RhinoRequest, data, code: int, extra: Dict,
                           on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        _rhino_order: RhinoOrder = extra.get("rhino_order")
        rhino_order = RhinoOrder(
            real_pair=data.get("symbol").upper(),
            order_id=data.get("orderId"),
            price=float(data.get("price")),
            amount=float(data.get("origQty")),
            execute_price=float(data.get("avgPrice")),
            execute_amount=float(data.get("executedQty")),
            # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
            direction=PositionDirection.SHORT.value if data.get(
                "side") == OrderDirection.SELL.value else PositionDirection.LONG.value,
            is_close=data.get("closePosition"),
            cex_exchange_sub=self.gateway.exchange_sub,
            rhino_get_time=int(time.time() * 1000),
            sign_key=extra.get("key"),
            sign_secret=extra.get("secret"),
            proxy=extra.get("proxy"),
            state=data.get("status"),
            client_order_id=data.get("clientOrderId"),

            pre_price=_rhino_order.pre_price,
            pre_order_id=_rhino_order.pre_order_id,
            pre_client_order_id=_rhino_order.pre_client_order_id,

            sell_price=_rhino_order.sell_price,
            sell_order_id=_rhino_order.sell_order_id,
            sell_client_order_id=_rhino_order.sell_client_order_id,
            buy_price=_rhino_order.buy_price,
            buy_order_id=_rhino_order.buy_order_id,
            buy_client_order_id=_rhino_order.buy_client_order_id,
        )

        if rhino_order.price == 0:
            rhino_order.price = float(data.get("avgPrice"))
        await on_transfer(rhino_order, on_transfer_extra_data)

    async def get_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "proxy": rhino_order.proxy,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "limit": 30,
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/allOrders",
            params=data,
            data=None,
            callback=self.on_get_orders,
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

    async def on_get_orders(self, request: RhinoRequest, data, code: int, extra: Dict,
                            on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_orders = []
        for d in data:
            rhino_order = RhinoOrder(
                real_pair=d.get("symbol").upper(),
                order_id=d.get("orderId"),
                price=float(d.get("price")),
                amount=float(d.get("origQty")),
                execute_price=float(d.get("avgPrice")),
                execute_amount=float(d.get("executedQty")),
                # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
                direction=PositionDirection.SHORT.value if d.get(
                    "side") == OrderDirection.SELL.value else PositionDirection.LONG.value,
                is_close=d.get("closePosition"),
                cex_exchange_sub=self.gateway.exchange_sub,
                rhino_get_time=int(time.time() * 1000),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
                state=d.get("status"),
                client_order_id=d.get("clientOrderId"),
            )
            # if rhino_order.price == 0 and float(d.get("executedQty")) > 0:
            #     rhino_order.price = float(d.get("cummulativeQuoteQty")) / float(d.get("executedQty"))
            rhino_orders.append(
                rhino_order
            )
        await on_transfer(rhino_orders, on_transfer_extra_data)

    async def get_open_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
            "proxy": rhino_order.proxy,
        }

        data = {
            "symbol": rhino_order.real_pair.upper(),
            "timestamp": int(time.time() * 1000)
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/fapi/v1/openOrders",
            params=data,
            data=None,
            callback=self.on_get_open_orders,
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

    async def on_get_open_orders(self, request: RhinoRequest, data, code: int, extra: Dict,
                                 on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_orders: List[RhinoOrder] = []
        for d in data:
            rhino_order = RhinoOrder(
                real_pair=d.get("symbol").upper(),
                order_id=d.get("orderId"),
                price=float(d.get("price")),
                stop_price=float(d.get("stopPrice")),
                amount=float(d.get("origQty")),
                execute_price=float(d.get("avgPrice")),
                execute_amount=float(d.get("executedQty")),
                # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
                direction=PositionDirection.SHORT.value if d.get(
                    "side") == OrderDirection.SELL.value else PositionDirection.LONG.value,
                is_close=d.get("closePosition"),
                cex_exchange_sub=self.gateway.exchange_sub,
                rhino_get_time=int(time.time() * 1000),
                sign_key=extra.get("key"),
                sign_secret=extra.get("secret"),
                proxy=extra.get("proxy"),
                state=d.get("status"),
                client_order_id=d.get("clientOrderId"),
                order_type=d.get("type"),
            )
            rhino_orders.append(rhino_order)
        await on_transfer(rhino_orders, on_transfer_extra_data)

    async def on_error(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass

    async def on_fail(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass


class BinanceUSwapWebsocketGateway(WebsocketClient):

    def __init__(self, gateway: BinanceUSwapGateway):
        super().__init__(gateway)
        self.depth_type = None  # 因为没办法从数据流中知道到底是单币种最佳订单还是全币种最佳订单

    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        super().subscribe(symbol_infos, callable_methods)
        symbol_infos: List[SymbolInfo] = symbol_infos.symbols.get(self.gateway.exchange_sub)
        subscribe_list = []
        all_ticker = False
        all_best_depth = False
        for symbol_info in symbol_infos:
            symbol_methods: List[Union[MethodEnum]] = symbol_info.symbol_methods
            for symbol_method in symbol_methods:
                if symbol_method == MethodEnum.GETDEPTHS.value:
                    depth_limit = symbol_info.rhino_depth.depth_limit
                    interval = symbol_info.rhino_depth.interval
                    depth_type = symbol_info.rhino_depth.depth_type
                    if depth_type == DepthType.SYMBOL.value:
                        subscribe_list.append(f"{symbol_info.real_pair}@depth{depth_limit}@{interval}ms")
                    elif depth_type == DepthType.SYMBOLBEST.value:
                        subscribe_list.append(f"{symbol_info.real_pair}@bookTicker")
                    elif depth_type == DepthType.BESTALL.value and not all_best_depth:
                        subscribe_list.append(f"!bookTicker")
                        all_best_depth = True
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
                            self.depth_type = "ALL"
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
            if isinstance(data, list):
                channel = data[0].get("e", None)
            else:
                channel = data.get("e", None)

            if channel is None:
                self.gateway.logger.error(f"{self.gateway.exchange_sub} websocket channel 为 None {data}")
                return
            if "depth" in channel:
                await self.on_depths(data)
            elif "aggTrade" in channel:
                await self.on_trades(data)
            elif "kline" in channel:
                await self.on_kline(data)
            elif "24hrTicker" in channel:
                await self.on_tickers(data)
            elif "bookTicker" in channel:
                await self.on_best_depths(data)

            # 进行 ping/pong 保活
            if time.time() - self.websocket_time > websocket_pong:
                await self.pong(b"pong")
                self.websocket_time = time.time()

        except Exception as e:
            self.gateway.logger.error(f"解析 websocket receive 有错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_best_depths(self, data):
        try:
            symbol = data.get("s").upper()
            rhino_depth = RhinoDepth(
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
                cex_type=SymbolType.USWAP.value,
                gateway_send_time=int(data.get("E")),
                rhino_get_time=int(time.time() * 1000),
            )
            rhino_depth.buy_price1 = float(data.get("b"))
            rhino_depth.buy_amount1 = float(data.get("B"))
            rhino_depth.sell_price1 = float(data.get("a"))
            rhino_depth.sell_amount1 = float(data.get("A"))
            self.gateway.logger.debug(
                f"websocket 推送数据成功 {self.gateway.exchange_sub} {symbol} best depth")
            # await self.on_transfer(rhino_depth)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINODEPTH.value)(rhino_depth)
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
            )
            if self.depth_type is None:
                websocket_listen.key = "BEST" + symbol + MethodEnum.GETDEPTHS.value
            else:
                websocket_listen.key = "ALL" + MethodEnum.GETDEPTHS.value
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket best depth 数据错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_tickers(self, data):
        try:
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
            gateway_send_time=int(data.get("E")),
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
            symbol = data.get("s").upper()
            kline_info = data.get("k")
            rhino_kline = RhinoKline(
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
                data_type=RhinoDataType.RHINOKLINE.value,
                cex_type=SymbolType.USWAP.value,
                gateway_send_time=int(data.get("E")),
                rhino_get_time=int(time.time() * 1000),
            )
            rhino_kline.open_price = float(kline_info.get("o"))
            rhino_kline.close_price = float(kline_info.get("c"))
            rhino_kline.high_price = float(kline_info.get("h"))
            rhino_kline.low_price = float(kline_info.get("l"))
            rhino_kline.is_end = kline_info.get("x")
            self.gateway.logger.debug(
                f"websocket 推送数据成功 {self.gateway.exchange_sub} kline {rhino_kline.real_pair} {rhino_kline.k_line_type} {rhino_kline.is_end} {rhino_kline.high_price}  {rhino_kline.low_price}")
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
            symbol = data.get("s").upper()
            rhino_trade = RhinoTrade(
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
                data_type=RhinoDataType.RHINOTRADE.value,
                cex_type=SymbolType.USWAP.value,
                # gateway_send_time=int(data.get("E")),
                rhino_get_time=int(time.time() * 1000),
                gateway_send_time=int(data.get("T"))
            )
            rhino_trade.amount = float(data.get("q"))
            rhino_trade.price = float(data.get("p"))
            rhino_trade.direction = PositionDirection.SHORT.value if data.get(
                "m") is True else PositionDirection.LONG.value
            self.gateway.logger.debug(
                f"websocket 推送数据成功 {self.gateway.exchange_sub} trade {rhino_trade.real_pair} {rhino_trade.amount} {rhino_trade.price}  {rhino_trade.direction}")
            # await self.on_transfer(rhino_trade)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOTRADE.value)(rhino_trade)
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
            symbol = data.get("s").upper()
            depth_limit = len(data.get("b"))
            rhino_depth = RhinoDepth(
                real_pair=symbol,
                cex_exchange_sub=self.gateway.exchange_sub,
                data_get_type=DataGetType.WEBSOCKET.value,
                cex_type=SymbolType.USWAP.value,
                gateway_send_time=int(data.get("E")),
                rhino_get_time=int(time.time() * 1000),
            )
            bids = data.get("b", {})
            bids_len = len(bids)
            asks = data.get("a", {})
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
                f"websocket 推送数据成功 {self.gateway.exchange_sub} depth {rhino_depth.real_pair}")
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
