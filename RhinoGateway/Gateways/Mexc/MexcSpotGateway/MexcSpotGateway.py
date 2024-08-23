import hashlib
import hmac
import time
import traceback
from typing import NoReturn, Union, Dict, List, Any, Callable

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import Exchange, ExchangeSub, CexOrderForceType, Chain, WithdrawStatus, OrderDirection
from RhinoObject.Rhino.RhinoEnum import MethodEnum, RhinoDataType
from RhinoObject.Rhino.RhinoObject import MixInfo, SymbolInfo, RhinoConfig, SymbolInfos, RhinoOrder, \
    RhinoLeverage, CallableMethods, RhinoAccount, RhinoFundingRate, RhinoKline, RhinoTrade, RhinoBalance, \
    WebsocketListen, RhinoDepth, RhinoWithdraw, RhinoIncreaseDepth
from RhinoObject.RhinoRequest.RhinoRequest import RhinoRequest
from RhinoObject.RhinoRequest.RhinoWebsocket import RhinoWebsocket
from RhinoObject.RhinoRequest.RhunoRequestEnum import Method

from RhinoGateway.Base.BaseGateway.BaseGateway import BaseGateway
from RhinoGateway.Base.RestFul.RestClient import RestClient
from RhinoGateway.Base.WebSocket.WebsocketClient import WebsocketClient
from RhinoGateway.Util.Util import get_RhinoDepth_from_MixInfo

rest_api = "https://api.mexc.com"
websocket_url = "wss://wbs.mexc.com/ws"
websocket_pong = 700  # 多少秒发一次 pong 进行 websocket 保活


class MexcSpotGateway(BaseGateway):
    def __init__(self, logger: RhinoLogger, rhino_collect_config: RhinoConfig, loop=None):
        super().__init__(logger, rhino_collect_config, loop)
        self.exchange = Exchange.MEXC.value
        self.exchange_sub = ExchangeSub.MEXCSPOT.value

        self.rest = MexcSpotRestGateway(self)
        self.websocket = MexcSpotWebsocketGateway(self)

    async def get_time(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_exchange_infos(symbol_info, callable_methods)

    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_coin_info(symbol_info, callable_methods)

    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_depths(symbol_info, callable_methods)

    async def get_trades(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_public_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def submit_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.submit_order(rhino_order, callable_methods)

    async def batch_submit_orders(self, rhino_orders: List[RhinoOrder],
                                  callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def cancel_order(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_account(self, rhino_account: RhinoAccount, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_account(rhino_account, callable_methods)

    async def withdraw(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.withdraw(symbol_info, callable_methods)

    async def withdraw_status(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.withdraw_status(symbol_info, callable_methods)

    async def update_leverage(self, rhino_leverage: RhinoLeverage,
                              callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_position(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_positions(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_orders(rhino_order, callable_methods)

    async def get_open_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def close_position(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def cancel_batch_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        # 现货没有批量下单
        await self.rest.cancel_orders(rhino_order, callable_methods)

    async def cancel_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def close_positions(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
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

    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        await super().subscribe(symbol_infos, callable_methods)


class MexcSpotRestGateway(RestClient):

    def __init__(self, gateway: MexcSpotGateway):
        super().__init__(gateway)

    async def sign(self, request: RhinoRequest):
        try:
            request_time = str(int(time.time() * 1000))
            api_key = request.extra.get("key")
            secret = request.extra.get("secret")
            encode_str = ""
            method = request.method
            if method == "GET":
                if request.params is not None and len(request.params) > 0:
                    for key, value in request.params.items():
                        encode_str += "&" + key + "=" + value
            elif method == "POST":
                if request.data is not None and len(request.data) > 0:
                    for key, value in request.data.items():
                        value = str(value).replace("(", "%28").replace(")", "%29").replace(" ", "%20")
                        encode_str += "&" + key + "=" + str(value)
            encode_str += "&recvWindow=5000&timestamp=" + request_time
            encode_byte = bytes(encode_str[1:], encoding="utf8")
            signature = hmac.new(secret.encode('utf-8'), encode_byte, hashlib.sha256).hexdigest()
            encode_str += "&signature=" + signature

            encode_str = encode_str.replace("%28", "(").replace("%29", ")").replace("%20", " ")

            request.headers = {
                "X-MEXC-APIKEY": api_key,
                "Content-Type": "application/json",
            }
            request.data = encode_str[1:]

            if method == "GET":
                request.url = request.url + "?" + request.data
            return request
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} sign is error")
            self.gateway.logger.error(traceback.format_exc())

    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        depth_limit = symbol_info.depth_limit
        real_pair = symbol_info.real_pair

        if depth_limit == 1:
            if len(real_pair) > 0:
                await self.get_best_depths(symbol_info, callable_methods)
            else:
                await self.get_all_best_depths(symbol_info, callable_methods)
        else:
            await self.get_more_depths(symbol_info, callable_methods)

    async def get_more_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
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
            callback=self.on_get_more_depths,
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

    async def on_get_more_depths(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                                 on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        depth_limit = request.params.get("limit", 0)
        if depth_limit == 0:
            pass
        rhino_depth = get_RhinoDepth_from_MixInfo(extra)
        rhino_depth.cex_exchange = Exchange.MEXC.value
        rhino_depth.cex_exchange_sub = ExchangeSub.MEXCSPOT.value
        rhino_depth.gateway_send_time = 0
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
        # await self.gateway.set_data(rhino_depth)
        await on_transfer(rhino_depth, on_transfer_extra_data)

    async def get_best_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        real_pair = symbol_info.real_pair
        params = {
            "symbol": real_pair,
        }
        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/ticker/bookTicker",
            params=params,
            data=None,
            headers=None,
            callback=self.on_get_best_depths,
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

    async def on_get_best_depths(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                                 on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        depth_limit = request.params.get("limit", 0)
        if depth_limit == 0:
            pass
        rhino_depth = get_RhinoDepth_from_MixInfo(extra)
        rhino_depth.cex_exchange = Exchange.MEXC.value
        rhino_depth.cex_exchange_sub = ExchangeSub.MEXCSPOT.value
        rhino_depth.gateway_send_time = 0
        rhino_depth.rhino_get_time = int(time.time() * 1000)

        rhino_depth.buy_price1 = float(data.get("bidPrice"))
        rhino_depth.buy_amount1 = float(data.get("bidQty"))
        rhino_depth.sell_price1 = float(data.get("askPrice"))
        rhino_depth.sell_amount1 = float(data.get("askQty"))
        # self.gateway.logger.debug(
        #     f"{self.gateway.exchange_sub} depth {extra.__str__()} 获取数据消耗时间 {int(time.time() * 1000) - extra.start_time}")
        # await self.gateway.set_data(rhino_depth)
        await on_transfer(rhino_depth, on_transfer_extra_data)

    async def get_all_best_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/ticker/bookTicker",
            params=None,
            data=None,
            headers=None,
            callback=self.on_get_all_best_depths,
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

    async def on_get_all_best_depths(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                                     on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:

        rhino_depth_dict = {}

        for d in data:
            rhino_depth = get_RhinoDepth_from_MixInfo(extra)
            rhino_depth.cex_exchange = Exchange.MEXC.value
            rhino_depth.cex_exchange_sub = ExchangeSub.MEXCSPOT.value
            rhino_depth.gateway_send_time = 0
            rhino_depth.rhino_get_time = int(time.time() * 1000)

            buy_price = d.get("bidPrice")
            buy_amount = d.get("bidQty")
            sell_price = d.get("askPrice")
            sell_amount = d.get("askQty")

            if buy_price is None or buy_amount is None or sell_price is None or sell_amount is None:
                continue

            rhino_depth.buy_price1 = float(buy_price)
            rhino_depth.buy_amount1 = float(buy_amount)
            rhino_depth.sell_price1 = float(sell_price)
            rhino_depth.sell_amount1 = float(sell_amount)

            rhino_depth_dict[d.get("symbol")] = rhino_depth
        # self.gateway.logger.debug(
        #     f"{self.gateway.exchange_sub} depth {extra.__str__()} 获取数据消耗时间 {int(time.time() * 1000) - extra.start_time}")
        # await self.gateway.set_data(rhino_depth)
        await on_transfer(rhino_depth_dict, on_transfer_extra_data)

    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None):

        extra = {
            "key": symbol_info.sign_key,
            "secret": symbol_info.sign_secret,
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/capital/config/getall",
            params=None,
            data=None,
            headers=None,
            callback=self.on_get_coin_info,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=True,
            proxy=symbol_info.proxy
        )
        await self.fetch(rhino_request)

    async def get_account(self, rhino_account: RhinoAccount, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "key": rhino_account.sign_key,
            "secret": rhino_account.sign_secret,
        }

        # data = {
        #     "timestamp": int(time.time() * 1000)
        # }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/account",
            params=None,
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

    async def on_get_coin_info(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                               on_transfer: Callable = None, on_transfer_extra_data: Any = None):
        symbol_infos = []

        for d in data:
            symbol_info = SymbolInfo(
                symbol=d.get("coin")
            )
            chains = []
            contracts = []
            withdraw_enables = []
            deposit_enables = []
            withdraw_amounts = []
            for network in d.get("networkList"):
                chains.append(network.get("network"))
                contracts.append(network.get("contract"))
                deposit_enables.append(network.get("depositEnable"))
                withdraw_enables.append(network.get("withdrawEnable"))
                withdraw_amounts.append(network.get("withdrawFee"))
            symbol_info.chains = chains
            symbol_info.chains_contracts = contracts
            symbol_info.withdraw_enables = withdraw_enables
            symbol_info.deposit_enables = deposit_enables
            symbol_info.withdraw_amounts = withdraw_amounts
            symbol_infos.append(symbol_info)
        await on_transfer(
            symbol_infos, on_transfer_extra_data
        )

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
            "quantity": str(float(rhino_order.amount)),
            # "timestamp": int(time.time() * 1000),
        }

        if rhino_order.order_type is not None:
            data["type"] = rhino_order.order_type

        if rhino_order.OrderForceType is not None:

            if rhino_order.OrderForceType == CexOrderForceType.IOC.value:
                data["type"] = "IMMEDIATE_OR_CANCEL"

            if rhino_order.OrderForceType == CexOrderForceType.FOK.value:
                data["type"] = "FILL_OR_KILL"

        if len(rhino_order.client_order_id) > 0:
            # data["newClientOrderId"] = rhino_order.client_order_id
            data["newClientOrderId"] = "111123sd333"

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
            execute_amount=0,
            # 这里之所以不用返回的 direction 是因为，返回的 direction 是 both ，不能用
            direction=extra.get("direction"),
            sign_key=extra.get("key"),
            sign_secret=extra.get("secret"),
            proxy=extra.get("proxy"),
            state=data.get("status"),
            order_type=data.get("type"),
            OrderForceType=data.get("type"),
            is_close=False,
            cex_exchange_sub=self.gateway.exchange_sub,
            rhino_get_time=int(time.time() * 1000),
            client_order_id=None,

            sell_price=_rhino_order.sell_price,
            sell_order_id=_rhino_order.sell_order_id,
            sell_client_order_id=_rhino_order.sell_client_order_id,
            buy_price=_rhino_order.buy_price,
            buy_order_id=_rhino_order.buy_order_id,
            buy_client_order_id=_rhino_order.buy_client_order_id,
        )
        await on_transfer(rhino_order, on_transfer_extra_data)

    async def withdraw(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "symbol": symbol_info.symbol,
            "key": symbol_info.sign_key,
            "secret": symbol_info.sign_secret,
        }

        network = ""

        if symbol_info.chain == Chain.BSC.value:
            # network = "BEP20(BSC)"
            network = "BNB Smart Chain(BEP20)"

        data = {
            "coin": symbol_info.symbol,
            "network": network,
            "address": symbol_info.to_contract,
            "amount": symbol_info.from_amount,
        }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api + "/api/v3/capital/withdraw/apply",
            params=None,
            data=data,
            callback=self.on_withdraw,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=symbol_info.time_out,
            proxy=symbol_info.proxy
        )

        await self.fetch(rhino_request)

    async def on_withdraw(self, request: RhinoRequest, data, code: int, extra: Dict,
                          on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        order_id = data.get("id")

        rhino_withdraw = RhinoWithdraw(
            withdraw_id=order_id,
        )
        await on_transfer(rhino_withdraw, on_transfer_extra_data)

    async def withdraw_status(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "symbol": symbol_info.symbol,
            "key": symbol_info.sign_key,
            "secret": symbol_info.sign_secret,
        }

        data = {
            "limit": str(symbol_info.depth_limit),
        }

        if symbol_info.symbol is not None and len(symbol_info.symbol) > 0:
            data["coin"] = symbol_info.symbol

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/capital/withdraw/history",
            params=data,
            data=None,
            callback=self.on_withdraw_status,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            is_sign=True,
            extra=extra,
            on_transfer_extra_data=callable_methods.extra_data,
            timeout=symbol_info.time_out,
            proxy=symbol_info.proxy
        )

        await self.fetch(rhino_request)

    async def on_withdraw_status(self, request: RhinoRequest, data, code: int, extra: Dict,
                                 on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        rhino_withdraws = []

        for d in data:
            rhino_withdraw = RhinoWithdraw(
                withdraw_id=d.get("id"),
                tx=d.get("txId"),
                symbol=d.get("coin"),
                amount=float(d.get("amount")),
                fee=d.get("transactionFee"),
                apply_time=d.get("applyTime"),
            )
            status = d.get("status")
            withdraw_status = None
            if status == 1:
                withdraw_status = WithdrawStatus.SUBMIT.value
            elif status == 2:
                withdraw_status = WithdrawStatus.CHECK.value
            elif status == 3:
                withdraw_status = WithdrawStatus.WAIT.value
            elif status == 4:
                withdraw_status = WithdrawStatus.PROCESSING.value
            elif status == 5:
                withdraw_status = WithdrawStatus.WINDINGPROCESSING.value
            elif status == 6:
                withdraw_status = WithdrawStatus.CONFIRM.value
            elif status == 7:
                withdraw_status = WithdrawStatus.SUCCESS.value
            elif status == 8:
                withdraw_status = WithdrawStatus.FAIL.value
            elif status == 9:
                withdraw_status = WithdrawStatus.CANCEL.value

            rhino_withdraw.withdraw_state = withdraw_status

            rhino_withdraws.append(rhino_withdraw)

        await on_transfer(rhino_withdraws, on_transfer_extra_data)

    async def get_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        extra = {
            "symbol": rhino_order.real_pair,
            "key": rhino_order.sign_key,
            "secret": rhino_order.sign_secret,
        }

        data = {
            "symbol": "BPRIVAUSDT"
        }

        # if rhino_order.real_pair is not None and len(rhino_order.real_pair) > 0:
        #     data["symbol"] = rhino_order.real_pair

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/api/v3/allOrders",
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
        rhino_withdraws = []

    async def on_error(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass

    async def on_fail(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass


class MexcSpotWebsocketGateway(WebsocketClient):

    def __init__(self, gateway: MexcSpotGateway):
        super().__init__(gateway)

    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        super().subscribe(symbol_infos, callable_methods)
        symbol_infos: List[SymbolInfo] = symbol_infos.symbols.get(self.gateway.exchange_sub)
        subscribe_list = []
        for symbol_info in symbol_infos:
            if isinstance(symbol_info.symbol_method, str):
                symbol_method = symbol_info.symbol_method
                if symbol_method == MethodEnum.GETTICKER.value:
                    subscribe_list.append("spot@public.bookTicker.v3.api@" + symbol_info.real_pair)
                if symbol_method == MethodEnum.GETTRADES.value:
                    subscribe_list.append("spot@public.deals.v3.api@" + symbol_info.real_pair)
                if symbol_method == MethodEnum.GETSUBDEPTHS.value:
                    subscribe_list.append("spot@public.increase.depth.v3.api@" + symbol_info.real_pair)
            else:
                for symbol_method in symbol_info.symbol_method:
                    if symbol_method == MethodEnum.GETTICKER.value:
                        subscribe_list.append("spot@public.bookTicker.v3.api@" + symbol_info.real_pair)
                    if symbol_method == MethodEnum.GETTRADES.value:
                        subscribe_list.append("spot@public.deals.v3.api@" + symbol_info.real_pair)
                    if symbol_method == MethodEnum.GETSUBDEPTHS.value:
                        subscribe_list.append("spot@public.increase.depth.v3.api@" + symbol_info.real_pair)

        self.subscribe_data = ('{"method": "SUBSCRIPTION","params":' + str(subscribe_list) + '}').replace('\'',
                                                                                                          '"')
        self.unsubscribe_data = ('{"method": "UNSUBSCRIPTION","params":' + str(subscribe_list) + '}').replace('\'',
                                                                                                              '"')
        self.send_data = subscribe_list

        rhino_websocket = RhinoWebsocket(
            url=websocket_url,
            on_connected=self.on_connected,
            on_receive=self.on_received,
            proxy=symbol_info.proxy
        )
        self.rhino_websocket = rhino_websocket
        await self.connect()

    async def on_received(self, data) -> NoReturn:
        try:
            if not isinstance(data, dict):
                return
            # self.gateway.logger.info(f"{data}")
            channel = data.get("c", None)
            if channel is None:
                self.gateway.logger.error(f"{self.gateway.exchange_sub} websocket channel 为 None")
                return
            if "Ticker" in channel:
                await self.on_ticket(data)

            if "deals" in channel:
                await self.on_trade(data)

            if "increase" in channel:
                await self.on_increase_depth(data)

            # 进行 ping/pong 保活
            if time.time() - self.websocket_time > websocket_pong:
                await self.pong('{"method":"PING"}')
                self.websocket_time = time.time()

        except Exception as e:
            self.gateway.logger.error(f"解析 websocket receive 有错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_ticket(self, data) -> NoReturn:
        try:
            symbol = data.get("s")
            rhino_ticker_object = RhinoDepth(
                real_pair=symbol,
                gateway_send_time=data.get("t"),
                rhino_get_time=int(time.time() * 1000),
                buy_price1=float(data.get("d").get("b")),
                buy_amount1=float(data.get("d").get("B")),
                sell_price1=float(data.get("d").get("a")),
                sell_amount1=float(data.get("d").get("A")),
            )
            # await self.on_transfer(rhino_ticker_object)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOTICKER.value)(
                rhino_ticker_object)
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
                key=symbol + MethodEnum.GETTICKER.value
            )
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket depth 数据错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_trade(self, data) -> NoReturn:
        try:
            trades = data.get("d").get("deals")
            symbol = data.get("s")
            rhino_trades = []
            for trade in trades:
                rhino_trade = RhinoTrade(
                    real_pair=symbol,
                    price=float(trade.get("p")),
                    amount=float(trade.get("v")),
                    time=trade.get("t"),
                    direction=OrderDirection.BUY.value if trade.get("S") == 1 else OrderDirection.SELL.value,
                    trade_end_time=int(time.time() * 1000)
                )
                rhino_trades.append(rhino_trade)
            # await self.on_transfer(rhino_ticker_object)
            await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOTRADE.value)(
                rhino_trades)
            websocket_listen = WebsocketListen(
                time=int(1000 * time.time()),
                gateway=self.gateway.exchange_sub,
                key=symbol + MethodEnum.GETTRADES.value
            )
            await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket depth 数据错误")
            self.gateway.logger.error(traceback.format_exc())

    async def on_increase_depth(self, data) -> NoReturn:
        """
        注意该方法并不是完整版本
        """
        try:
            symbol = data.get("s")
            asks = data.get("d").get("asks", [])
            # bids = data.get("d").get("bids", [])
            t = data.get("t")
            rhino_increase_depths = []
            for ask in asks:
                if float(ask.get("v")) != 0:
                    rhino_increase_depth = RhinoIncreaseDepth(
                        amount=float(ask.get("v")),
                        price=float(ask.get("p")),
                        real_pair=symbol,
                        gateway_send_time=t,
                        rhino_get_time=int(time.time() * 1000),
                        direction=OrderDirection.SELL.value
                    )
                    rhino_increase_depths.append(rhino_increase_depth)
                # await self.on_transfer(rhino_ticker_object)
            # for bid in bids:
            #     # if float(ask.get("v")) != 0:
            #     rhino_increase_depth = RhinoIncreaseDepth(
            #         amount=float(bid.get("v")),
            #         price=float(bid.get("p")),
            #         real_pair=symbol,
            #         gateway_send_time=t,
            #         rhino_get_time=int(time.time() * 1000),
            #         direction=OrderDirection.BUY.value
            #     )
            #     rhino_increase_depths.append(rhino_increase_depth)
            if len(rhino_increase_depths) > 0:
                await self.on_transfers.get(self.gateway.exchange_sub + RhinoDataType.RHINOSUBDEPTH.value)(
                    rhino_increase_depths)
                websocket_listen = WebsocketListen(
                    time=int(1000 * time.time()),
                    gateway=self.gateway.exchange_sub,
                    key=symbol + MethodEnum.GETSUBDEPTHS.value
                )
                await self.on_heart(websocket_listen)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} 解析 websocket depth 数据错误")
            self.gateway.logger.error(traceback.format_exc())
