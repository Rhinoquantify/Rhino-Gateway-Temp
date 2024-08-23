import hashlib
import hmac
import time
import traceback
from typing import NoReturn, Union, Dict, List, Any, Callable

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import Exchange, ExchangeSub, CexOrderForceType, Chain
from RhinoObject.Rhino.RhinoObject import MixInfo, SymbolInfo, RhinoConfig, SymbolInfos, RhinoOrder, \
    RhinoLeverage, CallableMethods, RhinoAccount, RhinoFundingRate, RhinoKline, RhinoTrade, RhinoBalance
from RhinoObject.RhinoRequest.RhinoRequest import RhinoRequest
from RhinoObject.RhinoRequest.RhunoRequestEnum import Method

from RhinoGateway.Base.BaseGateway.BaseGateway import BaseGateway
from RhinoGateway.Base.RestFul.RestClient import RestClient
from RhinoGateway.Util.Util import get_RhinoDepth_from_MixInfo

rest_api = "https://api.gateio.ws/api/v4"
websocket_url = ""


class GateSpotGateway(BaseGateway):
    def __init__(self, logger: RhinoLogger, rhino_collect_config: RhinoConfig, loop):
        super().__init__(logger, rhino_collect_config, loop)
        self.exchange = Exchange.GATE.value
        self.exchange_sub = ExchangeSub.GATEUSPOT.value

        self.rest = GateSpotRestGateway(self)

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
        pass

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
        pass

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


class GateSpotRestGateway(RestClient):

    def __init__(self, gateway: GateSpotGateway):
        super().__init__(gateway)

    async def sign(self, request: RhinoRequest):
        try:
            request_time = str(int(time.time()))
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
            encode_str += "&Timestamp=" + request_time
            encode_byte = bytes(encode_str[1:], encoding="utf8")
            signature = hmac.new(secret.encode('utf-8'), encode_byte, hashlib.sha512).hexdigest()
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

        real_pair = symbol_info.real_pair

        params = {
            "currency_pair": real_pair,
            "limit": symbol_info.depth_limit
        }
        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/spot/order_book",
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
        rhino_depth.cex_exchange = Exchange.GATE.value
        rhino_depth.cex_exchange_sub = ExchangeSub.GATEUSPOT.value
        rhino_depth.gateway_send_time = data.get("current")
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

    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None):

        extra = {
            "key": symbol_info.sign_key,
            "secret": symbol_info.sign_secret,
        }

        rhino_request = RhinoRequest(
            method=Method.GET.value,
            url=rest_api + "/spot/currencies",
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

    async def on_get_coin_info(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                               on_transfer: Callable = None, on_transfer_extra_data: Any = None):
        symbol_infos = []

        # for d in data:
        #     symbol_info = SymbolInfo(
        #         symbol=d.get("coin")
        #     )
        #     chains = []
        #     contracts = []
        #     withdraw_enables = []
        #     deposit_enables = []
        #     withdraw_amounts = []
        #     for network in d.get("networkList"):
        #         chains.append(network.get("network"))
        #         contracts.append(network.get("contract"))
        #         deposit_enables.append(network.get("depositEnable"))
        #         withdraw_enables.append(network.get("withdrawEnable"))
        #         withdraw_amounts.append(network.get("withdrawFee"))
        #     symbol_info.chains = chains
        #     symbol_info.chains_contracts = contracts
        #     symbol_info.withdraw_enables = withdraw_enables
        #     symbol_info.deposit_enables = deposit_enables
        #     symbol_info.withdraw_amounts = withdraw_amounts
        #     symbol_infos.append(symbol_info)
        await on_transfer(
            symbol_infos, on_transfer_extra_data
        )

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

        rhino_order = RhinoOrder(
            order_id=order_id,
        )
        await on_transfer(rhino_order, on_transfer_extra_data)

    async def on_error(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass

    async def on_fail(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass
