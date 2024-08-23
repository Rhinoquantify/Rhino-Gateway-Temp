import re
import time
import traceback
from typing import NoReturn, Union, Dict, Callable, Any, List

import sha3
from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import Exchange, ExchangeSub, CexOrderType
from RhinoObject.Rhino.RhinoEnum import MethodEnum
from RhinoObject.Rhino.RhinoObject import MixInfo, SymbolInfo, DEXInfo, RhinoConfig, SymbolInfos, RhinoOrder, \
    RhinoLeverage, CallableMethods, RhinoAccount, RhinoFundingRate, RhinoKline, RhinoTrade, RhinoBalance
from RhinoObject.RhinoRequest.RhinoRequest import RhinoRequest
from RhinoObject.RhinoRequest.RhunoRequestEnum import Method
from eth_abi import encode
from eth_account import Account
from web3 import Web3

from RhinoGateway.Base.BaseGateway.BaseGateway import BaseGateway
from RhinoGateway.Base.RestFul.RestClient import RestClient
from RhinoGateway.Util.Util import get_RhinoDepth_from_MixInfo

contract_address = Web3.toChecksumAddress("****")
ABI = """****"""
rest_api = "https://bsc-mainnet.nodereal.io/v1/****"


class BSCAMMSpotGateway(BaseGateway):
    def __init__(self, logger: RhinoLogger, rhino_collect_config: RhinoConfig, loop):
        super().__init__(logger, rhino_collect_config, loop)
        self.exchange = Exchange.BSC.value
        self.exchange_sub = ExchangeSub.BSCSPOT.value
        self.rest = BSCSpotRestGateway(self)

    async def get_time(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        await self.rest.get_exchange_infos(symbol_info, callable_methods)

    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

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
        await self.rest.get_order(rhino_order, callable_methods)

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
        await self.rest.get_pending(symbol_info, callable_methods)

    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None):
        pass


class BSCSpotRestGateway(RestClient):

    def __init__(self, gateway: BSCAMMSpotGateway):
        super().__init__(gateway)
        self.method_encryptions = {}

    def get_depths_data(self, symbol_info: SymbolInfo) -> str:
        pair_addresses = []
        dexs: Dict[int, DEXInfo] = symbol_info.dex
        for index, dex in dexs.items():
            pair_addresses.append(Web3.toChecksumAddress(dex.pair_contract))
        encode_data = encode(['address[]'], [pair_addresses]).hex()
        method = MethodEnum.GETDEPTHS.value
        method_encryption = self.method_encryptions.get(method, None)
        if method_encryption is None:
            encryption = sha3.keccak_256()
            encryption.update(b"getDepth(address[])")  # 这个目前有 BUG
            method_hex = encryption.hexdigest()[0:8]
            self.method_encryptions[method] = method_hex
            method_encryption = method_hex
        return "0x" + method_encryption + encode_data

    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        contract_data = self.get_depths_data(symbol_info)
        data = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    'to': Web3.toChecksumAddress(contract_address),
                    'data': contract_data
                },
                "latest"
            ],
            "id": 1
        }
        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api,
            params=None,
            data=data,
            headers=symbol_info.headers,
            callback=self.on_get_depths,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=symbol_info,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            special_sign="json",
            proxy=symbol_info.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_depths(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                            on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        # 解析 data
        try:
            rhino_depth = get_RhinoDepth_from_MixInfo(extra)
            rhino_depth.rhino_get_time = int(time.time() * 1000)
            rhino_depth.gateway_send_time = int(time.time() * 1000)
            # 前面 [66:] 是 data 中表示数组有几个的
            result = data.get("result")[66:]
            depth_limit = int(result[0:64], 16)
            reverses = [int(reverse, 16) for reverse in re.findall(r'.{64}', result[64:])]
            for i in range(1, depth_limit + 1):
                setattr(rhino_depth, f"pool{i}_reverse0", reverses[(i - 1) * 2])
                setattr(rhino_depth, f"pool{i}_reverse1", reverses[(i - 1) * 2 + 1])
            # self.gateway.logger.debug(
            #     f"{self.gateway.exchange_sub} depth {extra.__str__()} 获取数据消耗时间 {int(time.time() * 1000) - extra.start_time}")
            # await self.gateway.set_data(rhino_depth)
            await on_transfer(rhino_depth, on_transfer_extra_data)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} depth {extra.__str__()} 回调函数出错")
            self.gateway.logger.error(traceback.format_exc())

    def get_account_token_data(self, rhino_account: RhinoAccount) -> str:
        symbol_contract = rhino_account.symbol_contract
        encode_data = encode(['address'], [symbol_contract]).hex()
        method = MethodEnum.GETACCOUNT.value
        method_encryption = self.method_encryptions.get(method, None)
        if method_encryption is None:
            encryption = sha3.keccak_256()
            encryption.update(b"tokenAmount(address)")
            method_hex = encryption.hexdigest()[0:8]
            self.method_encryptions[method] = method_hex
            method_encryption = method_hex
        return "0x" + method_encryption + encode_data

    async def get_account(self, rhino_account: RhinoAccount, callable_methods: CallableMethods = None) -> NoReturn:

        data = ""

        if len(rhino_account.symbol_contract) > 0:
            contract_data = self.get_account_token_data(rhino_account)
            data = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [
                    {
                        'to': Web3.toChecksumAddress(contract_address),
                        'data': contract_data
                    },
                    "latest"
                ],
                "id": 1
            }
        else:
            data = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [f"{rhino_account.address}", "latest"],
                "id": 1
            }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api,
            params=None,
            data=data,
            headers=rhino_account.headers,
            callback=self.on_get_account,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_account.time_out,
            extra=rhino_account,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            special_sign="json",
            proxy=rhino_account.proxy
        )

        await self.fetch(rhino_request)

    async def on_get_account(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                             on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        # 解析 data
        try:
            rhino_account: RhinoAccount = RhinoAccount()
            amount = int(data.get("result"), 16)
            rhino_balance = RhinoBalance()
            rhino_balance.available = amount
            rhino_balance.balance = amount
            rhino_account.balance_list = [rhino_balance]
            await on_transfer(rhino_account, on_transfer_extra_data)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} depth {extra.__str__()} 回调函数出错")
            self.gateway.logger.error(traceback.format_exc())

    def get_exact_submit_order_data(self, rhino_order: RhinoOrder):
        encode_data_info = [rhino_order.amount_in, rhino_order.amount_out, rhino_order.receive_address,
                            rhino_order.routers, rhino_order.token_contracts]
        encode_data = encode(['uint256', 'uint256', 'address', 'address[]', 'address[]'], encode_data_info).hex()
        method = "submit_exact_order_data"
        method_encryption = self.method_encryptions.get(method, None)
        if method_encryption is None:
            encryption = sha3.keccak_256()
            encryption.update(b"swapExactCoin((uint256,uint256,address,address[],address[]))")
            method_hex = encryption.hexdigest()[0:8]
            self.method_encryptions[method] = method_hex
            method_encryption = method_hex

        transaction = {
            "from": rhino_order.sign_key,  # 发送交易的钱包地址
            'to': Web3.toChecksumAddress(contract_address),  # 合约地址
            "gas": "0x262352",  # Gas 限制
            "gasPrice": Web3.toWei(3, 'gwei'),  # Gas 价格
            'nonce': rhino_order.nonce,  # 交易 nonce
            "data": "0x" + method_encryption + encode_data  # 合约方法和参数的十六进制数据
        }

        return Account.sign_transaction(transaction, rhino_order.sign_secret)

    def get_exact_supporting_fee_submit_order_data(self, rhino_order: RhinoOrder):
        encode_data_info = [rhino_order.amount_in, rhino_order.amount_out, rhino_order.receive_address,
                            rhino_order.routers, rhino_order.token_contracts]
        encode_data = encode(['uint256', 'uint256', 'address', 'address[]', 'address[]'], encode_data_info).hex()
        method = "submit_exact_fee_order_data"
        method_encryption = self.method_encryptions.get(method, None)
        if method_encryption is None:
            encryption = sha3.keccak_256()
            encryption.update(b"swapExactSupportingFeeCoin((uint256,uint256,address,address[],address[]))")
            method_hex = encryption.hexdigest()[0:8]
            self.method_encryptions[method] = method_hex
            method_encryption = method_hex

        transaction = {
            "from": rhino_order.sign_key,  # 发送交易的钱包地址
            'to': Web3.toChecksumAddress(contract_address),  # 合约地址
            "gas": "0x262352",  # Gas 限制
            "gasPrice": Web3.toWei(3, 'gwei'),  # Gas 价格
            'nonce': rhino_order.nonce,  # 交易 nonce
            "data": "0x" + method_encryption + encode_data  # 合约方法和参数的十六进制数据
        }

        return Account.sign_transaction(transaction, rhino_order.sign_secret)

    async def submit_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        if rhino_order.token_burn:
            contract_data = self.get_exact_supporting_fee_submit_order_data(rhino_order)
        else:
            contract_data = self.get_exact_submit_order_data(rhino_order)

        data = {
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [
                contract_data.rawTransaction.hex()
            ],
            "id": 1
        }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api,
            params=None,
            data=data,
            headers=rhino_order.headers,
            callback=self.on_submit_order,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_order.time_out,
            extra=rhino_order,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            special_sign="json",
            proxy=rhino_order.proxy
        )
        await self.fetch(rhino_request)

    async def on_submit_order(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                              on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        # 解析 data
        try:
            rhino_order: RhinoOrder = RhinoOrder()
            rhino_order.eth_tx = data.get("result")
            error = data.get("error", {})
            rhino_order.code = error.get("code", -1)
            rhino_order.error_info = error.get("message", "")
            await on_transfer(rhino_order, on_transfer_extra_data)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} depth {extra.__str__()} 回调函数出错")
            self.gateway.logger.error(traceback.format_exc())

    async def get_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        data = {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [
                rhino_order.eth_tx
            ],
            "id": 1
        }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api,
            params=None,
            data=data,
            headers=rhino_order.headers,
            callback=self.on_get_order,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=rhino_order.time_out,
            extra=rhino_order,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            special_sign="json",
            proxy=rhino_order.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_order(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                           on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        # 解析 data
        try:
            rhino_order: RhinoOrder = RhinoOrder()
            result = data.get("result")

            if result is None:
                rhino_order.state = CexOrderType.FAILED.value
            else:
                logs = result.get("logs")
                if len(logs) == 0:
                    rhino_order.state = CexOrderType.FAILED.value
                else:
                    rhino_order.state = CexOrderType.FILLED.value
                    rhino_order.logs = logs
            await on_transfer(rhino_order, on_transfer_extra_data)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} depth {extra.__str__()} 回调函数出错")
            self.gateway.logger.error(traceback.format_exc())

    def get_withdraw_data(self, symbol_info: SymbolInfo):
        encode_data_info = [0, symbol_info.symbol_contract, symbol_info.to_contract]
        encode_data = encode(['uint256', 'address', 'address'], encode_data_info).hex()
        method = "withdraw"
        method_encryption = self.method_encryptions.get(method, None)
        if method_encryption is None:
            encryption = sha3.keccak_256()
            encryption.update(b"tokenTransfer(uint256,address,address)")
            method_hex = encryption.hexdigest()[0:8]
            self.method_encryptions[method] = method_hex
            method_encryption = method_hex

        transaction = {
            "from": symbol_info.sign_key,  # 发送交易的钱包地址
            'to': Web3.toChecksumAddress(contract_address),  # 合约地址
            "gas": "0x262352",  # Gas 限制
            "gasPrice": Web3.toWei(symbol_info.gas, 'gwei'),  # Gas 价格
            'nonce': symbol_info.nonce,  # 交易 nonce
            "data": "0x" + method_encryption + encode_data  # 合约方法和参数的十六进制数据
        }

        return Account.sign_transaction(transaction, symbol_info.sign_secret)

    async def withdraw(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        contract_data = self.get_withdraw_data(symbol_info)

        data = {
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [
                contract_data.rawTransaction.hex()
            ],
            "id": 1
        }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api,
            params=None,
            data=data,
            headers=symbol_info.headers,
            callback=self.on_withdraw,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=symbol_info,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            special_sign="json",
            proxy=symbol_info.proxy
        )
        await self.fetch(rhino_request)

    async def on_withdraw(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                          on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        # 解析 data
        try:
            rhino_order: RhinoOrder = RhinoOrder()
            result = data.get("result")

            if result is None:
                rhino_order.state = CexOrderType.FAILED.value
            else:
                rhino_order.state = CexOrderType.FILLED.value
                rhino_order.eth_tx = result
            await on_transfer(rhino_order, on_transfer_extra_data)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} depth {extra.__str__()} 回调函数出错")
            self.gateway.logger.error(traceback.format_exc())

    async def get_pending(self, symbol_info: SymbolInfo,
                          callable_methods: CallableMethods = None) -> NoReturn:

        data = {
            "jsonrpc": "2.0",
            "method": "txpool_content",
            "params": [],
            "id": 1
        }

        rhino_request = RhinoRequest(
            method=Method.POST.value,
            url=rest_api,
            params=None,
            data=data,
            headers=symbol_info.headers,
            callback=self.on_get_pending,
            on_failed=self.on_fail if callable_methods.on_failed is None else callable_methods.on_failed,
            on_error=self.on_error if callable_methods.on_error is None else callable_methods.on_error,
            on_transfer=self.gateway.set_data if callable_methods.on_transfer is None else callable_methods.on_transfer,
            timeout=symbol_info.time_out,
            extra=symbol_info,
            on_transfer_extra_data=callable_methods.extra_data,
            is_sign=False,
            special_sign="json",
            proxy=symbol_info.proxy
        )
        await self.fetch(rhino_request)

    async def on_get_pending(self, request: RhinoRequest, data, code: int, extra: MixInfo,
                             on_transfer: Callable = None, on_transfer_extra_data: Any = None) -> NoReturn:
        # 解析 data
        try:
            rhino_orders: List[RhinoOrder] = []
            pending_rhino_orders: List[RhinoOrder] = []
            queue_rhino_orders: List[RhinoOrder] = []

            result = data.get("result")
            for address, data in result.get("pending").items():
                rhino_order: RhinoOrder = RhinoOrder()

                key = list(data.keys())[0]
                info = data.get(key)
                to_address = info.get("to", None)
                input = info.get("input", None)
                gas_price = info.get("gasPrice", None)
                rhino_order.to_contract = to_address
                rhino_order.from_contract = info.get("from", None)
                rhino_order.logs = input
                rhino_order.gas_price = gas_price
                rhino_order.eth_tx = info.get("hash", None)
                pending_rhino_orders.append(rhino_order)

            for address, data in result.get("queued").items():
                rhino_order: RhinoOrder = RhinoOrder()

                key = list(data.keys())[0]
                info = data.get(key)
                to_address = info.get("to", None)
                input = info.get("input", None)
                gas_price = info.get("gasPrice", None)
                rhino_order.to_contract = to_address
                rhino_order.logs = input
                rhino_order.gas_price = gas_price
                rhino_order.eth_tx = info.get("hash", None)
                rhino_order.from_contract = info.get("from", None)
                queue_rhino_orders.append(rhino_order)

            rhino_orders.append(pending_rhino_orders)
            rhino_orders.append(queue_rhino_orders)

            await on_transfer(rhino_orders, on_transfer_extra_data)
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} depth {extra.__str__()} 回调函数出错")
            self.gateway.logger.error(traceback.format_exc())

    async def on_error(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass

    async def on_fail(self, request: RhinoRequest, data: Union[Dict], code: int, extra: MixInfo) -> NoReturn:
        pass
