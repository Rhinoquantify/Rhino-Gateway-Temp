from abc import ABC, abstractmethod
from typing import NoReturn, Union, Any, List

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Rhino.RhinoEnum import MethodEnum
from RhinoObject.Rhino.RhinoObject import SymbolInfo, RhinoDepth, RhinoTrade, RhinoConfig, SymbolInfos, RhinoOrder, \
    RhinoLeverage, CallableMethods, RhinoAccount, RhinoFundingRate, RhinoKline


class BaseGateway(ABC):

    def __init__(self, logger: RhinoLogger, rhino_config: RhinoConfig, loop=None):
        self.collect = None
        self.logger = logger
        self.rhino_config = rhino_config
        self.loop = loop
        self.methods = {}

        self.websocket = None

    #     self.init(logger, rhino_config)
    #     self.init_methods()
    #
    # def init(self, logger: RhinoLogger, rhino_config: RhinoConfig) -> NoReturn:
    #     collect_type = rhino_config.collect_type
    #     if collect_type == DealDataType.REDIS.value:
    #         if rhino_config.redis_config.DataType == RedisDataType.SET.value:
    #             self.collect = RhinoSetGetRedis.get_instance(logger, rhino_config)
    #         elif rhino_config.redis_config.DataType == RedisDataType.SUBSCRIBE.value:
    #             pass

    def init_methods(self) -> NoReturn:
        """
        用于其他模块的回掉
        :return:
        """
        self.methods[MethodEnum.GETDEPTHS.name] = self.get_depths
        self.methods[MethodEnum.GETTRADES.name] = self.get_trades

    def get_method(self, method: Union[MethodEnum]) -> Any:
        return self.methods.get(method, None)

    async def set_data(self, rhino_data: Union[RhinoDepth, RhinoTrade]) -> NoReturn:
        """
        使用 aredis 向 redis 中添加 depth 数据
        :param rhino_depth:
        :return:
        """
        self.logger.debug(f"{rhino_data}")
        # rhino_data.store_time = int(time.time() * 1000)  # 更新网络存储时间
        # await self.collect.set_data(rhino_data)

    @abstractmethod
    async def get_time(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_exchange_infos(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_coin_info(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_depths(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_public_trades(self, rhino_trade: RhinoTrade, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def submit_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def batch_submit_orders(self, rhino_orders: List[RhinoOrder],
                                  callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def cancel_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def cancel_batch_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def cancel_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def close_position(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        """
        平仓
        """
        pass

    @abstractmethod
    async def close_positions(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        """
        平仓
        """
        pass

    @abstractmethod
    async def get_account(self, rhino_account: RhinoAccount, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def withdraw(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def withdraw_status(self, symbol_info: SymbolInfo, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def update_leverage(self, rhino_leverage: RhinoLeverage,
                              callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_position(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_positions(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_order(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_open_orders(self, rhino_order: RhinoOrder, callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_funding_rate(self, rhino_funding_rate: RhinoFundingRate,
                               callable_methods: CallableMethods = None) -> NoReturn:
        pass

    @abstractmethod
    async def get_funding_rates(self, rhino_funding_rates: List[RhinoFundingRate],
                                callable_methods: CallableMethods = None):
        pass

    @abstractmethod
    async def get_klines(self, rhino_kline: RhinoKline,
                         callable_methods: CallableMethods = None):
        pass

    @abstractmethod
    async def get_pending(self, symbol_info: SymbolInfo,
                          callable_methods: CallableMethods = None):
        pass

    @abstractmethod
    async def subscribe(self, symbol_infos: SymbolInfos, callable_methods: CallableMethods = None) -> NoReturn:
        self.websocket.on_heart = callable_methods.on_heart

        subscribe_state = symbol_infos.subscribes.get(self.exchange_sub, None)
        if subscribe_state is None:
            self.logger.error(f"{self.exchange_sub} subscribe 状态遗漏")
        if subscribe_state:
            self.logger.info(f"{self.exchange_sub} 准备订阅")
            await self.websocket.subscribe(symbol_infos, callable_methods)

    async def unsubscribe(self):
        await self.websocket.unsubscribe()

    async def resubscribe(self):
        await self.websocket.reconnect()
