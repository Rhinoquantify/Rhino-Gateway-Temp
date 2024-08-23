import asyncio
import json
import time
import traceback
from abc import abstractmethod
from typing import NoReturn

import aiohttp
from RhinoObject.RhinoRequest.RhinoRequest import RhinoRequest
from RhinoObject.RhinoRequest.RhunoRequestEnum import Method


class RestClient(object):

    def __init__(self, gateway):
        self.gateway = gateway

    @abstractmethod
    async def sign(self, request: RhinoRequest) -> NoReturn:
        pass

    async def fetch(self, request: RhinoRequest) -> NoReturn:
        # 进行认证
        if request.is_sign:
            request = await self.sign(request)

        url = request.url
        params = request.params
        data = request.data
        method = request.method
        headers = request.headers
        success_call = request.callback
        fail_call = request.on_failed
        error_call = request.on_error
        transfer_call = request.on_transfer
        timeout_call = request.on_timeout
        extra = request.extra
        transfer_call_extra_data = request.on_transfer_extra_data
        proxy = request.proxy
        special_sign = request.special_sign
        # timeout = request.timeout
        timeout = aiohttp.ClientTimeout(total=request.timeout)

        response = None
        text = ""
        start_time = time.time()

        try:

            # self.gateway.logger.info(f"URL 是 {url} {params} {data}")

            async with aiohttp.ClientSession() as client:
                if method == Method.GET.value:
                    # async with client.get(url, params=params, headers=headers, timeout=timeout,
                    #                       proxy=proxy) as resp:
                    """
                    mexc get 如果带 params 参数，就会导致失败
                    """
                    async with client.get(url, headers=headers, timeout=timeout,
                                          proxy=proxy) as resp:
                        response = resp
                        code = resp.status
                        text = await resp.text()

                elif method == Method.POST.value:
                    if special_sign == "data":
                        async with client.post(url, data=data, headers=headers, timeout=timeout,
                                               proxy=proxy) as resp:
                            response = resp
                            code = resp.status
                            text = await resp.text()

                    elif special_sign == "json":
                        async with client.post(url, json=data, headers=headers, timeout=timeout,
                                               proxy=proxy) as resp:
                            response = resp
                            code = resp.status
                            text = await resp.text()

                elif method == Method.DELETE.value:

                    async with client.delete(url, params=params, headers=headers, timeout=timeout,
                                             proxy=proxy) as resp:
                        response = resp
                        code = resp.status
                        text = await resp.text()

                else:
                    await error_call(request, None, 0, extra, transfer_call_extra_data)
        except asyncio.TimeoutError as time_error:
            # self.gateway.logger.error(f"{self.gateway.exchange_sub} {url} {params} {data} 获取数据超时")
            # self.gateway.logger.error(f"{traceback.format_exc()}")
            if timeout_call is not None:
                await timeout_call(request, None, 0, extra, transfer_call_extra_data)
            return
        except Exception as e:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} {url} {params} {data} 获取数据错误")
            self.gateway.logger.error(f"{traceback.format_exc()}")
            await error_call(request, None, 0, extra, transfer_call_extra_data)
            return

        if response is None:
            self.gateway.logger.error(f"{self.gateway.exchange_sub} {url} {params} {data} 获取数据错误 数据为空")
            await error_call(request, None, 0, extra, transfer_call_extra_data)
            return

        text_time = 0
        get_time = time.time()

        try:
            text_time = time.time()
            response_data = json.loads(text)
            if code == 200:
                # self.gateway.logger.debug(f"{self.gateway.exchange_sub} {url} {params} {data} 获取数据成功")
                await success_call(request, response_data, code, extra, transfer_call, transfer_call_extra_data)
            else:
                if "binance" in response.real_url.host and response_data.get("code") == -2011:
                    """
                    binance spot: cancel_orders 某一个交易对取消全部订单的时候，如果没有订单会返回 2011 代码
                    """
                    self.gateway.logger.debug(f"{self.gateway.exchange_sub} {url} {params} {data} -2011 code")
                else:
                    self.gateway.logger.error(f"{self.gateway.exchange_sub} {url} {params} {data} 获取数据失败")
                    self.gateway.logger.error(f"{code} {text}")
                await fail_call(request, response_data, code, extra, transfer_call_extra_data)
        except Exception as e:
            self.gateway.logger.error(
                f"{self.gateway.exchange_sub} {url} {params} {data} 获取数据错误 start_time: {start_time} get_time:{get_time} text_time:{text_time}")
            self.gateway.logger.error(f"{traceback.format_exc()}")
            await error_call(request, None, 0, extra, transfer_call_extra_data)
