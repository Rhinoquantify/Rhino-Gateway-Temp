# -*- coding:utf-8 -*-

from distutils.core import setup

setup(
    name="RhinoGateway",
    version="0.1.27",
    description="The Rhino quantify of gateway",
    url="",
    author="XiNiu",
    author_email="xiniublog@163.com",
    license="GPL",
    packages=[
        "RhinoGateway",
        "RhinoGateway.Base",
        "RhinoGateway.Base.RestFul",
        "RhinoGateway.Base.WebSocket",
        "RhinoGateway.Base.BaseGateway",
        "RhinoGateway.Gateways",
        "RhinoGateway.Gateways.Binance",
        "RhinoGateway.Gateways.Binance.BinanceSpotGateway",
        "RhinoGateway.Gateways.Binance.BinanceUSwapGateway",
        "RhinoGateway.Gateways.BSCGateway",
        "RhinoGateway.Gateways.BSCGateway.BSCSpotGateway",
        "RhinoGateway.Gateways.Mexc",
        "RhinoGateway.Gateways.Mexc.MexcSpotGateway",
        "RhinoGateway.Util",
    ]
)
