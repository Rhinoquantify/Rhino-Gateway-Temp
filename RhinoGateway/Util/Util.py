from RhinoObject.Rhino.RhinoObject import RhinoDepth, MixInfo, SymbolInfo


def get_MixInfo_from_SymbolInfo(symbol_info: SymbolInfo) -> MixInfo:
    return MixInfo(
        proxy=symbol_info.proxy,
        symbol=symbol_info.symbol,
        chain=symbol_info.chain,
        key=symbol_info.key,
        pair=symbol_info.pair,
        real_pair=symbol_info.real_pair,
        cex_exchange=symbol_info.cex_exchange,
        cex_exchange_sub=symbol_info.cex_exchange_sub,
        dex_exchange=symbol_info.dex_exchange,
        dex_type=symbol_info.dex_type,
        cex_type=symbol_info.cex_type,
        pair_addresses=symbol_info.pair_addresses,
        from_contract=symbol_info.from_contract,
        headers=symbol_info.headers,
    )


def get_RhinoDepth_from_MixInfo(mix_info: MixInfo) -> RhinoDepth:
    return RhinoDepth(
        symbol=mix_info.symbol,
        chain=mix_info.chain,
        key=mix_info.key,
        pair=mix_info.pair,
        real_pair=mix_info.real_pair,
        cex_exchange=mix_info.cex_exchange,
        cex_exchange_sub=mix_info.cex_exchange_sub,
        dex_exchange=mix_info.dex_exchange,
        dex_type=mix_info.dex_type,
        cex_type=mix_info.cex_type,
        # start_time=mix_info.start_time,
    )
