import threading
from datetime import datetime, timedelta
from datetime import timezone
from typing import Optional, Callable

import zmq
import zmq.auth

from numpy import ndarray
from pandas import DataFrame
from pymysql import connect
from rqdatac import init
from rqdatac.services.get_price import get_price
from rqdatac.services.future import get_dominant_price
from rqdatac.services.basic import all_instruments
from rqdatac.services.calendar import get_next_trading_date
from rqdatac.share.errors import RQDataError

from vnpy.trader.database import DB_TZ
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import round_to, ZoneInfo
from vnpy.trader.datafeed import BaseDatafeed

# MT5 constants
# dictName['mt5.PERIOD_M1'] = {'mt5 value': 1, 'int minutes': 1, 'int seconds': 60}
# dictName['mt5.PERIOD_M2'] = {'mt5 value': 2, 'int minutes': 2, 'int seconds': 120}
# dictName['mt5.PERIOD_M3'] = {'mt5 value': 3, 'int minutes': 3, 'int seconds': 180}
# dictName['mt5.PERIOD_M4'] = {'mt5 value': 4, 'int minutes': 4, 'int seconds': 240}
# dictName['mt5.PERIOD_M5'] = {'mt5 value': 5, 'int minutes': 5, 'int seconds': 300}
# dictName['mt5.PERIOD_M6'] = {'mt5 value': 6, 'int minutes': 6, 'int seconds': 360}
# dictName['mt5.PERIOD_M10'] = {'mt5 value': 10, 'int minutes': 10, 'int seconds': 600}
# dictName['mt5.PERIOD_M12'] = {'mt5 value': 12, 'int minutes': 12, 'int seconds': 720}
# dictName['mt5.PERIOD_M15'] = {'mt5 value': 15, 'int minutes': 15, 'int seconds': 900}
# dictName['mt5.PERIOD_M20'] = {'mt5 value': 20, 'int minutes': 20, 'int seconds': 1200}
# dictName['mt5.PERIOD_M30'] = {'mt5 value': 30, 'int minutes': 30, 'int seconds': 1800}
# dictName['mt5.PERIOD_H1'] = {'mt5 value': 16385, 'int minutes': 60, 'int seconds': 3600}
# dictName['mt5.PERIOD_H2'] = {'mt5 value': 16386, 'int minutes': 120, 'int seconds': 7200}
# dictName['mt5.PERIOD_H3'] = {'mt5 value': 16387, 'int minutes': 180, 'int seconds': 10800}
# dictName['mt5.PERIOD_H4'] = {'mt5 value': 16388, 'int minutes': 240, 'int seconds': 14400}
# dictName['mt5.PERIOD_H6'] = {'mt5 value': 16390, 'int minutes': 360, 'int seconds': 21600}
# dictName['mt5.PERIOD_H8'] = {'mt5 value': 16392, 'int minutes': 480, 'int seconds': 28800}
# dictName['mt5.PERIOD_H12'] = {'mt5 value': 16396, 'int minutes': 720, 'int seconds': 43200}
# dictName['mt5.PERIOD_D1'] = {'mt5 value': 16408, 'int minutes': 1440, 'int seconds': 86400}
# dictName['mt5.PERIOD_W1'] = {'mt5 value': 32769, 'int minutes': 10080, 'int seconds': 604800}
# dictName['mt5.PERIOD_MN1'] = {'mt5 value': 49153, 'int minutes': 43200, 'int seconds': 2592000}
PERIOD_M1: int = 1
PERIOD_M3: int = 3
PERIOD_M5: int = 5
PERIOD_M6: int = 6
PERIOD_M10: int = 10
PERIOD_M12: int = 12
PERIOD_M15: int = 15
PERIOD_M20: int = 20
PERIOD_H1: int = 16385
PERIOD_D1: int = 16408

FUNCTION_QUERYCONTRACT: int = 0
FUNCTION_QUERYORDER: int = 1
FUNCTION_QUERYHISTORY: int = 2
FUNCTION_SUBSCRIBE: int = 3
FUNCTION_SENDORDER: int = 4
FUNCTION_CANCELORDER: int = 5

# Bar interval map
INTERVAL_VT2MT: dict[Interval, int] = {
    Interval.MINUTE: PERIOD_M1,
    Interval.MINUTE3: PERIOD_M3,
    Interval.MINUTE5: PERIOD_M5,
    Interval.MINUTE6: PERIOD_M6,
    Interval.MINUTE10: PERIOD_M10,
    Interval.MINUTE12: PERIOD_M12,
    Interval.MINUTE15: PERIOD_M15,
    Interval.MINUTE20: PERIOD_M20,
    Interval.HOUR: PERIOD_H1,
    Interval.DAILY: PERIOD_D1,
}


# INTERVAL_VT2MT: dict[Interval, str] = {
#     Interval.MINUTE: "1m",
#     Interval.MINUTE3: "3m",
#     Interval.MINUTE5: "5m",
#     Interval.HOUR: "60m",
#     Interval.DAILY: "1d",
# }

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()         # no need to adjust for daily bar
}

FUTURES_EXCHANGES: set[Exchange] = {
    Exchange.JPX,
    Exchange.OTC
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")
UTC_TZ: ZoneInfo = ZoneInfo("UTC")
NYC_TZ: ZoneInfo = ZoneInfo("America/New_York")

def to_mt_symbol(symbol: str, exchange: Exchange, all_symbols: ndarray) -> str:
    """将交易所代码转换为米筐代码"""
    rq_symbol: str = f"JP225"
    return rq_symbol


class Mt5Datafeed(BaseDatafeed):
    """MT5数据服务接口"""

    default_name: str = "MT5"

    default_setting: dict[str, str] = {
        "Server Host": "localhost",
        "REQ Port": "6888",
        "SUB Port": "8666",
    }

    exchanges: list[Exchange] = [Exchange.OTC]

    def __init__(self):
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = False
        self.symbols: ndarray = None

        self.callbacks: dict[str, Callable] = {
            "account": self.on_account_info,
            "price": self.on_price_info,
            "order": self.on_order_info,
            "position": self.on_position_info
        }

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        if not self.username:
            output("RQData数据服务初始化失败：用户名为空！")
            return False
        if not self.password:
            output("RQData数据服务初始化失败：密码为空！")
            return False
        try:
            self.client = Mt5Client(self)
            self.connect(self.default_setting)
        except RQDataError as ex:
            output(f"RQData数据服务初始化失败：{ex}")
            return False
        except RuntimeError as ex:
            output(f"发生运行时错误：{ex}")
            return False
        except Exception as ex:
            output(f"发生未知异常：{ex}")
            return False

        self.inited = True
        return True

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        address: str = setting["Server Host"]
        req_port: str = setting["REQ Port"]
        sub_port: str = setting["SUB Port"]

        req_address: str = f"tcp://{address}:{req_port}"
        sub_address: str = f"tcp://{address}:{sub_port}"

        self.client.start(req_address, sub_address)

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[list[BarData]]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        history: list[BarData] = []

        start_time: str = generate_mt5_datetime(req.start)
        end_time: str = generate_mt5_datetime(req.end)

        mt5_req: dict = {
            "type": FUNCTION_QUERYHISTORY,
            "symbol": to_mt_symbol(req.symbol, req.exchange, self.symbols),
            "interval": INTERVAL_VT2MT[req.interval],
            "start_time": start_time,
            "end_time": end_time,
        }
        packet: dict = self.client.send_request(mt5_req)

        if packet["result"] == -1:
            print("Query kline history failed")
        else:
            for d in packet["data"]:
                bar: BarData = BarData(
                    symbol=req.symbol.replace('.', '-'),
                    exchange=req.exchange,
                    datetime=generate_db_datetime(d["time"]),
                    interval=req.interval,
                    volume=d["real_volume"],
                    open_price=d["open"],
                    high_price=d["high"],
                    low_price=d["low"],
                    close_price=d["close"],
                    gateway_name=self.default_name
                )
                history.append(bar)

            data: dict = packet["data"]
            begin: datetime = generate_db_datetime(data[0]["time"])
            end: datetime = generate_db_datetime(data[-1]["time"])

            msg: str = f"Query kline history finished, {req.symbol.replace('.', '-')} - {req.interval.value}, {begin} - {end}"
            print(msg)

        return history

    def close(self) -> None:
        """Close server connections"""
        self.client.stop()
        self.client.join()

    def callback(self, packet: dict) -> None:
        """General callback for receiving packet data"""
        type_: str = packet["type"]
        callback_func: callable = self.callbacks.get(type_, None)

        if callback_func:
            callback_func(packet)

    def on_account_info(self, packet: dict) -> None:
        """Callback of account balance update"""
        print("on_account_info: " + str(packet))

    def on_price_info(self, packet: dict) -> None:
        """Callback of market price update"""
        print("on_price_info: " + str(packet))

    def on_order_info(self, packet: dict) -> None:
        """Callback of order update"""
        print("on_order_info: " + str(packet))

    def on_position_info(self, packet: dict) -> None:
        """Callback of holding positions update"""
        print("on_order_info: " + str(packet))

    def _query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[list[BarData]]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        # 股票期权不添加交易所后缀
        if exchange in [Exchange.SSE, Exchange.SZSE] and symbol in self.symbols:
            rq_symbol: str = symbol
        else:
            rq_symbol: str = to_mt_symbol(symbol, exchange, self.symbols)

        # 检查查询的代码在范围内
        if rq_symbol not in self.symbols:
            output(f"RQData查询K线数据失败：不支持的合约代码{req.vt_symbol}")
            return []

        rq_interval: str = INTERVAL_VT2MT.get(interval)
        if not rq_interval:
            output(f"RQData查询K线数据失败：不支持的时间周期{req.interval.value}")
            return []

        # 为了将米筐时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        # 只对衍生品合约才查询持仓量数据
        fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_price(
            rq_symbol,
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),        # 为了查询夜盘数据
            adjust_type="none"
        )

        data: list[BarData] = []

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                dt: datetime = row.Index[1].to_pydatetime() - adjustment
                dt: datetime = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ"
                )

                data.append(bar)

        return data

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> Optional[list[TickData]]:
        """查询Tick数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        start: datetime = req.start
        end: datetime = req.end

        # 股票期权不添加交易所后缀
        if exchange in [Exchange.SSE, Exchange.SZSE] and symbol in self.symbols:
            rq_symbol: str = symbol
        else:
            rq_symbol: str = to_mt_symbol(symbol, exchange, self.symbols)

        if rq_symbol not in self.symbols:
            output(f"RQData查询Tick数据失败：不支持的合约代码{req.vt_symbol}")
            return []

        # 只对衍生品合约才查询持仓量数据
        fields: list = [
            "open",
            "high",
            "low",
            "last",
            "prev_close",
            "volume",
            "total_turnover",
            "limit_up",
            "limit_down",
            "b1",
            "b2",
            "b3",
            "b4",
            "b5",
            "a1",
            "a2",
            "a3",
            "a4",
            "a5",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
        ]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_price(
            rq_symbol,
            frequency="tick",
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),        # 为了查询夜盘数据
            adjust_type="none"
        )

        data: list[TickData] = []

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                dt: datetime = row.Index[1].to_pydatetime()
                dt: datetime = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                tick: TickData = TickData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    open_price=row.open,
                    high_price=row.high,
                    low_price=row.low,
                    pre_close=row.prev_close,
                    last_price=row.last,
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    limit_up=row.limit_up,
                    limit_down=row.limit_down,
                    bid_price_1=row.b1,
                    bid_price_2=row.b2,
                    bid_price_3=row.b3,
                    bid_price_4=row.b4,
                    bid_price_5=row.b5,
                    ask_price_1=row.a1,
                    ask_price_2=row.a2,
                    ask_price_3=row.a3,
                    ask_price_4=row.a4,
                    ask_price_5=row.a5,
                    bid_volume_1=row.b1_v,
                    bid_volume_2=row.b2_v,
                    bid_volume_3=row.b3_v,
                    bid_volume_4=row.b4_v,
                    bid_volume_5=row.b5_v,
                    ask_volume_1=row.a1_v,
                    ask_volume_2=row.a2_v,
                    ask_volume_3=row.a3_v,
                    ask_volume_4=row.a4_v,
                    ask_volume_5=row.a5_v,
                    gateway_name="RQ"
                )

                data.append(tick)

        return data

    def _query_dominant_history(self, req: HistoryRequest, output: Callable = print) -> Optional[list[BarData]]:
        """查询期货主力K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        rq_interval: str = INTERVAL_VT2MT.get(interval)
        if not rq_interval:
            output(f"RQData查询K线数据失败：不支持的时间周期{req.interval.value}")
            return []

        # 为了将米筐时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        # 只对衍生品合约才查询持仓量数据
        fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_dominant_price(
            symbol.upper(),                         # 合约代码用大写
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),    # 为了查询夜盘数据
            adjust_type="pre",                      # 前复权
            adjust_method="prev_close_ratio"        # 切换前一日收盘价比例复权
        )

        data: list[BarData] = []

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                dt: datetime = row.Index[1].to_pydatetime() - adjustment
                dt: datetime = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ"
                )

                data.append(bar)

        return data


class Mt5Client:
    """
    The client for connecting to MT5 server based on ZeroMQ.
    """

    def __init__(self, gateway: Mt5Datafeed):
        """
        The init method of the client.

        gateway: the parent gateway object for pushing callback data
        """
        self.gateway: Mt5Datafeed = gateway

        self.context: zmq.Context = zmq.Context()
        self.socket_req: zmq.Socket = self.context.socket(zmq.REQ)
        # self.socket_sub: zmq.Socket = self.context.socket(zmq.SUB)
        # self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self.active: bool = False
        self.thread: threading.Thread = None
        self.lock: threading.Lock = threading.Lock()

    def start(self, req_address: str, sub_address: str) -> None:
        """Start ZeroMQ connection"""
        if self.active:
            return

        # Connect ZeroMQ sockets
        self.socket_req.connect(req_address)
        # self.socket_sub.connect(sub_address)

        # Start the thread to process incoming data
        self.active: bool = True
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def stop(self) -> None:
        """Stop ZeroMQ connection"""
        if not self.active:
            return

        self.socket_req.close()

        self.active = False

    def join(self) -> None:
        """Join to wait the thread exit loop"""
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None

    def run(self) -> None:
        """Function run in the thread"""
        # while self.active:
        #     if not self.socket_sub.poll(1000):
        #         continue
        #
        #     data: dict = self.socket_sub.recv_json(flags=zmq.NOBLOCK)
        #     self.callback(data)

        # Close ZeroMQ sockets
        # self.socket_req.close()
        # self.socket_sub.close()

    def callback(self, data: dict) -> None:
        """General callback function"""
        self.gateway.callback(data)

    def send_request(self, req: dict) -> dict:
        """Send request to server"""
        if not self.active:
            return {}

        self.socket_req.send_json(req)
        data: dict = self.socket_req.recv_json()
        return data


def generate_db_datetime(timestamp: int) -> datetime:
    """Generate db datetime"""
    dt: datetime = datetime.strptime(str(timestamp), "%Y.%m.%d %H:%M")
    mt5_dt: datetime = dt.replace(tzinfo=get_mt5_timezone())
    db_dt: datetime = mt5_dt.astimezone(DB_TZ)
    return db_dt

def generate_mt5_datetime(datetime: datetime) -> str:
    """Generate MT5(Summer: GMT+3, Winter: GMT+2) datetime"""
    mt5_dt: dict = datetime.astimezone(get_mt5_timezone())
    mt5_dt: dict = mt5_dt.replace(tzinfo=None)
    dt: str = mt5_dt.isoformat()
    dt: str = dt.replace('T', ' ')
    return dt

def get_mt5_timezone() -> timezone:
    """Generate MT5(Summer: GMT+3, Winter: GMT+2) timezone"""
    now: datetime = datetime.now(NYC_TZ)
    # America summer time(DST) is from the second Sunday in March to the first Sunday in November
    if now.dst().seconds > 0:
        return timezone(timedelta(hours=+3))
    else:
        return timezone(timedelta(hours=+2))
