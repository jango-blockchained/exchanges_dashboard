import datetime
import logging
import threading
import time
from typing import Any, Dict, List, Optional

import requests

from scraper_root.scraper.data_classes import AssetBalance, Position, Tick, Balance, Income, Order, Account
from scraper_root.scraper.persistence.repository import Repository
from scraper_root.scraper.utils import readable

logger = logging.getLogger()


def is_asset_usd_or_derivative(asset: str) -> bool:
    return asset.lower() in ["usdt", "busd", "usd", "usdc"]


def _params_to_str(params: Dict[str, Any]) -> str:
    """Encode params into a query string for EIP-712 signing, preserving insertion order."""
    from urllib.parse import urlencode
    return urlencode(params)


EIP712_DOMAIN = {
    "name": "AsterSignTransaction",
    "version": "1",
    "chainId": 1666,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}


class _AsterDexClientV3:
    """AsterDEX Futures V3 (Pro) client using EIP-712 wallet signing."""

    def __init__(
        self,
        user: str,
        signer: str,
        private_key: str,
        base_url: str = "https://fapi.asterdex.com",
    ) -> None:
        from eth_account.messages import encode_typed_data
        from eth_account import Account

        self.user = user.strip()
        self.signer = signer.strip()
        self._private_key = private_key.strip()
        if self._private_key and not self._private_key.startswith("0x"):
            self._private_key = "0x" + self._private_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/x-www-form-urlencoded",
        })
        self._encode_typed_data = encode_typed_data
        self._Account = Account
        self._last_nonce_us = 0
        self._nonce_seq = 0

    def _next_nonce(self) -> int:
        now_us = int(time.time() * 1_000_000)
        if now_us == self._last_nonce_us:
            self._nonce_seq += 1
        else:
            self._last_nonce_us = now_us
            self._nonce_seq = 0
        return now_us + self._nonce_seq

    def _sign_eip712(self, params: Dict[str, Any]) -> str:
        param_str = _params_to_str(params)

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [{"name": "msg", "type": "string"}],
            },
            "primaryType": "Message",
            "domain": EIP712_DOMAIN,
            "message": {"msg": param_str},
        }
        message = self._encode_typed_data(full_message=typed_data)
        signed = self._Account.sign_message(message, private_key=self._private_key)
        return signed.signature.hex()

    def _request(
        self,
        method: str,
        endpoint: str,
        signed: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url}{endpoint}"
        params = dict(params or {})
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["nonce"] = str(self._next_nonce())
            params["user"] = self.user
            params["signer"] = self.signer
            params["signature"] = self._sign_eip712(params)

        method = method.upper()
        try:
            if method == "GET":
                resp = self.session.get(url, params=params, timeout=30)
            elif method in ("POST", "PUT", "DELETE"):
                resp = self.session.request(method, url, data=params, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            try:
                error_body = e.response.text
            except Exception:
                error_body = "<unable to read response body>"
            logger.error(f"AsterDEX API error on {endpoint}: {e.response.status_code} {error_body}")
            raise
        return resp.json()

    # Public market data
    def get_exchange_info(self, symbol: Optional[str] = None) -> Any:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v3/exchangeInfo", params=params)

    def get_24hr_ticker(self, symbol: Optional[str] = None) -> Any:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v3/ticker/24hr", params=params)

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> Any:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self._request("GET", "/fapi/v3/klines", params=params)

    # Signed account / trading endpoints
    def get_account_info(self, recv_window: Optional[int] = None) -> Any:
        params = {}
        if recv_window is not None:
            params["recvWindow"] = recv_window
        return self._request("GET", "/fapi/v3/account", signed=True, params=params)

    def get_position_risk(self, symbol: Optional[str] = None, recv_window: Optional[int] = None) -> Any:
        params = {}
        if symbol:
            params["symbol"] = symbol
        if recv_window is not None:
            params["recvWindow"] = recv_window
        return self._request("GET", "/fapi/v3/positionRisk", signed=True, params=params)

    def get_open_orders(self, symbol: Optional[str] = None, recv_window: Optional[int] = None) -> Any:
        params = {}
        if symbol:
            params["symbol"] = symbol
        if recv_window is not None:
            params["recvWindow"] = recv_window
        return self._request("GET", "/fapi/v3/openOrders", signed=True, params=params)

    def get_income(
        self,
        symbol: Optional[str] = None,
        income_type: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
        recv_window: Optional[int] = None,
    ) -> Any:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if income_type:
            params["incomeType"] = income_type
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if recv_window is not None:
            params["recvWindow"] = recv_window
        return self._request("GET", "/fapi/v3/income", signed=True, params=params)


class AsterDexPerps:
    def __init__(self, account: Account, symbols: List[str], repository: Repository):
        logger.info(f"AsterDEX Perps initializing")
        self.account = account
        self.alias = self.account.alias
        self.symbols = symbols
        self.repository = repository

        self.client = _AsterDexClientV3(
            user=account.user_wallet,
            signer=account.signer_wallet,
            private_key=account.private_key,
        )

        # Verify API connection by fetching account info
        try:
            test = self.client.get_account_info()
            if test and 'assets' in test:
                logger.info(f"{self.alias}: REST login successful")
            else:
                logger.error(f"{self.alias}: Failed to login - unexpected response")
                raise SystemExit()
        except Exception as e:
            logger.error(f"{self.alias}: Failed to login: {e}")
            raise SystemExit()

        self.activesymbols = ["BTCUSDT"]

    def start(self):
        logger.info(f'{self.alias}: Starting AsterDEX Perps scraper')

        for symbol in self.symbols:
            symbol_trade_thread = threading.Thread(
                name=f'trade_thread_{symbol}', target=self.sync_current_price, args=(symbol,), daemon=True)
            symbol_trade_thread.start()

        sync_balance_thread = threading.Thread(
            name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        sync_positions_thread = threading.Thread(
            name=f'sync_positions_thread', target=self.sync_positions, daemon=True)
        sync_positions_thread.start()

        sync_trades_thread = threading.Thread(
            name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

        sync_orders_thread = threading.Thread(
            name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
        sync_orders_thread.start()

    def sync_account(self):
        while True:
            try:
                account = self.client.get_account_info()
                if not account or 'assets' not in account:
                    time.sleep(100)
                    continue

                asset_balances = []
                for asset in account['assets']:
                    asset_balances.append(AssetBalance(
                        asset=asset['asset'],
                        balance=float(asset.get('walletBalance', 0)),
                        unrealizedProfit=float(asset.get('unrealizedProfit', 0))
                    ))

                usd_assets = [asset for asset in account['assets'] if asset['asset'] in ['BUSD', 'USDT', 'USDC']]
                total_wallet_balance = sum(float(asset.get('walletBalance', 0)) for asset in usd_assets)
                total_upnl = sum(float(asset.get('unrealizedProfit', 0)) for asset in usd_assets)

                balance = Balance(
                    totalBalance=total_wallet_balance,
                    totalUnrealizedProfit=total_upnl,
                    assets=asset_balances
                )
                self.repository.process_balances(balance, account=self.alias)
                logger.warning(f'{self.alias}: Synced balance')
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process balance: {e}')
            time.sleep(100)

    def sync_positions(self):
        while True:
            try:
                self.activesymbols = ["BTCUSDT"]
                positions = []

                exchange_positions = self.client.get_position_risk()
                if exchange_positions:
                    for pos in exchange_positions:
                        position_size = float(pos.get('positionAmt', 0))
                        if position_size == 0:
                            continue

                        symbol = pos.get('symbol', '')
                        self.activesymbols.append(symbol)

                        positions.append(Position(
                            symbol=symbol,
                            entry_price=float(pos.get('entryPrice', 0)),
                            position_size=abs(position_size),
                            side=pos.get('positionSide', 'LONG'),
                            unrealizedProfit=float(pos.get('unrealizedProfit', 0)),
                            initial_margin=float(pos.get('initialMargin', 0)),
                            market_price=float(pos.get('markPrice', 0))
                        ))

                self.repository.process_positions(positions=positions, account=self.alias)
                logger.warning(f'{self.alias}: Synced positions')
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process positions: {e}')
            time.sleep(250)

    def sync_open_orders(self):
        while True:
            orders = []
            try:
                open_orders = self.client.get_open_orders()
                if open_orders:
                    for open_order in open_orders:
                        order = Order()
                        order.symbol = open_order.get('symbol', '')
                        order.price = float(open_order.get('price', 0))
                        order.quantity = float(open_order.get('origQty', 0))
                        order.side = open_order.get('side', '')
                        order.position_side = open_order.get('positionSide', '')
                        order.type = open_order.get('type', '')
                        orders.append(order)

                self.repository.process_orders(orders=orders, account=self.alias)
                logger.warning(f'{self.alias}: Synced orders')
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process open orders: {e}')

            time.sleep(120)

    def sync_current_price(self, symbol: str):
        while True:
            try:
                symbols_to_fetch = list(set(self.activesymbols + [symbol]))
                for sym in symbols_to_fetch:
                    try:
                        ticker = self.client.get_24hr_ticker(symbol=sym)
                        if ticker:
                            tick = Tick(
                                symbol=sym,
                                price=float(ticker.get('lastPrice', 0)),
                                qty=float(ticker.get('lastQty', 0)),
                                timestamp=int(ticker.get('closeTime', int(time.time() * 1000)))
                            )
                            self.repository.process_tick(tick=tick, account=self.alias)
                    except Exception as e:
                        logger.debug(f'{self.alias}: Failed to get ticker for {sym}: {e}')

                logger.info(f"{self.alias}: Processed ticks")
                time.sleep(60)
            except Exception as e:
                logger.warning(f'{self.alias}: Failed to process ticks: {e}')
                time.sleep(120)

    def sync_trades(self):
        max_fetches_in_cycle = 3
        first_trade_reached = False
        one_day_ms = 24 * 60 * 60 * 1000
        while True:
            try:
                two_years_ago = int(
                    (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2 * 365 - 1)).timestamp() * 1000)

                counter = 0
                while first_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    oldest_income = self.repository.get_oldest_income(account=self.account.alias)
                    if oldest_income is None:
                        oldest_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                    else:
                        oldest_timestamp = oldest_income.timestamp
                        logger.warning(f'Synced trades before {readable(oldest_timestamp)}')

                    oldest_timestamp = max(oldest_timestamp, two_years_ago)

                    exchange_incomes = self.client.get_income(
                        income_type='REALIZED_PNL',
                        limit=1000,
                        start_time=oldest_timestamp - one_day_ms,
                        end_time=oldest_timestamp - 1
                    )
                    logger.info(
                        f"Length of older trades fetched up to {readable(oldest_timestamp)}: {len(exchange_incomes)}")
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income['asset']):
                            exchange_income['income'] = self.income_to_usdt(
                                float(exchange_income['income']),
                                int(exchange_income['time']),
                                exchange_income['asset'])
                            exchange_income['asset'] = "USDT"

                        income = Income(
                            symbol=exchange_income['symbol'],
                            asset=exchange_income['asset'],
                            type=exchange_income['incomeType'],
                            income=float(exchange_income['income']),
                            timestamp=exchange_income['time'],
                            transaction_id=exchange_income['tranId']
                        )
                        incomes.append(income)

                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(exchange_incomes) < 1:
                        first_trade_reached = True

                newest_trade_reached = False
                while newest_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    newest_income = self.repository.get_newest_income(account=self.account.alias)
                    if newest_income is None:
                        newest_timestamp = int(
                            datetime.datetime.fromisoformat('2020-01-01 00:00:00+00:00').timestamp() * 1000)
                    else:
                        newest_timestamp = newest_income.timestamp
                        logger.warning(f'Synced newer trades since {readable(newest_timestamp)}')

                    newest_timestamp = max(newest_timestamp, two_years_ago)

                    exchange_incomes = self.client.get_income(
                        income_type='REALIZED_PNL',
                        limit=1000,
                        start_time=newest_timestamp + 1
                    )
                    logger.info(
                        f"Length of newer trades fetched from {readable(newest_timestamp)}: {len(exchange_incomes)}")
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income['asset']):
                            exchange_income['income'] = self.income_to_usdt(
                                float(exchange_income['income']),
                                int(exchange_income['time']),
                                exchange_income['asset'])
                            exchange_income['asset'] = "USDT"

                        income = Income(
                            symbol=exchange_income['symbol'],
                            asset=exchange_income['asset'],
                            type=exchange_income['incomeType'],
                            income=float(exchange_income['income']),
                            timestamp=exchange_income['time'],
                            transaction_id=exchange_income['tranId']
                        )
                        incomes.append(income)

                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(exchange_incomes) < 1:
                        newest_trade_reached = True

                logger.warning('Synced trades')
            except Exception as e:
                logger.exception(f'{self.account.alias} Failed to process trades: {e}')

            time.sleep(60)

    def income_to_usdt(self, income: float, income_timestamp: int, asset: str) -> float:
        if is_asset_usd_or_derivative(asset):
            return income

        symbol = f"{asset}USDT"
        candles = self.client.get_klines(
            symbol=symbol,
            interval='1m',
            start_time=int(income_timestamp) - 1000,
            limit=1
        )

        close_price = candles[-1][4]
        income *= float(close_price)
        return income
