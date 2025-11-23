import logging
import threading
import time
import datetime
from typing import List

from blofin import BloFinClient

from scraper_root.scraper.data_classes import AssetBalance, Position, Tick, Balance, Income, Order, \
    Account
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


class BlofinFutures:
    def __init__(self, account: Account, symbols: List[str], repository: Repository):
        logger.info(f"Blofin initializing")
        self.account = account
        self.alias = self.account.alias
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.passphrase = self.account.api_passphrase
        self.repository = repository

        # Blofin connection
        self.client = BloFinClient(
            api_key=self.api_key,
            api_secret=self.secret,
            passphrase=self.passphrase,
            use_server_time=False
        )

        # Verify API connection by fetching account balance
        try:
            test = self.client.account.get_balance(account_type='futures')
            if test and 'data' in test:
                logger.info(f"{self.alias}: REST login successful")
            else:
                logger.error(f"{self.alias}: Failed to login - unexpected response")
                raise SystemExit()
        except Exception as e:
            logger.error(f"{self.alias}: Failed to login: {e}")
            raise SystemExit()

        # Active symbols with positions
        self.activesymbols = []

    def start(self):
        logger.info(f'{self.alias}: Starting Blofin Futures scraper')

        for symbol in self.symbols:
            symbol_trade_thread = threading.Thread(name=f'trade_thread_{symbol}', target=self.sync_current_price,
                                                   args=(symbol,), daemon=True)
            symbol_trade_thread.start()

        sync_balance_thread = threading.Thread(name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        sync_positions_thread = threading.Thread(name=f'sync_positions_thread', target=self.sync_positions, daemon=True)
        sync_positions_thread.start()

        sync_trades_thread = threading.Thread(name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

        sync_orders_thread = threading.Thread(name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
        sync_orders_thread.start()

    def sync_account(self):
        while True:
            try:
                account = self.client.account.get_balance(account_type='futures')
                if account and 'data' in account:
                    assets_data = account['data']
                    asset_balances = []
                    total_balance = 0.0
                    total_upnl = 0.0

                    for asset in assets_data:
                        currency = asset.get('currency', 'USDT')
                        balance_val = float(asset.get('balance', 0))
                        available = float(asset.get('available', 0))

                        if currency == 'USDT':
                            total_balance = balance_val
                        else:
                            asset_balances.append(AssetBalance(
                                asset=currency,
                                balance=balance_val,
                                unrealizedProfit=0.0
                            ))

                    # Try to get unrealized PnL from positions
                    try:
                        positions_resp = self.client.trading.get_positions()
                        if positions_resp and 'data' in positions_resp:
                            for pos in positions_resp['data']:
                                upnl = float(pos.get('unrealizedPnl', 0))
                                total_upnl += upnl
                    except Exception:
                        pass

                    balance = Balance(
                        totalBalance=total_balance,
                        totalUnrealizedProfit=total_upnl,
                        assets=asset_balances
                    )
                    self.repository.process_balances(balance=balance, account=self.alias)
                    logger.warning(f'{self.alias}: Synced balance')
                time.sleep(100)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process balance: {e}')
                time.sleep(360)

    def sync_positions(self):
        while True:
            try:
                self.activesymbols = ["BTC-USDT"]
                positions = []

                exchange_positions = self.client.trading.get_positions()
                if exchange_positions and 'data' in exchange_positions:
                    for pos in exchange_positions['data']:
                        # Only include positions with actual size
                        quantity = float(pos.get('positions', 0))
                        if quantity != 0:
                            inst_id = pos.get('instId', '')
                            self.activesymbols.append(inst_id)

                            # Determine side from positionSide field
                            pos_side = pos.get('positionSide', 'net')
                            if pos_side == 'long':
                                side = "LONG"
                            elif pos_side == 'short':
                                side = "SHORT"
                            else:
                                # net mode - determine by quantity sign or side field
                                side = "LONG" if quantity > 0 else "SHORT"

                            positions.append(Position(
                                symbol=inst_id.replace('-', ''),  # BTC-USDT -> BTCUSDT for consistency
                                entry_price=float(pos.get('averagePrice', 0)),
                                position_size=abs(quantity),
                                side=side,
                                unrealizedProfit=float(pos.get('unrealizedPnl', 0)),
                                initial_margin=float(pos.get('initialMargin', 0)),
                                market_price=float(pos.get('markPrice', 0))
                            ))

                self.repository.process_positions(positions=positions, account=self.alias)
                logger.warning(f'{self.alias}: Synced positions')
                time.sleep(250)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process positions: {e}')
                time.sleep(360)

    def sync_open_orders(self):
        while True:
            orders = []
            try:
                open_orders = self.client.trading.get_active_orders()
                if open_orders and 'data' in open_orders:
                    for item in open_orders['data']:
                        order = Order()
                        inst_id = item.get('instId', '')
                        order.symbol = inst_id.replace('-', '')  # BTC-USDT -> BTCUSDT
                        order.price = float(item.get('price', 0))
                        order.quantity = float(item.get('size', 0))

                        # Side: buy/sell
                        side = item.get('side', '').upper()
                        order.side = side if side in ['BUY', 'SELL'] else side

                        # Position side
                        pos_side = item.get('positionSide', 'net')
                        if pos_side == 'long':
                            order.position_side = "LONG"
                        elif pos_side == 'short':
                            order.position_side = "SHORT"
                        else:
                            # Infer from side for net mode
                            order.position_side = "LONG" if side == 'BUY' else "SHORT"

                        order.type = item.get('orderType', 'limit').upper()
                        orders.append(order)

                logger.warning(f'{self.alias}: Synced orders')
                self.repository.process_orders(orders=orders, account=self.alias)
            except Exception as e:
                logger.warning(f'{self.alias}: Failed to process orders: {e}')
            time.sleep(120)

    def sync_current_price(self, symbol: str):
        while True:
            try:
                # Sync prices for active symbols
                symbols_to_fetch = list(set(self.activesymbols + [self._to_blofin_symbol(symbol)]))
                for sym in symbols_to_fetch:
                    try:
                        ticker = self.client.public.get_tickers(inst_id=sym)
                        if ticker and 'data' in ticker and len(ticker['data']) > 0:
                            tick_data = ticker['data'][0]
                            tick = Tick(
                                symbol=sym.replace('-', ''),  # BTC-USDT -> BTCUSDT
                                price=float(tick_data.get('last', 0)),
                                qty=float(tick_data.get('lastSize', 0)),
                                timestamp=int(tick_data.get('ts', int(time.time() * 1000)))
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
        """Sync trade/income history (closed PnL)"""
        last_synced_id = None

        while True:
            try:
                # Fetch order history to get closed trades
                params = {'limit': '100'}
                if last_synced_id:
                    params['after'] = last_synced_id

                exchange_orders = self.client.trading.get_order_history(**params)

                if exchange_orders and 'data' in exchange_orders:
                    incomes = []
                    for trade in exchange_orders['data']:
                        # Only process filled orders
                        state = trade.get('state', '')
                        if state != 'filled':
                            continue

                        pnl = float(trade.get('pnl', 0))
                        fee = float(trade.get('fee', 0))

                        # Only record if there's actual realized PnL
                        if pnl != 0:
                            inst_id = trade.get('instId', '')
                            income = Income(
                                symbol=inst_id.replace('-', ''),  # BTC-USDT -> BTCUSDT
                                asset='USDT',
                                type='REALIZED_PNL',
                                income=pnl + fee,  # Include fees in the income
                                timestamp=int(trade.get('updateTime', int(time.time() * 1000))),
                                transaction_id=trade.get('orderId', '')
                            )
                            incomes.append(income)

                        # Track last ID for pagination
                        order_id = trade.get('orderId')
                        if order_id:
                            last_synced_id = order_id

                    if incomes:
                        self.repository.process_incomes(incomes=incomes, account=self.alias)

                logger.info(f'{self.alias}: Synced trades')
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process trades: {e}')

            time.sleep(120)

    def _to_blofin_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT format to BTC-USDT format used by Blofin"""
        # Handle common USDT pairs
        if symbol.endswith('USDT'):
            base = symbol[:-4]
            return f"{base}-USDT"
        elif symbol.endswith('USDC'):
            base = symbol[:-4]
            return f"{base}-USDC"
        # If already in correct format or unknown, return as-is
        return symbol
