"""
Equity Trade Generator for FIX Protocol-based Trades

This module generates synthetic equity trade data with:
- FIX protocol standard fields (SIDE, ORDER_TYPE, EXEC_TYPE, etc.)
- Multi-currency support with CHF as base currency
- Global market coverage (NYSE, LSE, XETRA, etc.)
- Realistic pricing and commission structures
- Business day constraints
"""

import csv
import random
import uuid
from dataclasses import dataclass, asdict, fields
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
from pathlib import Path

from base_generator import init_random_seed


@dataclass
class EquityTrade:
    """Represents a single equity trade with FIX protocol fields"""
    trade_date: str
    settlement_date: str
    trade_id: str
    customer_id: str
    account_id: str
    order_id: str
    exec_id: str
    symbol: str
    isin: str
    side: str  # 1=Buy, 2=Sell (FIX standard)
    quantity: float
    price: float
    currency: str
    gross_amount: float  # Signed: positive for buys, negative for sells
    commission: float
    net_amount: float  # Signed: gross_amount - commission (buys), gross_amount + commission (sells)
    base_currency: str
    base_gross_amount: float  # Signed amount in CHF
    base_net_amount: float  # Signed amount in CHF
    fx_rate: float
    market: str  # Exchange/Market
    order_type: str  # Market, Limit, Stop, etc.
    exec_type: str  # New, PartialFill, Fill, etc.
    time_in_force: str  # DAY, GTC, IOC, etc.
    broker_id: str
    venue: str  # Trading venue


class EquityTradeGenerator:
    """Generates synthetic equity trade data for banking simulation"""
    
    def __init__(self, trading_customers: List, investment_accounts: List, fx_rates: Dict[str, float], seed: int = 42):
        # Initialize random state with seed for reproducibility
        self.fake = init_random_seed(seed)
        self.trading_customers = trading_customers
        self.investment_accounts = investment_accounts
        self.fx_rates = fx_rates
        self.base_currency = "CHF"
        
        # Create mapping from customer_id to their investment accounts
        self.customer_investments = {}
        for account in investment_accounts:
            if account.customer_id not in self.customer_investments:
                self.customer_investments[account.customer_id] = []
            self.customer_investments[account.customer_id].append(account)
        
        # Classify trading customers: 10% are high-volume traders (10-30 trades/month)
        self.high_volume_traders = random.sample(
            self.trading_customers,
            max(1, int(len(self.trading_customers) * 0.1))  # At least 1 if we have trading customers
        )
        
        # FIX protocol constants
        self.fix_sides = ["1", "2"]  # 1=Buy, 2=Sell
        self.order_types = ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
        self.exec_types = ["NEW", "PARTIAL_FILL", "FILL", "CANCELED", "REPLACED"]
        self.time_in_force = ["DAY", "GTC", "IOC", "FOK"]
        
        # Global market definitions
        self.markets = {
            "NYSE": {"currency": "USD", "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V"]},
            "NASDAQ": {"currency": "USD", "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "INTC", "CSCO", "ADBE"]},
            "LSE": {"currency": "GBP", "symbols": ["LLOY", "BP", "SHEL", "AZN", "HSBA", "VOD", "GSK", "BT", "BARC", "RIO"]},
            "XETRA": {"currency": "EUR", "symbols": ["SAP", "SIE", "DTE", "ALV", "BAS", "BMW", "DAI", "VOW", "MUV2", "IFX"]},
            "SIX": {"currency": "CHF", "symbols": ["NESN", "ROG", "NOVN", "UHR", "UBSG", "CS", "ABB", "ZURN", "GIVN", "CFR"]},
            "TSE": {"currency": "JPY", "symbols": ["7203", "6758", "9984", "9433", "8306", "6861", "4063", "8316", "7267", "4502"]}
        }
        
        # ISIN patterns by market
        self.isin_patterns = {
            "NYSE": "US{}{:08d}",
            "NASDAQ": "US{}{:08d}",
            "LSE": "GB{}{:08d}",
            "XETRA": "DE{}{:08d}",
            "SIX": "CH{}{:08d}",
            "TSE": "JP{}{:08d}"
        }
        
        # Commission rates by market (basis points)
        self.commission_rates = {
            "NYSE": 0.005,  # 0.5%
            "NASDAQ": 0.005,
            "LSE": 0.006,
            "XETRA": 0.004,
            "SIX": 0.008,
            "TSE": 0.006
        }
        
        # Brokers
        self.brokers = ["UBS", "CS", "JPM", "GS", "MS", "DB", "BAML", "CITI", "WF", "TD"]
        
        # Venues
        self.venues = ["BATS", "CHI-X", "DARK", "BLOCK", "SIP", "CROSS"]

    def _is_business_day(self, check_date: date) -> bool:
        """Check if a date is a business day (Mon-Fri)"""
        return check_date.weekday() < 5

    def _get_business_date(self, target_date: date) -> date:
        """Get the nearest business day (forward)"""
        while not self._is_business_day(target_date):
            target_date += timedelta(days=1)
        return target_date

    def _calculate_settlement_date(self, trade_date: date) -> date:
        """Calculate settlement date (T+2 business days)"""
        settlement = trade_date
        business_days_added = 0
        
        while business_days_added < 2:
            settlement += timedelta(days=1)
            if self._is_business_day(settlement):
                business_days_added += 1
                
        return settlement

    def _generate_single_trade(self, customer, trade_date: datetime) -> EquityTrade:
        """Generate a single equity trade"""
        
        # Select investment account for this customer
        customer_investments = self.customer_investments.get(customer.customer_id, [])
        if not customer_investments:
            raise ValueError(f"No investment accounts found for customer {customer.customer_id}")
        
        investment_account = random.choice(customer_investments)
        
        # Select market and symbol
        market = random.choice(list(self.markets.keys()))
        market_info = self.markets[market]
        symbol = random.choice(market_info["symbols"])
        currency = market_info["currency"]
        
        # Generate ISIN
        pattern = self.isin_patterns[market]
        check_digits = random.randint(10, 99)
        security_number = random.randint(10000000, 99999999)
        isin = pattern.format(check_digits, security_number)
        
        # Trade details
        side = random.choice(self.fix_sides)
        is_buy = side == "1"
        
        quantity = round(random.uniform(10, 1000), 4)
        
        # Price generation based on market
        if market in ["NYSE", "NASDAQ"]:
            price = round(random.uniform(50, 300), 2)
        elif market == "LSE":
            price = round(random.uniform(100, 2000), 2)  # Pence
        elif market == "XETRA":
            price = round(random.uniform(20, 150), 2)
        elif market == "SIX":
            price = round(random.uniform(50, 1000), 2)
        else:  # TSE
            price = round(random.uniform(1000, 5000), 0)
        
        # Calculate amounts
        gross_amount = quantity * price
        if not is_buy:
            gross_amount = -gross_amount  # Negative for sells
            
        commission = abs(gross_amount) * self.commission_rates[market]
        
        # Net amount calculation (commission reduces proceeds for both buys and sells)
        if is_buy:
            net_amount = gross_amount - commission  # More negative (cost more)
        else:
            net_amount = gross_amount + commission  # Less negative (receive more)
        
        # FX conversion to CHF
        fx_rate = self.fx_rates.get(currency, 1.0)
        base_gross_amount = gross_amount * fx_rate
        base_net_amount = net_amount * fx_rate
        
        # Use investment account for settlements
        account_id = investment_account.account_id
        
        # Generate IDs
        trade_id = f"TRD_{uuid.uuid4().hex[:12].upper()}"
        order_id = f"ORD_{uuid.uuid4().hex[:12].upper()}"
        exec_id = f"EXE_{uuid.uuid4().hex[:12].upper()}"
        
        # Settlement date
        settlement_date = self._calculate_settlement_date(trade_date.date())
        
        return EquityTrade(
            trade_date=trade_date.isoformat() + "Z",
            settlement_date=settlement_date.isoformat(),
            trade_id=trade_id,
            customer_id=customer.customer_id,
            account_id=investment_account.account_id,
            order_id=order_id,
            exec_id=exec_id,
            symbol=symbol,
            isin=isin,
            side=side,
            quantity=quantity,
            price=price,
            currency=currency,
            gross_amount=round(gross_amount, 2),
            commission=round(commission, 4),
            net_amount=round(net_amount, 2),
            base_currency=self.base_currency,
            base_gross_amount=round(base_gross_amount, 2),
            base_net_amount=round(base_net_amount, 2),
            fx_rate=round(fx_rate, 6),
            market=market,
            order_type=random.choice(self.order_types),
            exec_type=random.choice(self.exec_types),
            time_in_force=random.choice(self.time_in_force),
            broker_id=random.choice(self.brokers),
            venue=random.choice(self.venues)
        )

    def generate_daily_trades(self, target_date: datetime) -> List[EquityTrade]:
        """Generate trades for a specific business day"""
        
        # Check if it's a business day
        if not self._is_business_day(target_date.date()):
            return []
        
        trades = []
        
        # Generate trades for trading customers
        for customer in self.trading_customers:
            # Determine trading volume based on customer type
            if customer in self.high_volume_traders:
                # High-volume traders: aim for 10-30 trades/month = 0.5-1.5 trades/day on average
                # But with bursts of activity (some days 0, some days 3-8 trades)
                num_trades = random.choices([0, 1, 2, 3, 4, 5, 6, 7, 8], weights=[20, 15, 15, 15, 12, 10, 8, 3, 2])[0]
            else:
                # Regular traders: 0-5 trades per day (original distribution)
                num_trades = random.choices([0, 1, 2, 3, 4, 5], weights=[15, 25, 30, 20, 8, 2])[0]
            
            for _ in range(num_trades):
                # Random trade time during trading hours (09:00 - 17:00)
                trade_time = target_date.replace(
                    hour=random.randint(9, 16),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                    microsecond=random.randint(0, 999999)
                )
                
                trade = self._generate_single_trade(customer, trade_time)
                trades.append(trade)
        
        return trades

    def save_daily_trades_to_csv(self, trades: List[EquityTrade], output_dir: Path, target_date: datetime):
        """Save trades to CSV file"""
        if not trades:
            return
            
        filename = f"trades_{target_date.strftime('%Y-%m-%d')}.csv"
        filepath = output_dir / filename
        
        # Get field names from dataclass
        fieldnames = [field.name for field in fields(EquityTrade)]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for trade in trades:
                writer.writerow(asdict(trade))
        
        print(f"Generated {len(trades)} trades for {target_date.strftime('%Y-%m-%d')} -> {filename}")

    def generate_period_data(self, start_date: datetime, end_date: datetime, output_dir: Path) -> Dict:
        """Generate equity trade data for a date range"""
        
        output_dir.mkdir(exist_ok=True)
        
        current_date = start_date
        total_trades = 0
        trading_days = 0
        
        summary = {
            "total_trades": 0,
            "trading_days": 0,
            "trading_customers": len(self.trading_customers),
            "high_volume_traders": len(self.high_volume_traders),
            "markets": list(self.markets.keys()),
            "base_currency": self.base_currency
        }
        
        while current_date <= end_date:
            trades = self.generate_daily_trades(current_date)
            
            if trades:
                self.save_daily_trades_to_csv(trades, output_dir, current_date)
                total_trades += len(trades)
                trading_days += 1
            
            current_date += timedelta(days=1)
        
        summary["total_trades"] = total_trades
        summary["trading_days"] = trading_days
        
        return summary
