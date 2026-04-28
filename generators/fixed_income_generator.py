"""
Fixed Income Trade Generator for Bonds and Interest Rate Swaps

This module generates synthetic fixed income trade data with:
- Government and Corporate Bonds
- Interest Rate Swaps (IRS)
- Multi-currency support with CHF as base currency
- Realistic yield curves and credit spreads
- Settlement conventions (T+1, T+2, T+3)
- Duration and DV01 calculations for FRTB
"""

import csv
import random
import uuid
from dataclasses import dataclass, asdict, fields
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any
from pathlib import Path

from faker import Faker
from base_generator import BaseGenerator


@dataclass
class FixedIncomeTrade:
    """Represents a single fixed income trade (Bond or Swap)"""
    trade_date: str
    settlement_date: str
    trade_id: str
    customer_id: str
    account_id: str
    order_id: str
    instrument_type: str  # BOND, IRS (Interest Rate Swap)
    instrument_id: str  # ISIN for bonds, swap ID for swaps
    issuer: str  # Government, Corporate name
    issuer_type: str  # SOVEREIGN, CORPORATE, SUPRANATIONAL
    currency: str
    side: str  # 1=Buy/Pay, 2=Sell/Receive (FIX standard)
    
    # Bond-specific fields
    notional: float  # Face value
    price: float  # Clean price (as % of par, e.g., 98.50)
    accrued_interest: float
    gross_amount: float  # Signed: price * notional + accrued
    
    # Swap-specific fields
    fixed_rate: Optional[float]  # For IRS
    floating_rate_index: Optional[str]  # LIBOR, EURIBOR, SOFR, SARON
    tenor_years: float  # Maturity in years
    
    # Common fields
    commission: float
    net_amount: float  # Signed: gross_amount +/- commission
    base_currency: str  # CHF
    base_gross_amount: float  # Signed amount in CHF
    base_net_amount: float  # Signed amount in CHF
    fx_rate: float
    
    # Risk metrics for FRTB
    coupon_rate: float  # Annual coupon rate (%)
    maturity_date: str
    duration: float  # Modified duration (years)
    dv01: float  # Dollar value of 1bp move (in base currency)
    credit_rating: str  # AAA, AA, A, BBB, BB, B, CCC
    credit_spread_bps: float  # Spread over risk-free rate in basis points
    
    # Market data
    market: str  # Trading venue
    broker_id: str
    venue: str
    liquidity_score: float  # 1-10, for NMRF classification
    
    # Metadata
    created_at: str


class FixedIncomeTradeGenerator(BaseGenerator):
    """Generator for synthetic fixed income trades"""
    
    # Government bond issuers by currency
    SOVEREIGN_ISSUERS = {
        'CHF': ['Swiss Confederation', 'Canton Zurich', 'Canton Geneva'],
        'EUR': ['Germany', 'France', 'Netherlands', 'Austria', 'Belgium'],
        'USD': ['US Treasury', 'US Agency'],
        'GBP': ['UK Gilt', 'UK DMO'],
    }
    
    # Corporate bond issuers
    CORPORATE_ISSUERS = [
        'Nestle SA', 'Novartis AG', 'Roche Holding AG', 'UBS Group AG',
        'Credit Suisse Group AG', 'ABB Ltd', 'Zurich Insurance Group AG',
        'Swiss Re AG', 'LafargeHolcim Ltd', 'Sika AG',
        'Siemens AG', 'Volkswagen AG', 'BMW AG', 'Allianz SE',
        'Deutsche Bank AG', 'BNP Paribas SA', 'Total SA', 'Shell plc',
        'HSBC Holdings plc', 'Barclays plc', 'Apple Inc', 'Microsoft Corp',
        'JPMorgan Chase & Co', 'Bank of America Corp'
    ]
    
    # Credit ratings with typical spreads (bps over risk-free)
    CREDIT_RATINGS = {
        'AAA': (5, 15),
        'AA': (15, 30),
        'A': (30, 60),
        'BBB': (60, 120),
        'BB': (120, 250),
        'B': (250, 500),
        'CCC': (500, 1000),
    }
    
    # Floating rate indices by currency
    FLOATING_INDICES = {
        'CHF': 'SARON',
        'EUR': 'EURIBOR',
        'USD': 'SOFR',
        'GBP': 'SONIA',
    }
    
    # Markets/Exchanges
    MARKETS = {
        'CHF': ['SIX Swiss Exchange', 'OTC'],
        'EUR': ['Eurex', 'OTC', 'Xetra'],
        'USD': ['NYSE', 'OTC'],
        'GBP': ['LSE', 'OTC'],
    }
    
    def __init__(self, config, customers: List[str], accounts: List[Dict], 
                 fx_rates: Dict[str, float], start_date: date, end_date: date):
        """
        Initialize the generator
        
        Args:
            config: GeneratorConfig instance
            customers: List of customer IDs
            accounts: List of account dictionaries with account_id, customer_id, base_currency
            fx_rates: Dictionary of FX rates to CHF
            start_date: Start date for trade generation
            end_date: End date for trade generation
        """
        super().__init__(config)
        self.customers = customers
        self.accounts = accounts
        self.fx_rates = fx_rates
        self.start_date = start_date
        self.end_date = end_date
        # self.fake is already initialized by BaseGenerator._init_random_state()
        
        # Build account lookup
        self.customer_accounts = {}
        for account in accounts:
            cust_id = account['customer_id']
            if cust_id not in self.customer_accounts:
                self.customer_accounts[cust_id] = []
            self.customer_accounts[cust_id].append(account)
    
    def _is_business_day(self, check_date: date) -> bool:
        """Check if date is a business day (Mon-Fri)"""
        return check_date.weekday() < 5
    
    def _next_business_day(self, from_date: date, days: int = 1) -> date:
        """Get next business day, skipping weekends"""
        current = from_date
        while days > 0:
            current += timedelta(days=1)
            if self._is_business_day(current):
                days -= 1
        return current
    
    def _generate_isin(self, country_code: str, instrument_type: str) -> str:
        """Generate a realistic ISIN code"""
        # ISIN format: 2-char country + 9-char identifier + 1 check digit
        identifier = ''.join([str(random.randint(0, 9)) for _ in range(9)])
        return f"{country_code}{identifier}{random.randint(0, 9)}"
    
    def _calculate_accrued_interest(self, notional: float, coupon_rate: float, 
                                   days_since_coupon: int) -> float:
        """Calculate accrued interest"""
        annual_coupon = notional * (coupon_rate / 100)
        daily_accrual = annual_coupon / 365
        return daily_accrual * days_since_coupon
    
    def _calculate_duration(self, years_to_maturity: float, coupon_rate: float, 
                           yield_rate: float) -> float:
        """Calculate modified duration (simplified)"""
        # Simplified Macaulay duration approximation
        if coupon_rate == 0:  # Zero coupon bond
            return years_to_maturity
        else:
            # Approximation for coupon bonds
            macaulay = (1 + yield_rate/100) / (yield_rate/100) - \
                      (1 + yield_rate/100 + years_to_maturity * (coupon_rate/100 - yield_rate/100)) / \
                      (coupon_rate/100 * ((1 + yield_rate/100)**years_to_maturity - 1) + yield_rate/100)
            modified = macaulay / (1 + yield_rate/100)
            return max(0.1, modified)  # Ensure positive
    
    def _calculate_dv01(self, notional: float, duration: float, price: float) -> float:
        """Calculate DV01 (dollar value of 1 basis point move)"""
        # DV01 = Modified Duration × Price × Notional × 0.0001
        return duration * (price / 100) * notional * 0.0001
    
    def generate_bond_trade(self, customer_id: str, account: Dict, 
                           trade_date: date) -> FixedIncomeTrade:
        """Generate a single bond trade"""
        
        currency = random.choice(['CHF', 'EUR', 'USD', 'GBP'])
        fx_rate = self.fx_rates.get(currency, 1.0)
        
        # Determine issuer type and issuer
        issuer_type = random.choices(
            ['SOVEREIGN', 'CORPORATE', 'SUPRANATIONAL'],
            weights=[0.4, 0.5, 0.1]
        )[0]
        
        if issuer_type == 'SOVEREIGN':
            issuer = random.choice(self.SOVEREIGN_ISSUERS.get(currency, ['Government']))
            credit_rating = random.choices(
                ['AAA', 'AA', 'A'],
                weights=[0.6, 0.3, 0.1]
            )[0]
        elif issuer_type == 'SUPRANATIONAL':
            issuer = random.choice(['European Investment Bank', 'World Bank', 
                                   'International Finance Corporation'])
            credit_rating = 'AAA'
        else:  # CORPORATE
            issuer = random.choice(self.CORPORATE_ISSUERS)
            credit_rating = random.choices(
                ['AAA', 'AA', 'A', 'BBB', 'BB', 'B'],
                weights=[0.05, 0.15, 0.30, 0.30, 0.15, 0.05]
            )[0]
        
        # Generate bond characteristics
        tenor_years = random.choice([1, 2, 3, 5, 7, 10, 15, 20, 30])
        maturity_date = trade_date + timedelta(days=int(tenor_years * 365))
        
        coupon_rate = round(random.uniform(0.5, 5.0), 3)  # Annual coupon %
        
        # Credit spread based on rating
        spread_range = self.CREDIT_RATINGS[credit_rating]
        credit_spread_bps = round(random.uniform(*spread_range), 1)
        
        # Yield = risk-free rate + credit spread
        risk_free_rate = 2.5  # Simplified
        yield_rate = risk_free_rate + (credit_spread_bps / 100)
        
        # Price (as % of par) - bonds trade around par, adjusted for yield
        price_deviation = random.uniform(-5, 5)
        price = round(100 + price_deviation, 2)
        
        # Notional amount
        notional = random.choice([10000, 25000, 50000, 100000, 250000, 500000, 1000000])
        
        # Accrued interest (random days since last coupon)
        days_since_coupon = random.randint(0, 180)
        accrued_interest = self._calculate_accrued_interest(notional, coupon_rate, days_since_coupon)
        
        # Side (Buy or Sell)
        side = random.choice(['1', '2'])  # 1=Buy, 2=Sell
        
        # Gross amount (clean price + accrued interest)
        gross_amount = (price / 100) * notional + accrued_interest
        if side == '2':  # Sell
            gross_amount = -gross_amount
        
        # Commission (10-30 bps of notional)
        commission = notional * random.uniform(0.0010, 0.0030)
        
        # Net amount
        if side == '1':  # Buy
            net_amount = gross_amount + commission
        else:  # Sell
            net_amount = gross_amount - commission
        
        # Convert to base currency (CHF)
        base_gross_amount = gross_amount * fx_rate
        base_net_amount = net_amount * fx_rate
        
        # Calculate risk metrics
        duration = self._calculate_duration(tenor_years, coupon_rate, yield_rate)
        dv01 = self._calculate_dv01(notional, duration, price) * fx_rate  # In CHF
        
        # Settlement date (T+1 for bonds in most markets)
        settlement_date = self._next_business_day(trade_date, days=1)
        
        # ISIN
        country_code = {'CHF': 'CH', 'EUR': 'DE', 'USD': 'US', 'GBP': 'GB'}.get(currency, 'CH')
        isin = self._generate_isin(country_code, 'BOND')
        
        # Market
        market = random.choice(self.MARKETS.get(currency, ['OTC']))
        
        # Liquidity score (1-10, sovereigns more liquid)
        if issuer_type == 'SOVEREIGN':
            liquidity_score = random.uniform(7, 10)
        elif issuer_type == 'SUPRANATIONAL':
            liquidity_score = random.uniform(6, 9)
        else:
            liquidity_score = random.uniform(3, 8)
        
        return FixedIncomeTrade(
            trade_date=trade_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            settlement_date=settlement_date.strftime('%Y-%m-%d'),
            trade_id=f"FI_{uuid.uuid4().hex[:12].upper()}",
            customer_id=customer_id,
            account_id=account['account_id'],
            order_id=f"ORD_{uuid.uuid4().hex[:8].upper()}",
            instrument_type='BOND',
            instrument_id=isin,
            issuer=issuer,
            issuer_type=issuer_type,
            currency=currency,
            side=side,
            notional=notional,
            price=price,
            accrued_interest=round(accrued_interest, 2),
            gross_amount=round(gross_amount, 2),
            fixed_rate=None,
            floating_rate_index=None,
            tenor_years=tenor_years,
            commission=round(commission, 2),
            net_amount=round(net_amount, 2),
            base_currency='CHF',
            base_gross_amount=round(base_gross_amount, 2),
            base_net_amount=round(base_net_amount, 2),
            fx_rate=round(fx_rate, 6),
            coupon_rate=coupon_rate,
            maturity_date=maturity_date.strftime('%Y-%m-%d'),
            duration=round(duration, 4),
            dv01=round(dv01, 2),
            credit_rating=credit_rating,
            credit_spread_bps=credit_spread_bps,
            market=market,
            broker_id=f"BRK_{random.randint(100, 999)}",
            venue=market,
            liquidity_score=round(liquidity_score, 2),
            created_at=self.get_utc_timestamp()
        )
    
    def generate_swap_trade(self, customer_id: str, account: Dict, 
                           trade_date: date) -> FixedIncomeTrade:
        """Generate a single interest rate swap trade"""
        
        currency = random.choice(['CHF', 'EUR', 'USD', 'GBP'])
        fx_rate = self.fx_rates.get(currency, 1.0)
        
        # Swap characteristics
        tenor_years = random.choice([1, 2, 3, 5, 7, 10])
        maturity_date = trade_date + timedelta(days=int(tenor_years * 365))
        
        # Fixed rate (swap rate)
        fixed_rate = round(random.uniform(1.0, 4.5), 3)
        
        # Floating rate index
        floating_rate_index = self.FLOATING_INDICES.get(currency, 'LIBOR')
        
        # Notional amount (swaps typically larger)
        notional = random.choice([500000, 1000000, 2500000, 5000000, 10000000])
        
        # Side: 1=Pay Fixed/Receive Floating, 2=Receive Fixed/Pay Floating
        side = random.choice(['1', '2'])
        
        # Swap NPV at inception (typically near zero, small variation)
        npv = notional * random.uniform(-0.002, 0.002)
        gross_amount = npv
        if side == '2':
            gross_amount = -gross_amount
        
        # Commission (smaller for swaps, 1-5 bps)
        commission = notional * random.uniform(0.0001, 0.0005)
        
        # Net amount
        if side == '1':
            net_amount = gross_amount + commission
        else:
            net_amount = gross_amount - commission
        
        # Convert to base currency
        base_gross_amount = gross_amount * fx_rate
        base_net_amount = net_amount * fx_rate
        
        # Calculate risk metrics
        # For swaps, duration approximates to tenor/2
        duration = tenor_years / 2.0
        dv01 = self._calculate_dv01(notional, duration, 100) * fx_rate
        
        # Settlement (T+2 for swaps)
        settlement_date = self._next_business_day(trade_date, days=2)
        
        # Swap ID
        swap_id = f"IRS_{currency}_{uuid.uuid4().hex[:8].upper()}"
        
        return FixedIncomeTrade(
            trade_date=trade_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            settlement_date=settlement_date.strftime('%Y-%m-%d'),
            trade_id=f"FI_{uuid.uuid4().hex[:12].upper()}",
            customer_id=customer_id,
            account_id=account['account_id'],
            order_id=f"ORD_{uuid.uuid4().hex[:8].upper()}",
            instrument_type='IRS',
            instrument_id=swap_id,
            issuer='N/A',
            issuer_type='DERIVATIVE',
            currency=currency,
            side=side,
            notional=notional,
            price=100.0,  # Swaps don't have price
            accrued_interest=0.0,
            gross_amount=round(gross_amount, 2),
            fixed_rate=fixed_rate,
            floating_rate_index=floating_rate_index,
            tenor_years=tenor_years,
            commission=round(commission, 2),
            net_amount=round(net_amount, 2),
            base_currency='CHF',
            base_gross_amount=round(base_gross_amount, 2),
            base_net_amount=round(base_net_amount, 2),
            fx_rate=round(fx_rate, 6),
            coupon_rate=0.0,  # N/A for swaps
            maturity_date=maturity_date.strftime('%Y-%m-%d'),
            duration=round(duration, 4),
            dv01=round(dv01, 2),
            credit_rating='N/A',
            credit_spread_bps=0.0,
            market='OTC',
            broker_id=f"BRK_{random.randint(100, 999)}",
            venue='OTC',
            liquidity_score=round(random.uniform(5, 8), 2),
            created_at=self.get_utc_timestamp()
        )
    
    def generate_trades(self, num_trades: int = 1000, 
                       bond_swap_ratio: float = 0.7) -> List[FixedIncomeTrade]:
        """
        Generate multiple fixed income trades
        
        Args:
            num_trades: Number of trades to generate
            bond_swap_ratio: Ratio of bonds to swaps (0.7 = 70% bonds, 30% swaps)
        
        Returns:
            List of FixedIncomeTrade objects
        """
        trades = []
        
        num_bonds = int(num_trades * bond_swap_ratio)
        num_swaps = num_trades - num_bonds
        
        print(f"Generating {num_bonds} bond trades and {num_swaps} swap trades...")
        
        for i in range(num_trades):
            # Random customer and account
            customer_id = random.choice(self.customers)
            if customer_id not in self.customer_accounts:
                continue
            
            account = random.choice(self.customer_accounts[customer_id])
            
            # Random trade date
            days_range = (self.end_date - self.start_date).days
            trade_date = self.start_date + timedelta(days=random.randint(0, days_range))
            
            # Ensure business day
            while not self._is_business_day(trade_date):
                trade_date += timedelta(days=1)
            
            # Add random time
            trade_date = datetime.combine(trade_date, 
                                         datetime.min.time().replace(
                                             hour=random.randint(9, 16),
                                             minute=random.randint(0, 59),
                                             second=random.randint(0, 59)))
            
            # Generate bond or swap
            if i < num_bonds:
                trade = self.generate_bond_trade(customer_id, account, trade_date)
            else:
                trade = self.generate_swap_trade(customer_id, account, trade_date)
            
            trades.append(trade)
            
            if (i + 1) % 100 == 0:
                print(f"  Generated {i + 1}/{num_trades} trades...")
        
        print(f"✓ Generated {len(trades)} fixed income trades")
        return trades
    
    def save_to_csv(self, trades: List[FixedIncomeTrade], output_path: Path):
        """Save trades to CSV file"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            if not trades:
                return
            
            # Get field names from dataclass
            field_names = [field.name for field in fields(FixedIncomeTrade)]
            writer = csv.DictWriter(f, fieldnames=field_names)
            
            writer.writeheader()
            for trade in trades:
                writer.writerow(asdict(trade))
        
        print(f"✓ Saved {len(trades)} trades to {output_path}")
    
    def save_to_csv_by_date(self, trades: List[FixedIncomeTrade], output_dir: Path):
        """
        Save trades to separate CSV files grouped by trade date
        
        Args:
            trades: List of FixedIncomeTrade objects
            output_dir: Directory where date-specific CSV files will be saved
        """
        from collections import defaultdict
        
        if not trades:
            print("No trades to save")
            return []
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Group trades by date
        trades_by_date = defaultdict(list)
        for trade in trades:
            # Extract date from timestamp (format: 'YYYY-MM-DD HH:MM:SS')
            trade_date = trade.trade_date.split(' ')[0]  # Get 'YYYY-MM-DD' part
            trades_by_date[trade_date].append(trade)
        
        # Get field names from dataclass
        field_names = [field.name for field in fields(FixedIncomeTrade)]
        
        # Save each date to a separate file
        files_created = []
        for trade_date, date_trades in sorted(trades_by_date.items()):
            output_file = output_dir / f'fixed_income_trades_{trade_date}.csv'
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()
                for trade in date_trades:
                    writer.writerow(asdict(trade))
            
            files_created.append((trade_date, len(date_trades), output_file))
            print(f"  ✓ {output_file.name}: {len(date_trades)} trades")
        
        print(f"\n✓ Saved {len(trades)} trades across {len(files_created)} files in {output_dir}")
        return files_created
    
    def generate(self) -> Dict[str, Any]:
        """Generate fixed income trades - implementation of abstract method"""
        trades = self.generate_trades()
        return {
            'trades': trades,
            'total_trades': len(trades),
            'bonds': len([t for t in trades if t.instrument_type == 'BOND']),
            'swaps': len([t for t in trades if t.instrument_type == 'IRS'])
        }


if __name__ == "__main__":
    # Example usage
    from datetime import date
    
    # Mock data
    customers = [f"CUST_{str(i).zfill(5)}" for i in range(1, 11)]
    accounts = [
        {'account_id': f'ACC_{str(i).zfill(8)}', 'customer_id': f'CUST_{str(i).zfill(5)}', 
         'base_currency': 'CHF'}
        for i in range(1, 11)
    ]
    fx_rates = {'CHF': 1.0, 'EUR': 0.95, 'USD': 0.88, 'GBP': 1.12}
    
    generator = FixedIncomeTradeGenerator(
        customers=customers,
        accounts=accounts,
        fx_rates=fx_rates,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31)
    )
    
    trades = generator.generate_trades(num_trades=100)
    generator.save_to_csv(trades, Path('generated_data/fixed_income_trades/fi_trades.csv'))
