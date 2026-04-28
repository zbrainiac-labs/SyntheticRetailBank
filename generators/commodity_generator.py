"""
Commodity Trade Generator for Energy, Metals, and Agricultural Products

This module generates synthetic commodity trade data with:
- Energy: Crude Oil, Natural Gas, Electricity
- Precious Metals: Gold, Silver, Platinum, Palladium
- Base Metals: Copper, Aluminum, Zinc, Nickel
- Agricultural: Wheat, Corn, Soybeans, Coffee, Sugar
- Multi-currency support with CHF as base currency
- Realistic pricing and market conventions
- Delta calculations for FRTB
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
class CommodityTrade:
    """Represents a single commodity trade"""
    trade_date: str
    settlement_date: str
    trade_id: str
    customer_id: str
    account_id: str
    order_id: str
    
    # Commodity details
    commodity_type: str  # ENERGY, PRECIOUS_METAL, BASE_METAL, AGRICULTURAL
    commodity_name: str  # Crude Oil, Gold, Copper, Wheat, etc.
    commodity_code: str  # WTI, XAU, HG, ZW, etc.
    contract_type: str  # SPOT, FUTURE, FORWARD, SWAP
    
    # Trade details
    side: str  # 1=Buy, 2=Sell (FIX standard)
    quantity: float  # In commodity units
    unit: str  # Barrel, Troy Ounce, Metric Ton, Bushel, etc.
    price: float  # Price per unit
    currency: str  # Trading currency
    
    # Financial details
    gross_amount: float  # Signed: quantity * price
    commission: float
    net_amount: float  # Signed: gross_amount +/- commission
    base_currency: str  # CHF
    base_gross_amount: float  # Signed amount in CHF
    base_net_amount: float  # Signed amount in CHF
    fx_rate: float
    
    # Contract details
    contract_size: float  # Standard contract size
    num_contracts: float  # Number of contracts
    delivery_month: Optional[str]  # For futures (e.g., "2025-03")
    delivery_location: Optional[str]  # Physical delivery location
    
    # Risk metrics for FRTB
    delta: float  # Price sensitivity (change in value for $1 move in commodity)
    vega: Optional[float]  # Volatility sensitivity (for options, if applicable)
    spot_price: float  # Current spot price
    forward_price: Optional[float]  # Forward price if applicable
    volatility: float  # Implied or historical volatility (%)
    
    # Market data
    exchange: str  # CME, ICE, LME, NYMEX, etc.
    broker_id: str
    venue: str
    liquidity_score: float  # 1-10, for NMRF classification
    
    # Metadata
    created_at: str


class CommodityTradeGenerator(BaseGenerator):
    """Generator for synthetic commodity trades"""
    
    # Commodity definitions with typical price ranges and units
    COMMODITIES = {
        'ENERGY': {
            'Crude Oil WTI': {
                'code': 'CL',
                'unit': 'Barrel',
                'contract_size': 1000,  # barrels per contract
                'price_range': (60, 90),
                'currency': 'USD',
                'exchange': 'NYMEX',
                'volatility_range': (20, 40),
            },
            'Crude Oil Brent': {
                'code': 'BRN',
                'unit': 'Barrel',
                'contract_size': 1000,
                'price_range': (62, 92),
                'currency': 'USD',
                'exchange': 'ICE',
                'volatility_range': (20, 40),
            },
            'Natural Gas': {
                'code': 'NG',
                'unit': 'MMBtu',
                'contract_size': 10000,
                'price_range': (2.5, 6.0),
                'currency': 'USD',
                'exchange': 'NYMEX',
                'volatility_range': (30, 60),
            },
            'Heating Oil': {
                'code': 'HO',
                'unit': 'Gallon',
                'contract_size': 42000,
                'price_range': (2.0, 3.5),
                'currency': 'USD',
                'exchange': 'NYMEX',
                'volatility_range': (25, 45),
            },
        },
        'PRECIOUS_METAL': {
            'Gold': {
                'code': 'XAU',
                'unit': 'Troy Ounce',
                'contract_size': 100,
                'price_range': (1800, 2100),
                'currency': 'USD',
                'exchange': 'COMEX',
                'volatility_range': (10, 20),
            },
            'Silver': {
                'code': 'XAG',
                'unit': 'Troy Ounce',
                'contract_size': 5000,
                'price_range': (22, 28),
                'currency': 'USD',
                'exchange': 'COMEX',
                'volatility_range': (15, 30),
            },
            'Platinum': {
                'code': 'XPT',
                'unit': 'Troy Ounce',
                'contract_size': 50,
                'price_range': (900, 1100),
                'currency': 'USD',
                'exchange': 'NYMEX',
                'volatility_range': (15, 25),
            },
            'Palladium': {
                'code': 'XPD',
                'unit': 'Troy Ounce',
                'contract_size': 100,
                'price_range': (1500, 2000),
                'currency': 'USD',
                'exchange': 'NYMEX',
                'volatility_range': (20, 35),
            },
        },
        'BASE_METAL': {
            'Copper': {
                'code': 'HG',
                'unit': 'Pound',
                'contract_size': 25000,
                'price_range': (3.5, 4.5),
                'currency': 'USD',
                'exchange': 'COMEX',
                'volatility_range': (15, 30),
            },
            'Aluminum': {
                'code': 'ALI',
                'unit': 'Metric Ton',
                'contract_size': 25,
                'price_range': (2200, 2800),
                'currency': 'USD',
                'exchange': 'LME',
                'volatility_range': (15, 25),
            },
            'Zinc': {
                'code': 'ZNC',
                'unit': 'Metric Ton',
                'contract_size': 25,
                'price_range': (2500, 3200),
                'currency': 'USD',
                'exchange': 'LME',
                'volatility_range': (20, 35),
            },
            'Nickel': {
                'code': 'NKL',
                'unit': 'Metric Ton',
                'contract_size': 6,
                'price_range': (16000, 22000),
                'currency': 'USD',
                'exchange': 'LME',
                'volatility_range': (25, 45),
            },
        },
        'AGRICULTURAL': {
            'Corn': {
                'code': 'ZC',
                'unit': 'Bushel',
                'contract_size': 5000,
                'price_range': (4.5, 6.5),
                'currency': 'USD',
                'exchange': 'CBOT',
                'volatility_range': (15, 30),
            },
            'Wheat': {
                'code': 'ZW',
                'unit': 'Bushel',
                'contract_size': 5000,
                'price_range': (5.5, 8.0),
                'currency': 'USD',
                'exchange': 'CBOT',
                'volatility_range': (20, 35),
            },
            'Soybeans': {
                'code': 'ZS',
                'unit': 'Bushel',
                'contract_size': 5000,
                'price_range': (12.0, 15.0),
                'currency': 'USD',
                'exchange': 'CBOT',
                'volatility_range': (15, 30),
            },
            'Coffee': {
                'code': 'KC',
                'unit': 'Pound',
                'contract_size': 37500,
                'price_range': (1.5, 2.5),
                'currency': 'USD',
                'exchange': 'ICE',
                'volatility_range': (25, 40),
            },
            'Sugar': {
                'code': 'SB',
                'unit': 'Pound',
                'contract_size': 112000,
                'price_range': (0.15, 0.25),
                'currency': 'USD',
                'exchange': 'ICE',
                'volatility_range': (20, 35),
            },
        },
    }
    
    def __init__(self, config, customers: List[str], accounts: List[Dict], 
                 fx_rates: Dict[str, float], start_date: date, end_date: date):
        """
        Initialize the generator
        
        Args:
            config: GeneratorConfig instance
            customers: List of customer IDs
            accounts: List of account dictionaries
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
    
    def _generate_delivery_month(self, trade_date: date) -> str:
        """Generate a delivery month for futures (1-12 months forward)"""
        months_forward = random.randint(1, 12)
        delivery_date = trade_date + timedelta(days=30 * months_forward)
        return delivery_date.strftime('%Y-%m')
    
    def _calculate_delta(self, quantity: float, contract_size: float, 
                        price: float, fx_rate: float) -> float:
        """Calculate delta (price sensitivity in CHF)"""
        # Delta = quantity * contract_size * fx_rate
        # This represents the change in portfolio value for $1 move in commodity price
        return quantity * contract_size * fx_rate
    
    def generate_trade(self, customer_id: str, account: Dict, 
                      trade_date: date) -> CommodityTrade:
        """Generate a single commodity trade"""
        
        # Select commodity type and commodity
        commodity_type = random.choice(list(self.COMMODITIES.keys()))
        commodity_name = random.choice(list(self.COMMODITIES[commodity_type].keys()))
        commodity_spec = self.COMMODITIES[commodity_type][commodity_name]
        
        # Contract type
        contract_type = random.choices(
            ['SPOT', 'FUTURE', 'FORWARD', 'SWAP'],
            weights=[0.2, 0.5, 0.2, 0.1]
        )[0]
        
        # Price (with some random variation)
        price_range = commodity_spec['price_range']
        spot_price = round(random.uniform(*price_range), 2)
        
        # Forward price (slightly higher for futures/forwards)
        if contract_type in ['FUTURE', 'FORWARD']:
            forward_price = spot_price * random.uniform(1.0, 1.05)
            price = round(forward_price, 2)
        else:
            forward_price = None
            price = spot_price
        
        # Volatility
        vol_range = commodity_spec['volatility_range']
        volatility = round(random.uniform(*vol_range), 1)
        
        # Quantity (number of contracts)
        num_contracts = random.choice([1, 2, 5, 10, 25, 50, 100])
        contract_size = commodity_spec['contract_size']
        quantity = num_contracts * contract_size
        
        # Side (Buy or Sell)
        side = random.choice(['1', '2'])  # 1=Buy, 2=Sell
        
        # Currency and FX rate
        currency = commodity_spec['currency']
        fx_rate = self.fx_rates.get(currency, 1.0)
        
        # Gross amount
        gross_amount = quantity * price
        if side == '2':  # Sell
            gross_amount = -gross_amount
        
        # Commission (5-20 bps of notional)
        commission = abs(gross_amount) * random.uniform(0.0005, 0.0020)
        
        # Net amount
        if side == '1':  # Buy
            net_amount = gross_amount + commission
        else:  # Sell
            net_amount = gross_amount - commission
        
        # Convert to base currency (CHF)
        base_gross_amount = gross_amount * fx_rate
        base_net_amount = net_amount * fx_rate
        
        # Calculate delta
        delta = self._calculate_delta(quantity, 1.0, price, fx_rate)
        if side == '2':  # Sell
            delta = -delta
        
        # Settlement date (T+2 for most commodities, T+0 for spot)
        if contract_type == 'SPOT':
            settlement_date = trade_date
        else:
            settlement_date = self._next_business_day(trade_date, days=2)
        
        # Delivery month (for futures)
        delivery_month = None
        if contract_type == 'FUTURE':
            delivery_month = self._generate_delivery_month(trade_date)
        
        # Delivery location
        delivery_locations = {
            'ENERGY': ['Cushing, OK', 'Rotterdam', 'Singapore', 'Houston, TX'],
            'PRECIOUS_METAL': ['London', 'New York', 'Zurich'],
            'BASE_METAL': ['London', 'Rotterdam', 'Singapore'],
            'AGRICULTURAL': ['Chicago', 'Kansas City', 'Minneapolis'],
        }
        delivery_location = random.choice(delivery_locations.get(commodity_type, ['N/A']))
        
        # Liquidity score (energy and precious metals more liquid)
        if commodity_type in ['ENERGY', 'PRECIOUS_METAL']:
            liquidity_score = random.uniform(7, 10)
        elif commodity_type == 'BASE_METAL':
            liquidity_score = random.uniform(6, 9)
        else:  # AGRICULTURAL
            liquidity_score = random.uniform(4, 8)
        
        return CommodityTrade(
            trade_date=trade_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            settlement_date=settlement_date.strftime('%Y-%m-%d'),
            trade_id=f"CMD_{uuid.uuid4().hex[:12].upper()}",
            customer_id=customer_id,
            account_id=account['account_id'],
            order_id=f"ORD_{uuid.uuid4().hex[:8].upper()}",
            commodity_type=commodity_type,
            commodity_name=commodity_name,
            commodity_code=commodity_spec['code'],
            contract_type=contract_type,
            side=side,
            quantity=quantity,
            unit=commodity_spec['unit'],
            price=price,
            currency=currency,
            gross_amount=round(gross_amount, 2),
            commission=round(commission, 2),
            net_amount=round(net_amount, 2),
            base_currency='CHF',
            base_gross_amount=round(base_gross_amount, 2),
            base_net_amount=round(base_net_amount, 2),
            fx_rate=round(fx_rate, 6),
            contract_size=contract_size,
            num_contracts=num_contracts,
            delivery_month=delivery_month,
            delivery_location=delivery_location if contract_type != 'SWAP' else None,
            delta=round(delta, 2),
            vega=None,  # Would need options for vega
            spot_price=spot_price,
            forward_price=forward_price,
            volatility=volatility,
            exchange=commodity_spec['exchange'],
            broker_id=f"BRK_{random.randint(100, 999)}",
            venue=commodity_spec['exchange'],
            liquidity_score=round(liquidity_score, 2),
            created_at=self.get_utc_timestamp()
        )
    
    def generate_trades(self, num_trades: int = 1000) -> List[CommodityTrade]:
        """
        Generate multiple commodity trades
        
        Args:
            num_trades: Number of trades to generate
        
        Returns:
            List of CommodityTrade objects
        """
        trades = []
        
        print(f"Generating {num_trades} commodity trades...")
        
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
            
            trade = self.generate_trade(customer_id, account, trade_date)
            trades.append(trade)
            
            if (i + 1) % 100 == 0:
                print(f"  Generated {i + 1}/{num_trades} trades...")
        
        print(f"✓ Generated {len(trades)} commodity trades")
        return trades
    
    def save_to_csv(self, trades: List[CommodityTrade], output_path: Path):
        """Save trades to CSV file"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            if not trades:
                return
            
            # Get field names from dataclass
            field_names = [field.name for field in fields(CommodityTrade)]
            writer = csv.DictWriter(f, fieldnames=field_names)
            
            writer.writeheader()
            for trade in trades:
                writer.writerow(asdict(trade))
        
        print(f"✓ Saved {len(trades)} trades to {output_path}")
    
    def save_to_csv_by_date(self, trades: List[CommodityTrade], output_dir: Path):
        """
        Save trades to separate CSV files grouped by trade date
        
        Args:
            trades: List of CommodityTrade objects
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
        field_names = [field.name for field in fields(CommodityTrade)]
        
        # Save each date to a separate file
        files_created = []
        for trade_date, date_trades in sorted(trades_by_date.items()):
            output_file = output_dir / f'commodity_trades_{trade_date}.csv'
            
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
        """Generate commodity trades - implementation of abstract method"""
        trades = self.generate_trades()
        return {
            'trades': trades,
            'total_trades': len(trades),
            'commodity_types': {}
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
    
    generator = CommodityTradeGenerator(
        customers=customers,
        accounts=accounts,
        fx_rates=fx_rates,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31)
    )
    
    trades = generator.generate_trades(num_trades=100)
    generator.save_to_csv(trades, Path('generated_data/commodity_trades/commodity_trades.csv'))
