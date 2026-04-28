"""
Foreign Exchange (FX) rate generation module
"""
import csv
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass
import math

from config import GeneratorConfig


@dataclass
class FXRate:
    """FX rate data structure"""
    date: datetime
    from_currency: str
    to_currency: str
    rate: float
    bid_rate: float
    ask_rate: float


@dataclass
class Account:
    """Account master data structure"""
    account_id: str
    account_type: str
    base_currency: str
    customer_id: str
    status: str


class FXRateGenerator:
    """Generates realistic FX rates with market volatility"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.base_currency = "USD"  # Base currency for all conversions
        self.currencies = config.available_currencies
        
        # Initial exchange rates (approximate real-world rates)
        self.base_rates = {
            "USD": 1.0000,
            "EUR": 0.8500,  # 1 USD = 0.85 EUR
            "GBP": 0.7500,  # 1 USD = 0.75 GBP
            "JPY": 150.0000,  # 1 USD = 150 JPY
            "CAD": 1.3500   # 1 USD = 1.35 CAD
        }
        
        # Volatility parameters for each currency pair
        self.volatility = {
            "EUR": 0.008,  # 0.8% daily volatility
            "GBP": 0.012,  # 1.2% daily volatility
            "JPY": 0.010,  # 1.0% daily volatility
            "CAD": 0.006   # 0.6% daily volatility
        }
    
    def generate_fx_rates(self) -> List[FXRate]:
        """Generate FX rates for all currency pairs across the date range"""
        fx_rates = []
        current_rates = self.base_rates.copy()
        
        current_date = self.config.start_date
        while current_date <= self.config.end_date:
            # Skip weekends for FX rates (markets closed)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                daily_rates = self._generate_daily_rates(current_date, current_rates)
                fx_rates.extend(daily_rates)
                
                # Update current rates for next day (market evolution)
                current_rates = self._evolve_rates(current_rates)
            
            current_date += timedelta(days=1)
        
        return fx_rates
    
    def _generate_daily_rates(self, date: datetime, current_rates: Dict[str, float]) -> List[FXRate]:
        """Generate FX rates for a specific date"""
        daily_rates = []
        
        for currency in self.currencies:
            if currency == self.base_currency:
                continue
            
            mid_rate = current_rates[currency]
            
            # Generate bid-ask spread (typically 0.1% - 0.5% for major currencies)
            spread_pct = random.uniform(0.001, 0.005)  # 0.1% to 0.5%
            spread = mid_rate * spread_pct
            
            bid_rate = mid_rate - spread / 2
            ask_rate = mid_rate + spread / 2
            
            # Create FX rate record
            fx_rate = FXRate(
                date=date,
                from_currency=self.base_currency,
                to_currency=currency,
                rate=round(mid_rate, 6),
                bid_rate=round(bid_rate, 6),
                ask_rate=round(ask_rate, 6)
            )
            daily_rates.append(fx_rate)
            
            # Also create reverse rate (e.g., EUR to USD)
            reverse_mid_rate = 1.0 / mid_rate
            reverse_spread = reverse_mid_rate * spread_pct
            reverse_bid = reverse_mid_rate - reverse_spread / 2
            reverse_ask = reverse_mid_rate + reverse_spread / 2
            
            reverse_fx_rate = FXRate(
                date=date,
                from_currency=currency,
                to_currency=self.base_currency,
                rate=round(reverse_mid_rate, 6),
                bid_rate=round(reverse_bid, 6),
                ask_rate=round(reverse_ask, 6)
            )
            daily_rates.append(reverse_fx_rate)
        
        return daily_rates
    
    def _evolve_rates(self, current_rates: Dict[str, float]) -> Dict[str, float]:
        """Evolve FX rates for the next day using random walk with mean reversion"""
        new_rates = current_rates.copy()
        
        for currency in self.currencies:
            if currency == self.base_currency:
                continue
            
            # Mean reversion factor (tendency to return to base rate)
            mean_reversion = 0.001  # 0.1% pull toward base rate
            drift_toward_base = (self.base_rates[currency] - current_rates[currency]) * mean_reversion
            
            # Random volatility component
            volatility = self.volatility[currency]
            random_change = random.gauss(0, volatility)
            
            # Calculate new rate
            rate_change = drift_toward_base + random_change
            new_rate = current_rates[currency] * (1 + rate_change)
            
            # Prevent extreme moves (circuit breaker)
            max_change = 0.05  # 5% maximum daily change
            change_ratio = new_rate / current_rates[currency]
            if change_ratio > (1 + max_change):
                new_rate = current_rates[currency] * (1 + max_change)
            elif change_ratio < (1 - max_change):
                new_rate = current_rates[currency] * (1 - max_change)
            
            new_rates[currency] = max(0.0001, new_rate)  # Prevent negative rates
        
        return new_rates
    
    def save_fx_rates_to_csv(self, fx_rates: List[FXRate], output_dir: str) -> str:
        """Save FX rates to CSV file"""
        filename = f"{output_dir}/fx_rates.csv"
        fieldnames = ["date", "from_currency", "to_currency", "mid_rate", "bid_rate", "ask_rate"]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for fx_rate in fx_rates:
                writer.writerow({
                    "date": fx_rate.date.strftime("%Y-%m-%d"),
                    "from_currency": fx_rate.from_currency,
                    "to_currency": fx_rate.to_currency,
                    "mid_rate": fx_rate.rate,
                    "bid_rate": fx_rate.bid_rate,
                    "ask_rate": fx_rate.ask_rate
                })
        
        return filename
    
    def save_fx_rates_to_csv_by_date(self, fx_rates: List[FXRate], output_dir: str) -> List[Tuple[str, int, str]]:
        """
        Save FX rates to separate CSV files grouped by date
        
        Args:
            fx_rates: List of FXRate objects
            output_dir: Directory where date-specific CSV files will be saved
        
        Returns:
            List of tuples (date, count, filename)
        """
        from collections import defaultdict
        from pathlib import Path
        
        if not fx_rates:
            print("No FX rates to save")
            return []
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Group rates by date
        rates_by_date = defaultdict(list)
        for fx_rate in fx_rates:
            rate_date = fx_rate.date.strftime("%Y-%m-%d")
            rates_by_date[rate_date].append(fx_rate)
        
        # Field names for CSV
        fieldnames = ["date", "from_currency", "to_currency", "mid_rate", "bid_rate", "ask_rate"]
        
        # Save each date to a separate file
        files_created = []
        for rate_date, date_rates in sorted(rates_by_date.items()):
            filename = f"{output_dir}/fx_rates_{rate_date}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for fx_rate in date_rates:
                    writer.writerow({
                        "date": fx_rate.date.strftime("%Y-%m-%d"),
                        "from_currency": fx_rate.from_currency,
                        "to_currency": fx_rate.to_currency,
                        "mid_rate": fx_rate.rate,
                        "bid_rate": fx_rate.bid_rate,
                        "ask_rate": fx_rate.ask_rate
                    })
            
            files_created.append((rate_date, len(date_rates), filename))
            print(f"  ✓ fx_rates_{rate_date}.csv: {len(date_rates)} rates")
        
        print(f"\n✓ Saved {len(fx_rates)} FX rates across {len(files_created)} files in {output_dir}")
        return files_created
    
    def get_fx_rate(self, fx_rates: List[FXRate], date: datetime, 
                   from_currency: str, to_currency: str) -> float:
        """Get FX rate for a specific date and currency pair"""
        if from_currency == to_currency:
            return 1.0
        
        # Find the rate for the specific date
        for fx_rate in fx_rates:
            if (fx_rate.date.date() == date.date() and 
                fx_rate.from_currency == from_currency and 
                fx_rate.to_currency == to_currency):
                return fx_rate.rate
        
        # If no exact date found, use the most recent rate
        relevant_rates = [
            fx_rate for fx_rate in fx_rates
            if (fx_rate.from_currency == from_currency and 
                fx_rate.to_currency == to_currency and
                fx_rate.date.date() <= date.date())
        ]
        
        if relevant_rates:
            latest_rate = max(relevant_rates, key=lambda r: r.date)
            return latest_rate.rate
        
        # Fallback to base rates
        if from_currency == "USD":
            return self.base_rates.get(to_currency, 1.0)
        elif to_currency == "USD":
            return 1.0 / self.base_rates.get(from_currency, 1.0)
        else:
            # Cross rate calculation
            usd_to_from = 1.0 / self.base_rates.get(from_currency, 1.0)
            usd_to_to = self.base_rates.get(to_currency, 1.0)
            return usd_to_to / usd_to_from


class AccountGenerator:
    """Generates account master data"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
    
    def generate_accounts(self, customers: List) -> List[Account]:
        """Generate account master data for all customers"""
        accounts = []
        
        for customer in customers:
            # Each customer gets 1-3 accounts
            num_accounts = random.choices([1, 2, 3], weights=[50, 35, 15], k=1)[0]
            
            for i in range(num_accounts):
                account_type = random.choice(["CHECKING", "SAVINGS", "BUSINESS", "INVESTMENT"])
                
                # Account currency distribution
                if account_type in ["CHECKING", "SAVINGS"]:
                    # Domestic accounts mostly in USD
                    base_currency = random.choices(
                        self.config.available_currencies,
                        weights=[70, 10, 10, 5, 5],  # USD bias
                        k=1
                    )[0]
                else:
                    # Business/Investment accounts more international
                    base_currency = random.choices(
                        self.config.available_currencies,
                        weights=[40, 20, 20, 10, 10],  # More diverse
                        k=1
                    )[0]
                
                account_id = f"{customer.customer_id}_{account_type}_{i+1:02d}"
                
                account = Account(
                    account_id=account_id,
                    account_type=account_type,
                    base_currency=base_currency,
                    customer_id=customer.customer_id,
                    status=random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT"])  # Mostly active
                )
                accounts.append(account)
        
        return accounts
    
    def save_accounts_to_csv(self, accounts: List[Account], output_dir: str) -> str:
        """Save account master data to CSV file"""
        filename = f"{output_dir}/accounts.csv"
        fieldnames = ["account_id", "account_type", "base_currency", 
                      "customer_id", "status"]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for account in accounts:
                writer.writerow({
                    "account_id": account.account_id,
                    "account_type": account.account_type,
                    "base_currency": account.base_currency,
                    "customer_id": account.customer_id,
                    "status": account.status
                })
        
        return filename

