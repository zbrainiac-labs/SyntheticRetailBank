"""
Transaction generation module with anomaly detection capabilities
"""
import csv
import random
import uuid
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from config import GeneratorConfig
from customer_generator import Customer
from anomaly_patterns import AnomalyPatternGenerator, AnomalyType


@dataclass
class Transaction:
    """Transaction data structure"""
    booking_date: datetime
    value_date: datetime
    transaction_id: str
    account_id: str
    amount: float  # Positive for incoming, negative for outgoing
    currency: str
    base_amount: float  # Positive for incoming, negative for outgoing
    base_currency: str
    fx_rate: float
    counterparty_account: str
    description: str


class TransactionGenerator:
    """Generates realistic transaction data with anomaly patterns"""
    
    def __init__(self, config: GeneratorConfig, customers: List[Customer], fx_rates: List = None, accounts_file: str = None):
        self.config = config
        self.customers = customers
        self.fx_rates = fx_rates or []
        self.base_currency = "USD"
        self.anomaly_generator = AnomalyPatternGenerator(config)
        self.transactions: List[Transaction] = []
        self.anomaly_characteristics = {}
        self.accounts_by_customer = {}  # Map customer_id to list of account_numbers
        
        # Load accounts data
        if accounts_file:
            self._load_accounts(accounts_file)
        
        # Initialize anomaly characteristics for anomalous customers
        for customer in customers:
            if customer.has_anomaly:
                self.anomaly_characteristics[customer.customer_id] = \
                    self.anomaly_generator.generate_anomaly_characteristics(customer.customer_id)
    
    def _load_accounts(self, accounts_file: str):
        """Load accounts data and map to customers"""
        try:
            with open(accounts_file, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    customer_id = row['customer_id']
                    account_id = row['account_id']
                    
                    if customer_id not in self.accounts_by_customer:
                        self.accounts_by_customer[customer_id] = []
                    self.accounts_by_customer[customer_id].append({
                        'account_id': account_id,
                        'account_type': row['account_type'],
                        'base_currency': row['base_currency'],
                        'status': row['status']
                    })
        except FileNotFoundError:
            print(f"Warning: Accounts file {accounts_file} not found. Using customer_id fallback.")
    
    def generate_all_transactions(self) -> List[Transaction]:
        """Generate all transactions for the specified period"""
        transactions = []
        
        current_date = self.config.start_date
        while current_date <= self.config.end_date:
            # Skip weekends for most transactions (some anomalous ones might occur)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                daily_transactions = self._generate_daily_transactions(current_date)
                transactions.extend(daily_transactions)
            
            current_date += timedelta(days=1)
        
        self.transactions = transactions
        return transactions
    
    def _generate_daily_transactions(self, date: datetime) -> List[Transaction]:
        """Generate transactions for a specific day"""
        daily_transactions = []
        
        for customer in self.customers:
            # Check if customer was onboarded by this date
            onboarding_date = datetime.strptime(customer.onboarding_date, "%Y-%m-%d")
            if date < onboarding_date:
                continue
            
            # Calculate expected transactions for this customer on this day
            monthly_transactions = self.config.avg_transactions_per_customer_per_month
            # Use Poisson distribution approach for more realistic distribution
            # Business days only, so divide by ~22 business days per month
            daily_rate = monthly_transactions / 22
            
            # Check for high frequency anomaly
            if customer.has_anomaly and customer.customer_id in self.anomaly_characteristics:
                anomaly_chars = self.anomaly_characteristics[customer.customer_id]
                should_apply, anomaly_types = self.anomaly_generator.should_apply_anomaly(
                    customer.customer_id, anomaly_chars, date
                )
                
                if should_apply and AnomalyType.HIGH_FREQUENCY in anomaly_types:
                    # Generate multiple transactions for high frequency anomaly
                    num_transactions = random.randint(5, anomaly_chars["high_frequency_threshold"])
                    for _ in range(num_transactions):
                        transaction = self._generate_single_transaction(customer, date)
                        # Apply anomalies to some of these transactions
                        if random.random() < 0.4:  # 40% of high-frequency transactions are anomalous
                            transaction = self.anomaly_generator.apply_anomaly_to_transaction(
                                transaction.__dict__, anomaly_types, anomaly_chars
                            )
                            transaction = Transaction(**transaction)
                        daily_transactions.append(transaction)
                    continue
            
            # Regular transaction generation using Poisson distribution
            # Generate number of transactions for this customer today
            num_transactions = np.random.poisson(daily_rate)
            
            # Ensure at least some activity (minimum 1 transaction every few days on average)
            if num_transactions == 0 and random.random() < 0.3:  # 30% chance of at least 1 transaction
                num_transactions = 1
            
            # Cap maximum transactions per day for non-anomalous customers
            num_transactions = min(num_transactions, 8)
            
            for _ in range(num_transactions):
                transaction = self._generate_single_transaction(customer, date)
                
                # Apply anomalies if customer is flagged and conditions are met
                if customer.has_anomaly and customer.customer_id in self.anomaly_characteristics:
                    anomaly_chars = self.anomaly_characteristics[customer.customer_id]
                    should_apply, anomaly_types = self.anomaly_generator.should_apply_anomaly(
                        customer.customer_id, anomaly_chars, date
                    )
                    
                    if should_apply:
                        transaction_dict = self.anomaly_generator.apply_anomaly_to_transaction(
                            transaction.__dict__, anomaly_types, anomaly_chars
                        )
                        transaction = Transaction(**transaction_dict)
                
                daily_transactions.append(transaction)
        
        return daily_transactions
    
    def _select_account_for_customer(self, customer: Customer) -> dict:
        """Select an account for the customer based on transaction patterns"""
        customer_accounts = self.accounts_by_customer.get(customer.customer_id, [])
        
        if not customer_accounts:
            # Fallback: create a synthetic account ID
            return {
                'account_id': f"{customer.customer_id}_CHECKING_01",
                'account_type': 'CHECKING',
                'base_currency': 'USD',
                'status': 'ACTIVE'
            }
        
        # Weighted selection based on account type
        # CHECKING accounts are used most frequently for transactions
        account_weights = []
        for account in customer_accounts:
            if account['account_type'] == 'CHECKING':
                account_weights.append(0.6)  # 60% of transactions
            elif account['account_type'] == 'SAVINGS':
                account_weights.append(0.2)  # 20% of transactions
            elif account['account_type'] == 'BUSINESS':
                account_weights.append(0.15)  # 15% of transactions
            elif account['account_type'] == 'INVESTMENT':
                account_weights.append(0.05)  # 5% of transactions
            else:
                account_weights.append(0.1)  # Default weight
        
        # Normalize weights
        total_weight = sum(account_weights)
        if total_weight > 0:
            account_weights = [w / total_weight for w in account_weights]
        else:
            account_weights = [1.0 / len(customer_accounts)] * len(customer_accounts)
        
        # Select account based on weights
        selected_account = random.choices(customer_accounts, weights=account_weights, k=1)[0]
        return selected_account
    
    def _generate_single_transaction(self, customer: Customer, date: datetime) -> Transaction:
        """Generate a single transaction for a customer"""
        
        # Select an account for this customer
        selected_account = self._select_account_for_customer(customer)
        
        # Generate transaction time (business hours mostly) in UTC
        # Assuming the bank operates in EST/EDT (UTC-5/UTC-4), convert to UTC
        # Business hours 9 AM - 5 PM EST = 14:00 - 22:00 UTC (EST) or 13:00 - 21:00 UTC (EDT)
        # For simplicity, using UTC+0 equivalent of business hours
        hour = random.choices(
            range(14, 22),  # 2 PM to 10 PM UTC (equivalent to 9 AM - 5 PM EST)
            weights=[1, 2, 3, 4, 5, 5, 4, 3],  # Peak around 5-6 PM UTC
            k=1
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        microsecond = random.randint(0, 999999)
        booking_time = date.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)
        
        # Generate transaction details
        transaction_id = f"TXN_{uuid.uuid4().hex[:12].upper()}"
        is_incoming = random.choice([True, False])
        currency = random.choice(self.config.available_currencies)
        
        # Generate amount (log-normal distribution for realistic amounts)
        raw_amount = self._generate_transaction_amount()
        
        # Generate counterparty account
        counterparty = self._generate_counterparty_account(is_incoming)
        
        # Generate description
        description = self._generate_transaction_description(is_incoming, raw_amount)
        
        # Calculate value date based on transaction type and amount
        value_date = self._calculate_value_date(booking_time, is_incoming, raw_amount, currency)
        
        # Calculate FX conversion to base currency
        fx_rate, raw_base_amount = self._calculate_fx_conversion(raw_amount, currency, value_date)
        
        # Apply sign: positive for incoming, negative for outgoing
        amount = raw_amount if is_incoming else -raw_amount
        base_amount = raw_base_amount if is_incoming else -raw_base_amount
        
        return Transaction(
            booking_date=booking_time,
            value_date=value_date,
            transaction_id=transaction_id,
            account_id=selected_account['account_id'],
            amount=round(amount, 2),
            currency=currency,
            base_amount=round(base_amount, 2),
            base_currency=self.base_currency,
            fx_rate=round(fx_rate, 6),
            counterparty_account=counterparty,
            description=description
        )
    
    def _generate_transaction_amount(self) -> float:
        """Generate realistic transaction amount using log-normal distribution"""
        # Use log-normal distribution for realistic amount distribution
        # Parameters for log-normal distribution
        mu = 6.5  # Mean of underlying normal distribution
        sigma = 1.2  # Standard deviation
        
        amount = np.random.lognormal(mu, sigma)
        
        # Clamp to configured range
        amount = max(self.config.min_transaction_amount, amount)
        amount = min(self.config.max_transaction_amount, amount)
        
        return amount
    
    def _generate_counterparty_account(self, is_incoming: bool) -> str:
        """Generate realistic counterparty account number"""
        if is_incoming:
            # Incoming transaction sources
            prefixes = ["PAYROLL_", "VENDOR_", "CLIENT_", "INVEST_", "BANK_"]
        else:
            # Outgoing transaction destinations
            prefixes = ["SUPPLIER_", "UTILITY_", "LOAN_", "INVEST_", "TRANSFER_"]
        
        prefix = random.choice(prefixes)
        account_id = f"{random.randint(1000000000, 9999999999):010d}"
        return f"{prefix}{account_id}"
    
    def _generate_transaction_description(self, is_incoming: bool, amount: float) -> str:
        """Generate realistic transaction description"""
        if is_incoming:
            descriptions = [
                "Salary payment",
                "Client payment for services",
                "Investment dividend",
                "Insurance claim payment",
                "Refund payment",
                "Freelance payment",
                "Rental income",
                "Interest payment"
            ]
        else:
            descriptions = [
                "Utility payment",
                "Supplier payment",
                "Loan repayment",
                "Investment purchase",
                "Insurance premium",
                "Equipment purchase",
                "Service fee payment",
                "Transfer to savings"
            ]
        
        base_description = random.choice(descriptions)
        
        # Add amount-based qualifiers for larger amounts
        if amount > 10000:
            base_description = f"Large {base_description.lower()}"
        elif amount > 5000:
            base_description = f"Substantial {base_description.lower()}"
        
        return base_description
    
    def _calculate_value_date(self, booking_date: datetime, is_incoming: bool, 
                            amount: float, currency: str) -> datetime:
        """Calculate realistic value date based on transaction characteristics"""
        
        # Different settlement patterns based on transaction type and amount
        if is_incoming:  # Incoming transactions
            if amount < 1000:
                # Small incoming payments: same day or next business day
                days_offset = random.choices([0, 1], weights=[70, 30], k=1)[0]
            elif amount < 10000:
                # Medium incoming payments: 0-2 business days
                days_offset = random.choices([0, 1, 2], weights=[50, 40, 10], k=1)[0]
            else:
                # Large incoming payments: may take longer for verification
                days_offset = random.choices([0, 1, 2, 3], weights=[20, 40, 30, 10], k=1)[0]
        else:  # Outgoing transactions
            if amount < 1000:
                # Small outgoing payments: usually same day
                days_offset = random.choices([0, 1], weights=[80, 20], k=1)[0]
            elif amount < 10000:
                # Medium outgoing payments: 0-1 business days
                days_offset = random.choices([0, 1], weights=[60, 40], k=1)[0]
            else:
                # Large outgoing payments: may require additional processing
                days_offset = random.choices([0, 1, 2], weights=[30, 50, 20], k=1)[0]
        
        # International transactions (non-USD) may have additional delays
        if currency != "USD" and random.random() < 0.3:  # 30% chance of additional delay
            days_offset += random.choice([1, 2])
        
        # Calculate value date, skipping weekends
        value_date = booking_date
        days_added = 0
        
        while days_added < days_offset:
            value_date += timedelta(days=1)
            # Skip weekends
            if value_date.weekday() < 5:  # Monday = 0, Friday = 4
                days_added += 1
        
        return value_date
    
    def _calculate_fx_conversion(self, amount: float, currency: str, value_date: datetime) -> Tuple[float, float]:
        """Calculate FX rate and base currency amount"""
        if currency == self.base_currency:
            return 1.0, amount
        
        # Import FXRateGenerator here to avoid circular imports
        from fx_generator import FXRateGenerator
        
        if self.fx_rates:
            # Use provided FX rates
            fx_generator = FXRateGenerator(self.config)
            fx_rate = fx_generator.get_fx_rate(self.fx_rates, value_date, currency, self.base_currency)
        else:
            # Use base rates if no FX rates provided
            base_rates = {
                "USD": 1.0000,
                "EUR": 0.8500,
                "GBP": 0.7500,
                "JPY": 150.0000,
                "CAD": 1.3500
            }
            fx_rate = 1.0 / base_rates.get(currency, 1.0)
        
        base_amount = amount * fx_rate
        return fx_rate, base_amount
    
    def get_transactions_by_date(self) -> dict:
        """Group all transactions by date (optimized for batch processing)"""
        transactions_by_date = {}
        for t in self.transactions:
            date_str = t.booking_date.strftime("%Y-%m-%d")
            if date_str not in transactions_by_date:
                transactions_by_date[date_str] = []
            transactions_by_date[date_str].append(t)
        return transactions_by_date
    
    def get_transactions_for_date(self, date: datetime) -> List[Transaction]:
        """Get all transactions for a specific date (legacy method, slower)"""
        date_str = date.strftime("%Y-%m-%d")
        return [
            t for t in self.transactions 
            if t.booking_date.strftime("%Y-%m-%d") == date_str
        ]
    
    def save_all_daily_transactions_to_csv(self, output_dir: str, show_progress: bool = True) -> dict:
        """Save all transactions grouped by date (optimized batch processing)"""
        from pathlib import Path
        import os
        
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Group transactions by date once (single pass through all transactions)
        print("  📊 Grouping transactions by date...")
        transactions_by_date = self.get_transactions_by_date()
        
        fieldnames = [
            "booking_date", "value_date", "transaction_id", "account_id", "amount", 
            "currency", "base_amount", "base_currency", "fx_rate", 
            "counterparty_account", "description"
        ]
        
        files_created = []
        transaction_counts = {}
        
        total_days = len(transactions_by_date)
        print(f"  💾 Writing {total_days} daily files...")
        
        # Write all files in batch
        for idx, (date_str, transactions) in enumerate(sorted(transactions_by_date.items()), 1):
            filename = f"{output_dir}/pay_transactions_{date_str}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Pre-format all rows at once (batch conversion)
                rows = []
                for t in transactions:
                    rows.append({
                        "booking_date": t.booking_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "value_date": t.value_date.strftime("%Y-%m-%d"),
                        "transaction_id": t.transaction_id,
                        "account_id": t.account_id,
                        "amount": t.amount,
                        "currency": t.currency,
                        "base_amount": t.base_amount,
                        "base_currency": t.base_currency,
                        "fx_rate": t.fx_rate,
                        "counterparty_account": t.counterparty_account,
                        "description": t.description
                    })
                
                # Write all rows at once (bulk write)
                writer.writerows(rows)
            
            files_created.append(filename)
            transaction_counts[date_str] = len(transactions)
            
            # Show progress every 50 files for large datasets
            if show_progress and idx % 50 == 0:
                print(f"  ⏳ Progress: {idx}/{total_days} files ({idx*100//total_days}%)")
        
        return {
            'files': files_created,
            'counts': transaction_counts
        }
    
    def save_daily_transactions_to_csv(self, date: datetime, output_dir: str) -> str:
        """Save transactions for a specific date to CSV file (legacy method, slower)"""
        transactions = self.get_transactions_for_date(date)
        
        if not transactions:
            return None
        
        filename = f"{output_dir}/pay_transactions_{date.strftime('%Y-%m-%d')}.csv"
        fieldnames = [
            "booking_date", "value_date", "transaction_id", "account_id", "amount", 
            "currency", "base_amount", "base_currency", "fx_rate", 
            "counterparty_account", "description"
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for transaction in transactions:
                writer.writerow({
                    "booking_date": transaction.booking_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "value_date": transaction.value_date.strftime("%Y-%m-%d"),
                    "transaction_id": transaction.transaction_id,
                    "account_id": transaction.account_id,
                    "amount": transaction.amount,
                    "currency": transaction.currency,
                    "base_amount": transaction.base_amount,
                    "base_currency": transaction.base_currency,
                    "fx_rate": transaction.fx_rate,
                    "counterparty_account": transaction.counterparty_account,
                    "description": transaction.description
                })
        
        return filename

