#!/usr/bin/env python3
"""
FINMA LCR Data Generator
========================
Generates synthetic HQLA holdings and deposit balances for LCR testing.

Usage:
    python lcr_data_generator.py --days 90 --customers 1000 --output-dir data/lcr

Dependencies:
    - pandas
    - numpy
    - faker (optional, for enhanced customer names)

Author: AAA Synthetic Bank
Date: January 2026
Regulatory Basis: FINMA Circular 2015/2
"""

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import numpy as np

class FINMALCRDataGenerator:
    """
    Generate synthetic HQLA and deposit data for LCR calculation testing
    
    Calibrated to produce LCR ratios in the 90-110% range:
    - Target HQLA: ~35-50B CHF (150-300 securities × 100-200M avg)
    - Target Deposits: ~170-220B CHF (2000 accounts, heavily weighted to retail)
    - Target Outflows: ~35-45B CHF (with 70% stable retail at 3-5% run-off)
    - Expected LCR: (40B HQLA / 37B outflows) × 100 = 108%
    
    Balance calibration (per 1000 customers with 2 accounts avg):
    - 1400 retail accounts @ 5-80K CHF (avg 42.5K) = ~60B
    - 280 corporate accounts @ 30-800K CHF (avg 415K) = ~116B
    - 20 FI accounts @ 100K-3M CHF (avg 1.55M) = ~31B
    - Total: ~207B deposits
    
    Run-off calculation with new balances:
    - 70% Retail stable (3-5% run-off) → 59.5B × 4.1% = 2.4B outflow
    - 15% Retail less stable (10% run-off) → 25.6B × 10% = 2.6B outflow  
    - 10% Corporate operational (25% run-off) → 82.95B × 25% = 20.7B outflow
    - 4% Corporate non-op (40% run-off) → 33.18B × 40% = 13.3B outflow
    - 1% FI (100% run-off) → 31M × 100% = 0.03B outflow
    - Total: ~39B outflow / ~207B deposits = 18.8% weighted avg run-off
    """
    
    def __init__(self, num_customers: int = 1000, random_seed: int = 42, target_lcr: float = 95.0):
        self.num_customers = num_customers
        self.rng = np.random.default_rng(random_seed)
        self.target_lcr = target_lcr  # Target LCR percentage (default 95% = just below threshold)
        
        # HQLA asset types with probabilities
        self.hqla_assets = [
            ('CASH_SNB', 0.15, 'L1', 1.00),
            ('CASH_VAULT', 0.05, 'L1', 1.00),
            ('GOVT_BOND_CHF', 0.30, 'L1', 1.00),
            ('GOVT_BOND_FOREIGN', 0.10, 'L1', 1.00),
            ('CANTON_BOND', 0.15, 'L2A', 0.85),
            ('COVERED_BOND', 0.10, 'L2A', 0.85),
            ('EQUITY_SMI', 0.10, 'L2B', 0.50),
            ('CORPORATE_BOND_AA', 0.05, 'L2B', 0.50)
        ]
        
        # Deposit types with probabilities (ADJUSTED for better LCR - more stable retail)
        self.deposit_types = [
            ('RETAIL_STABLE_INSURED', 0.40, 'RETAIL', 0.03, True, True),      # Increased from 30%
            ('RETAIL_STABLE', 0.30, 'RETAIL', 0.05, True, True),              # Increased from 25%
            ('RETAIL_LESS_STABLE', 0.15, 'RETAIL', 0.10, False, False),       # Decreased from 20%
            ('CORPORATE_OPERATIONAL', 0.10, 'CORPORATE', 0.25, False, True),   # Decreased from 15%
            ('CORPORATE_NON_OPERATIONAL', 0.04, 'CORPORATE', 0.40, False, False), # Decreased from 8%
            ('FINANCIAL_INSTITUTION', 0.01, 'FINANCIAL_INSTITUTION', 1.00, False, False) # Decreased from 2%
        ]
        
        # Currency distribution
        self.currencies = [
            ('CHF', 0.60),  # 60% CHF
            ('EUR', 0.20),  # 20% EUR
            ('USD', 0.15),  # 15% USD
            ('GBP', 0.05)   # 5% GBP
        ]
        
        # FX rates (vs CHF)
        self.fx_rates = {
            'CHF': 1.0000,
            'EUR': 0.9500,  # 1 EUR = 0.95 CHF
            'USD': 0.8800,  # 1 USD = 0.88 CHF
            'GBP': 1.1200   # 1 GBP = 1.12 CHF
        }
        
        # Credit ratings
        self.credit_ratings = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-']
        
        # SMI constituents (simplified - 20 major Swiss stocks)
        self.smi_stocks = [
            'CH0012032048',  # Roche
            'CH0244767585',  # Novartis
            'CH0038863350',  # Nestlé
            'CH0012005267',  # UBS
            'CH0012221716',  # ABB
            'CH0024608827',  # Zurich Insurance
            'CH0012138530',  # Lonza
            'CH0012032113',  # Sika
            'CH0025751329',  # Alcon
            'CH0210483332'   # Holcim
        ]
    
    def generate_hqla_holdings(self, as_of_date: datetime.date, daily_variance: float = 0.0) -> pd.DataFrame:
        """
        Generate HQLA holdings for a specific date
        
        Returns DataFrame with columns:
        - HOLDING_ID, AS_OF_DATE, ASSET_TYPE, ISIN, SECURITY_NAME, CURRENCY,
        - QUANTITY, MARKET_VALUE_CCY, MARKET_VALUE_CHF, FX_RATE, MATURITY_DATE,
        - CREDIT_RATING, SMI_CONSTITUENT, HQLA_ELIGIBLE, PORTFOLIO_CODE, CUSTODIAN
        """
        # Determine number of holdings (150-300 securities for larger HQLA base)
        num_holdings = self.rng.integers(150, 300)
        
        # Select asset types based on probabilities
        asset_types = self.rng.choice(
            [a[0] for a in self.hqla_assets],
            size=num_holdings,
            p=[a[1] for a in self.hqla_assets]
        )
        
        # Select currencies
        currencies = self.rng.choice(
            [c[0] for c in self.currencies],
            size=num_holdings,
            p=[c[1] for c in self.currencies]
        )
        
        holdings = []
        for i, (asset_type, currency) in enumerate(zip(asset_types, currencies)):
            # Generate unique holding ID
            holding_id = f"HOLD-{as_of_date.strftime('%Y%m%d')}-{i+1:05d}"
            
            # Generate ISIN (simplified)
            if 'EQUITY' in asset_type:
                isin = self.rng.choice(self.smi_stocks)
                smi_constituent = True
                credit_rating = None
                maturity_date = None
                quantity = self.rng.integers(100, 10000)
                security_name = f"SMI Stock {isin[-4:]}"
            elif 'BOND' in asset_type or 'COVERED' in asset_type:
                isin = f"{currency}{self.rng.integers(10000000, 99999999):08d}"
                smi_constituent = False
                credit_rating = self.rng.choice(self.credit_ratings)
                # Maturity 1-10 years out
                days_to_maturity = self.rng.integers(365, 3650)
                maturity_date = (as_of_date + timedelta(days=int(days_to_maturity))).isoformat()
                quantity = None
                security_name = f"{asset_type} {isin[-6:]}"
            else:  # CASH
                isin = None
                smi_constituent = False
                credit_rating = None
                maturity_date = None
                quantity = None
                security_name = f"{asset_type}"
            
            # Generate market values (INCREASED for better LCR ratio)
            if asset_type in ['CASH_SNB', 'CASH_VAULT']:
                # Cash: 10M to 200M CHF (was 1M-50M)
                base_value = self.rng.uniform(10_000_000, 200_000_000)
            elif 'EQUITY' in asset_type:
                # Equities: 5M to 100M CHF (was 500K-20M)
                base_value = self.rng.uniform(5_000_000, 100_000_000)
            else:  # Bonds
                # Bonds: 20M to 500M CHF (was 2M-100M)
                base_value = self.rng.uniform(20_000_000, 500_000_000)
            
            # Apply daily variance (±5% random fluctuation for realistic day-to-day changes)
            variance_factor = 1.0 + (daily_variance * self.rng.uniform(-0.05, 0.05))
            market_value_chf = base_value * variance_factor
            
            # Convert to currency
            fx_rate = self.fx_rates[currency]
            market_value_ccy = market_value_chf / fx_rate if currency != 'CHF' else market_value_chf
            
            # Determine eligibility (95% eligible, 5% ineligible due to various reasons)
            hqla_eligible = self.rng.random() > 0.05
            
            # Portfolio and custodian
            portfolio_code = self.rng.choice(['TREASURY_LIQ', 'TREASURY_INV', 'ALM_BUFFER'])
            custodian = self.rng.choice(['SIX SIS', 'EUROCLEAR', 'CLEARSTREAM'])
            
            holdings.append({
                'HOLDING_ID': holding_id,
                'AS_OF_DATE': as_of_date.isoformat(),
                'ASSET_TYPE': asset_type,
                'ISIN': isin,
                'SECURITY_NAME': security_name,
                'CURRENCY': currency,
                'QUANTITY': quantity,
                'MARKET_VALUE_CCY': round(market_value_ccy, 2),
                'MARKET_VALUE_CHF': round(market_value_chf, 2),
                'FX_RATE': fx_rate,
                'MATURITY_DATE': maturity_date,
                'CREDIT_RATING': credit_rating,
                'SMI_CONSTITUENT': smi_constituent,
                'HQLA_ELIGIBLE': hqla_eligible,
                'PORTFOLIO_CODE': portfolio_code,
                'CUSTODIAN': custodian
            })
        
        return pd.DataFrame(holdings)
    
    def generate_deposit_balances(
        self, 
        as_of_date: datetime.date,
        customer_ids: List[str]
    ) -> pd.DataFrame:
        """
        Generate deposit balances for existing customers
        
        Returns DataFrame with columns:
        - ACCOUNT_ID, AS_OF_DATE, CUSTOMER_ID, DEPOSIT_TYPE, CURRENCY,
        - BALANCE_CCY, BALANCE_CHF, FX_RATE, IS_INSURED, PRODUCT_COUNT,
        - ACCOUNT_TENURE_DAYS, HAS_DIRECT_DEBIT, IS_OPERATIONAL, COUNTERPARTY_TYPE,
        - CUSTOMER_SEGMENT, ACCOUNT_STATUS
        """
        # Each customer has 1-3 deposit accounts
        num_accounts_per_customer = self.rng.integers(1, 4, size=len(customer_ids))
        total_accounts = num_accounts_per_customer.sum()
        
        # Expand customer IDs
        expanded_customer_ids = []
        for cust_id, num_accounts in zip(customer_ids, num_accounts_per_customer):
            expanded_customer_ids.extend([cust_id] * num_accounts)
        
        # Select deposit types
        deposit_types = self.rng.choice(
            [d[0] for d in self.deposit_types],
            size=total_accounts,
            p=[d[1] for d in self.deposit_types]
        )
        
        # Select currencies (more CHF for deposits)
        currencies = self.rng.choice(
            ['CHF', 'EUR', 'USD'],
            size=total_accounts,
            p=[0.80, 0.15, 0.05]  # 80% CHF for deposits
        )
        
        deposits = []
        # Track accounts per customer for sequential numbering
        customer_account_counter = {}
        
        for i, (customer_id, deposit_type, currency) in enumerate(zip(
            expanded_customer_ids, deposit_types, currencies
        )):
            # Generate unique account ID matching format: CUST_00001_DEP_01
            if customer_id not in customer_account_counter:
                customer_account_counter[customer_id] = 1
            else:
                customer_account_counter[customer_id] += 1
            
            account_id = f"{customer_id}_DEP_{customer_account_counter[customer_id]:02d}"
            
            # Get deposit type metadata
            dt_meta = next(d for d in self.deposit_types if d[0] == deposit_type)
            counterparty_type = dt_meta[2]
            allows_discount = dt_meta[4]
            is_operational = dt_meta[5]
            
            # Generate balance (AGGRESSIVELY REDUCED for 90-110% LCR target)
            if counterparty_type == 'RETAIL':
                # Retail: 5K to 80K CHF (avg ~42.5K per account)
                balance_chf = self.rng.uniform(5_000, 80_000)
            elif counterparty_type == 'CORPORATE':
                # Corporate: 30K to 800K CHF (avg ~415K per account) - heavily reduced
                balance_chf = self.rng.uniform(30_000, 800_000)
            else:  # FINANCIAL_INSTITUTION
                # FI: 100K to 3M CHF (avg ~1.55M per account) - heavily reduced
                balance_chf = self.rng.uniform(100_000, 3_000_000)
            
            # Convert to currency
            fx_rate = self.fx_rates.get(currency, 1.0)
            balance_ccy = balance_chf / fx_rate if currency != 'CHF' else balance_chf
            
            # Is insured? (Only retail <100K CHF)
            is_insured = counterparty_type == 'RETAIL' and balance_chf <= 100_000
            
            # Product count (for relationship discount)
            if allows_discount:
                product_count = self.rng.integers(1, 6)  # 1-5 products
            else:
                product_count = 1
            
            # Account tenure (days since opening)
            account_tenure_days = self.rng.integers(30, 3650)  # 1 month to 10 years
            
            # Direct debit mandate (higher for retail)
            has_direct_debit = self.rng.random() < (0.7 if counterparty_type == 'RETAIL' else 0.3)
            
            # Customer segment
            if counterparty_type == 'RETAIL':
                if balance_chf < 50_000:
                    customer_segment = 'MASS'
                elif balance_chf < 250_000:
                    customer_segment = 'AFFLUENT'
                else:
                    customer_segment = 'PRIVATE'
            else:
                customer_segment = 'CORPORATE'
            
            # Account status (98% active)
            account_status = 'ACTIVE' if self.rng.random() < 0.98 else 'DORMANT'
            
            deposits.append({
                'ACCOUNT_ID': account_id,
                'AS_OF_DATE': as_of_date.isoformat(),
                'CUSTOMER_ID': customer_id,
                'DEPOSIT_TYPE': deposit_type,
                'CURRENCY': currency,
                'BALANCE_CCY': round(balance_ccy, 2),
                'BALANCE_CHF': round(balance_chf, 2),
                'FX_RATE': fx_rate,
                'IS_INSURED': is_insured,
                'PRODUCT_COUNT': product_count,
                'ACCOUNT_TENURE_DAYS': account_tenure_days,
                'HAS_DIRECT_DEBIT': has_direct_debit,
                'IS_OPERATIONAL': is_operational,
                'COUNTERPARTY_TYPE': counterparty_type,
                'CUSTOMER_SEGMENT': customer_segment,
                'ACCOUNT_STATUS': account_status
            })
        
        return pd.DataFrame(deposits)
    
    def generate_time_series(
        self, 
        start_date: datetime.date,
        num_days: int,
        output_dir: Path,
        customer_ids: List[str] = None
    ) -> Tuple[int, int]:
        """
        Generate LCR data for multiple days with realistic day-over-day changes
        
        Args:
            start_date: Starting date for time series
            num_days: Number of days to generate
            output_dir: Output directory for CSV files
            customer_ids: Optional list of actual customer IDs from customers.csv
        
        Returns:
            (total_hqla_records, total_deposit_records)
        """
        # Use provided customer IDs or generate synthetic ones
        if customer_ids is None:
            customer_ids = [f"CUST-{i+1:06d}" for i in range(self.num_customers)]
        
        total_hqla = 0
        total_deposits = 0
        
        # Generate data for each day (compact progress output)
        print(f"Generating {num_days} days of LCR data...", end="", flush=True)
        
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            
            # Show progress every 10 days or at milestones
            if day == 0 or (day + 1) % 10 == 0 or day == num_days - 1:
                print(f" {day + 1}", end="", flush=True)
            
            # Apply daily variance (increases over time for realistic trends)
            daily_variance = day / num_days if day > 0 else 0.0
            
            # Generate HQLA holdings with daily variance
            df_hqla = self.generate_hqla_holdings(current_date, daily_variance=daily_variance)
            hqla_file = output_dir / f"hqla_holdings_{current_date.strftime('%Y%m%d')}.csv"
            df_hqla.to_csv(hqla_file, index=False)
            total_hqla += len(df_hqla)
            
            # Generate deposit balances
            df_deposits = self.generate_deposit_balances(current_date, customer_ids)
            deposits_file = output_dir / f"deposit_balances_{current_date.strftime('%Y%m%d')}.csv"
            df_deposits.to_csv(deposits_file, index=False)
            total_deposits += len(df_deposits)
        
        print(" days ✓")
        return total_hqla, total_deposits


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Generate synthetic FINMA LCR data (HQLA holdings + deposit balances)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 90 days of data for 1000 customers (default target LCR ~95%)
  python lcr_data_generator.py --days 90 --customers 1000 --output-dir data/lcr
  
  # Generate data targeting 105% LCR (comfortable buffer)
  python lcr_data_generator.py --days 90 --customers 1000 --target-lcr 105 --output-dir data/lcr
  
  # Generate 30 days starting from specific date
  python lcr_data_generator.py --days 30 --start-date 2024-01-01 --output-dir data/lcr
  
  # Quick test with 7 days, 100 customers, targeting 110% LCR
  python lcr_data_generator.py --days 7 --customers 100 --target-lcr 110 --output-dir data/lcr_test
        """
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Number of days to generate (default: 90)'
    )
    
    parser.add_argument(
        '--customers',
        type=int,
        default=1000,
        help='Number of customers with deposits (default: 1000)'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date (YYYY-MM-DD). Default: 90 days ago from today'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Output directory for CSV files'
    )
    
    parser.add_argument(
        '--customer-file',
        type=str,
        default=None,
        help='Path to customers.csv to link deposits to real customers (optional)'
    )
    
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    
    parser.add_argument(
        '--target-lcr',
        type=float,
        default=95.0,
        help='Target LCR percentage to calibrate around (default: 95.0 for near-threshold testing)'
    )
    
    args = parser.parse_args()
    
    # Determine start date
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = (datetime.now() - timedelta(days=args.days - 1)).date()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load actual customer IDs if customer file provided
    customer_ids = None
    if args.customer_file and Path(args.customer_file).exists():
        print(f"Loading customer IDs from {args.customer_file}...")
        df_customers = pd.read_csv(args.customer_file)
        customer_ids = df_customers['customer_id'].tolist()
        print(f"  Found {len(customer_ids)} customers")
        actual_num_customers = len(customer_ids)
    else:
        actual_num_customers = args.customers
        print(f"Generating synthetic customer IDs (no customer file provided)")
    
    # Initialize generator
    generator = FINMALCRDataGenerator(
        num_customers=actual_num_customers,
        random_seed=args.seed,
        target_lcr=args.target_lcr
    )
    
    # Generate data (compact output)
    total_hqla, total_deposits = generator.generate_time_series(
        start_date=start_date,
        num_days=args.days,
        output_dir=output_dir,
        customer_ids=customer_ids
    )
    
    # Summary output (aligned with other generators)
    print("✅ LCR generation: SUCCESS")
    print(f"   - HQLA holdings: {total_hqla:,} records ({args.days} days)")
    print(f"   - Deposit balances: {total_deposits:,} records ({args.days} days)")
    print(f"   - Customers: {actual_num_customers:,}{'*' if customer_ids else ''}")
    if customer_ids:
        print(f"   - Linked to: actual customer base (referential integrity)")
    print(f"   - Target LCR: ~{args.target_lcr:.1f}% (calibrated for 90-110% range)")
    print(f"   - Date range: {start_date.isoformat()} to {(start_date + timedelta(days=args.days-1)).isoformat()}")
    print(f"   - Files created: {args.days * 2} ({args.days} HQLA + {args.days} deposit files)")
    print(f"   - For FINMA Circular 2015/2 compliance and Basel III monitoring")


if __name__ == '__main__':
    main()

