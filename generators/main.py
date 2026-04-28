#!/usr/bin/env python3
"""
Synthetic banking Data Generator - Summary Report

This tool generates realistic payment transaction data for Client Due Diligence (CDD) 
scenarios, including configurable anomalies to simulate potential financial crime.

Usage:
    python main.py [options]

Example:
    python main.py --customers 50 --anomaly-rate 5.0 --output-dir ./output
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from config import GeneratorConfig
from file_generator import FileGenerator
from swift_generator import SWIFTGenerator
from pep_generator import PEPGenerator
from mortgage_email_generator import MortgageEmailGenerator
from address_update_generator import AddressUpdateGenerator
from customer_update_generator import CustomerUpdateGenerator
from fixed_income_generator import FixedIncomeTradeGenerator
from commodity_generator import CommodityTradeGenerator
from customer_lifecycle_generator import CustomerLifecycleGenerator
from employee_generator import EmployeeGenerator


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Generate CDD payment statements with configurable anomalies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Generate default dataset (10 customers, 2% anomalies):
    python main.py

  Generate larger dataset with more anomalies:
    python main.py --customers 100 --anomaly-rate 5.0

  Generate with SWIFT ISO20022 messages:
    python main.py --customers 50 --generate-swift

  Custom SWIFT settings:
    python main.py --customers 100 --generate-swift --swift-percentage 25 --swift-avg-messages 2.0

  Custom period and output directory:
    python main.py --customers 25 --period 12 --output-dir ./custom_data

  Clean previous output before generating:
    python main.py --clean --customers 50

  Complete generation with SWIFT (recommended):
    python main.py --customers 100 --period 12 --generate-swift --anomaly-rate 5.0

  Generate all artifacts (banking data, SWIFT, PEP, mortgage emails, address updates):
    python main.py --customers 50 --generate-swift --generate-pep --generate-mortgage-emails --generate-address-updates --clean

  Generate complete dataset with FRTB market risk data:
    python main.py --customers 100 --generate-swift --generate-pep --generate-fixed-income --generate-commodities --clean

  Generate complete banking dataset with all features:
    python main.py --customers 100 --generate-swift --generate-pep --generate-address-updates --generate-lifecycle --clean

  Generate PEP data only:
    python main.py --generate-pep --pep-records 100

  Generate mortgage emails only:
    python main.py --generate-mortgage-emails --mortgage-customers 5

  Generate address updates for SCD Type 2:
    python main.py --generate-address-updates --address-update-files 8

  Generate fixed income trades (bonds & swaps):
    python main.py --generate-fixed-income --fixed-income-trades 1000 --bond-swap-ratio 0.7

  Generate commodity trades:
    python main.py --generate-commodities --commodity-trades 500
        """
    )
    
    parser.add_argument(
        "--customers", "-c",
        type=int,
        default=10,
        help="Number of customers to generate (default: 10)"
    )
    
    parser.add_argument(
        "--anomaly-rate", "-a",
        type=float,
        default=2.0,
        help="Percentage of customers with anomalies (default: 2.0)"
    )
    
    parser.add_argument(
        "--period", "-p",
        type=int,
        default=24,
        help="Generation period in months (default: 24)"
    )
    
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic data generation (default: 42). Use same seed for reproducible output."
    )
    
    parser.add_argument(
        "--transactions-per-month", "-t",
        type=float,
        default=3.5,
        help="Average transactions per customer per month (default: 3.5)"
    )
    
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="generated_data",
        help="Output directory for generated files (default: generated_data)"
    )
    
    parser.add_argument(
        "--start-date", "-s",
        type=str,
        help="Start date for generation period (YYYY-MM-DD format). If not provided, calculated from current date minus period."
    )
    
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean output directory before generating new files"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--min-amount",
        type=float,
        default=10.0,
        help="Minimum transaction amount (default: 10.0)"
    )
    
    parser.add_argument(
        "--max-amount",
        type=float,
        default=50000.0,
        help="Maximum transaction amount (default: 50000.0)"
    )
    
    # SWIFT message generation options
    parser.add_argument(
        "--generate-swift",
        action="store_true",
        help="Generate SWIFT ISO20022 messages for customers"
    )
    
    parser.add_argument(
        "--swift-percentage",
        type=float,
        default=30.0,
        help="Percentage of customers to generate SWIFT messages for (default: 30.0)"
    )
    
    parser.add_argument(
        "--swift-avg-messages",
        type=float,
        default=1.2,
        help="Average SWIFT messages per selected customer (default: 1.2)"
    )
    
    parser.add_argument(
        "--swift-workers",
        type=int,
        default=4,
        help="Number of parallel workers for SWIFT generation (default: 4)"
    )
    
    parser.add_argument(
        "--swift-generator-script",
        type=str,
        default="swift_message_generator.py",
        help="Path to SWIFT message generator script (default: swift_message_generator.py)"
    )
    
    parser.add_argument(
        "--swift-generator-dir",
        type=str,
        default=".",
        help="Directory containing SWIFT generator script (default: current directory)"
    )
    
    parser.add_argument(
        "--swift-output-dir",
        type=str,
        help="Output directory for SWIFT XML files (default: {output_dir}/swift_messages)"
    )
    
    # PEP generation options
    parser.add_argument(
        "--generate-pep",
        action="store_true",
        help="Generate PEP (Politically Exposed Persons) data"
    )
    
    parser.add_argument(
        "--pep-records",
        type=int,
        default=50,
        help="Number of PEP records to generate (default: 50)"
    )
    
    # Mortgage email generation options
    parser.add_argument(
        "--generate-mortgage-emails",
        action="store_true",
        help="Generate mortgage request emails"
    )
    
    parser.add_argument(
        "--mortgage-customers",
        type=int,
        default=3,
        help="Number of customers to generate mortgage emails for (default: 3)"
    )
    
    # Address update generation options
    parser.add_argument(
        "--generate-address-updates",
        action="store_true",
        help="Generate address update files for SCD Type 2 processing"
    )
    
    parser.add_argument(
        "--address-update-files",
        type=int,
        default=6,
        help="Number of address update files to generate (default: 6)"
    )
    
    parser.add_argument(
        "--updates-per-file",
        type=int,
        help="Number of address updates per file (default: 5-15%% of customers)"
    )
    
    # Fixed income generation options
    parser.add_argument(
        "--generate-fixed-income",
        action="store_true",
        help="Generate fixed income trades (bonds and swaps)"
    )
    
    parser.add_argument(
        "--fixed-income-trades",
        type=int,
        default=1000,
        help="Number of fixed income trades to generate (default: 1000)"
    )
    
    parser.add_argument(
        "--bond-swap-ratio",
        type=float,
        default=0.7,
        help="Ratio of bonds to swaps (default: 0.7 = 70%% bonds, 30%% swaps)"
    )
    
    # Commodity generation options
    parser.add_argument(
        "--generate-commodities",
        action="store_true",
        help="Generate commodity trades (energy, metals, agricultural)"
    )
    
    parser.add_argument(
        "--commodity-trades",
        type=int,
        default=500,
        help="Number of commodity trades to generate (default: 500)"
    )
    
    # Customer lifecycle event generation options
    parser.add_argument(
        "--generate-lifecycle",
        action="store_true",
        help="Generate customer lifecycle events and status history"
    )
    
    # Employee hierarchy generation options
    parser.add_argument(
        "--generate-employees",
        action="store_true",
        help="Generate employee hierarchy (client advisors, team leaders, super team leaders)"
    )
    
    # Customer update generation options
    parser.add_argument(
        "--generate-customer-updates",
        action="store_true",
        help="Generate customer update files for SCD Type 2 processing"
    )
    
    parser.add_argument(
        "--customer-update-files",
        type=int,
        default=8,
        help="Number of customer update files to generate (default: 8)"
    )
    
    parser.add_argument(
        "--generate-customer-snapshot",
        action="store_true",
        help="Generate initial customer snapshot with extended attributes"
    )
    
    return parser.parse_args()


def validate_arguments(args):
    """Validate command line arguments"""
    errors = []
    
    if args.customers <= 0:
        errors.append("Number of customers must be positive")
    
    if not (0 <= args.anomaly_rate <= 100):
        errors.append("Anomaly rate must be between 0 and 100")
    
    if args.period <= 0:
        errors.append("Period must be positive")
    
    if args.transactions_per_month <= 0:
        errors.append("Transactions per month must be positive")
    
    if args.min_amount <= 0:
        errors.append("Minimum amount must be positive")
    
    if args.max_amount <= args.min_amount:
        errors.append("Maximum amount must be greater than minimum amount")
    
    if args.start_date:
        try:
            datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            errors.append("Start date must be in YYYY-MM-DD format")
    
    # SWIFT-specific validations
    if args.generate_swift:
        if not (0 <= args.swift_percentage <= 100):
            errors.append("SWIFT percentage must be between 0 and 100")
        
        if args.swift_avg_messages <= 0:
            errors.append("SWIFT average messages must be positive")
        
        if args.swift_workers <= 0:
            errors.append("SWIFT workers must be positive")
    
    if errors:
        print("Error: Invalid arguments:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)


def create_config(args) -> GeneratorConfig:
    """Create configuration from command line arguments"""
    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    
    return GeneratorConfig(
        random_seed=args.seed,
        num_customers=args.customers,
        anomaly_percentage=args.anomaly_rate,
        generation_period_months=args.period,
        start_date=start_date,
        avg_transactions_per_customer_per_month=args.transactions_per_month,
        min_transaction_amount=args.min_amount,
        max_transaction_amount=args.max_amount,
        output_directory=args.output_dir
    )


def print_banner():
    """Print application banner"""
    print("=" * 60)
    print("Synthetic banking Data Generator")
    print("=" * 60)


def main():
    """Main application entry point"""
    try:
        print_banner()
        
        # Parse and validate arguments
        args = parse_arguments()
        validate_arguments(args)
        
        # Create configuration
        config = create_config(args)
        
        if args.verbose:
            print("\nConfiguration:")
            print(f"  Customers: {config.num_customers}")
            print(f"  Anomaly rate: {config.anomaly_percentage}%")
            print(f"  Period: {config.generation_period_months} months")
            print(f"  Start date: {config.start_date.strftime('%Y-%m-%d')}")
            print(f"  End date: {config.end_date.strftime('%Y-%m-%d')}")
            print(f"  Transactions/customer/month: {config.avg_transactions_per_customer_per_month}")
            print(f"  Amount range: ${config.min_transaction_amount} - ${config.max_transaction_amount}")
            print(f"  Output directory: {config.output_directory}")
        
        # Initialize file generator
        file_generator = FileGenerator(config)
        
        # Clean output directory if requested
        if args.clean:
            print("\nCleaning output directory...")
            file_generator.clean_output_directory()
        
        # Generate files
        print("\nStarting data generation...")
        start_time = datetime.now()
        
        # Check if user specified any specific generation flags (exclusive mode)
        specific_generation_requested = any([
            args.generate_swift,
            args.generate_pep,
            args.generate_mortgage_emails,
            args.generate_address_updates,
            args.generate_customer_updates,
            args.generate_customer_snapshot,
            args.generate_fixed_income,
            args.generate_commodities,
            args.generate_lifecycle,
            args.generate_employees
        ])
        
        # Generate basic files only if no specific generation was requested
        # OR if specific generators need customer/account data as dependencies
        if not specific_generation_requested:
            # Default mode: generate all basic files
            print("\nGenerating basic files...")
            results = file_generator.generate_all_files()
        else:
            # Exclusive mode: generate only customer master data (required dependency)
            print("\n⚙️  Specific generation mode: generating ONLY requested entities")
            print("   (Minimal customer data will be generated as dependency)")
            results = file_generator.generate_minimal_files()
        
        # Generate SWIFT messages if requested
        swift_results = None
        if args.generate_swift:
            try:
                print("\n" + "=" * 80)
                print("SWIFT MESSAGE GENERATION")
                print("=" * 80)
                
                # Determine SWIFT output directory
                swift_output_dir = args.swift_output_dir
                if not swift_output_dir:
                    swift_output_dir = str(Path(config.output_directory) / "swift_messages")
                
                # Initialize SWIFT generator
                swift_generator = SWIFTGenerator(args.swift_generator_script, seed=config.random_seed)
                
                # Generate SWIFT messages
                customer_file_path = str(Path(config.output_directory) / "master_data" / "customers.csv")
                swift_results = swift_generator.generate_swift_messages(
                    customer_file_path=customer_file_path,
                    output_dir=swift_output_dir,
                    customer_percentage=args.swift_percentage,
                    avg_messages=args.swift_avg_messages,
                    max_workers=args.swift_workers,
                    swift_generator_dir=args.swift_generator_dir
                )
                
                # Save SWIFT summary
                swift_summary_file = Path(swift_output_dir) / "swift_synthetic_summary.json"
                with open(swift_summary_file, "w") as f:
                    import json
                    json.dump(swift_results['summary'], f, indent=2)
                
                print(f"📋 SWIFT Summary saved: {swift_summary_file}")
                
            except Exception as e:
                print(f"\n❌ SWIFT generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate PEP data if requested
        pep_results = None
        if args.generate_pep:
            try:
                print("\n" + "=" * 80)
                print("PEP (POLITICALLY EXPOSED PERSONS) DATA GENERATION")
                print("=" * 80)
                
                # Initialize PEP generator with customer file
                customer_file_path = str(Path(config.output_directory) / "master_data" / "customers.csv")
                pep_generator = PEPGenerator(customer_file=customer_file_path, seed=config.random_seed)
                
                # Generate PEP data
                pep_records = pep_generator.generate_pep_data(args.pep_records)
                
                # Save to CSV
                pep_output_file = str(Path(config.output_directory) / "master_data" / "pep_data.csv")
                pep_generator.save_to_csv(pep_records, pep_output_file)
                
                # Create results summary
                pep_results = {
                    'total_records': len(pep_records),
                    'output_file': pep_output_file,
                    'categories': {},
                    'risk_levels': {},
                    'statuses': {}
                }
                
                # Calculate statistics
                for record in pep_records:
                    pep_results['categories'][record.pep_category] = pep_results['categories'].get(record.pep_category, 0) + 1
                    pep_results['risk_levels'][record.risk_level] = pep_results['risk_levels'].get(record.risk_level, 0) + 1
                    pep_results['statuses'][record.status] = pep_results['statuses'].get(record.status, 0) + 1
                
                print(f"✅ Generated {pep_results['total_records']} PEP records")
                print(f"📁 PEP file: {pep_output_file}")
                
            except Exception as e:
                print(f"\n❌ PEP generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate mortgage emails if requested
        mortgage_results = None
        if args.generate_mortgage_emails:
            try:
                print("\n" + "=" * 80)
                print("MORTGAGE EMAIL GENERATION")
                print("=" * 80)
                
                # Initialize mortgage email generator
                email_output_dir = str(Path(config.output_directory) / "emails")
                
                mortgage_generator = MortgageEmailGenerator(output_dir=email_output_dir)
                
                # Generate mortgage emails
                customer_file_path = str(Path(config.output_directory) / "master_data" / "customers.csv")
                address_file_path = str(Path(config.output_directory) / "master_data" / "customer_addresses.csv")
                mortgage_generator.generate_mortgage_emails(
                    customer_file=customer_file_path,
                    address_file=address_file_path,
                    num_customers=args.mortgage_customers
                )
                
                # Create results summary
                mortgage_results = {
                    'customers': args.mortgage_customers,
                    'total_emails': args.mortgage_customers * 3,  # 3 email types per customer
                    'output_dir': email_output_dir
                }
                
                print(f"✅ Generated mortgage emails for {mortgage_results['customers']} customers")
                print(f"📧 Total emails: {mortgage_results['total_emails']} (3 types per customer)")
                print(f"📁 Email directory: {email_output_dir}")
                
            except Exception as e:
                print(f"\n❌ Mortgage email generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate address updates if requested
        address_update_results = None
        if args.generate_address_updates:
            try:
                print("\n" + "=" * 80)
                print("ADDRESS UPDATE GENERATION FOR SCD TYPE 2")
                print("=" * 80)
                
                # Initialize address update generator
                customer_file_path = str(Path(config.output_directory) / "master_data" / "customers.csv")
                output_dir = str(Path(config.output_directory) / "master_data")
                
                address_generator = AddressUpdateGenerator(customer_file_path, output_dir, seed=config.random_seed)
                generated_files = address_generator.generate_address_updates(
                    num_update_files=args.address_update_files,
                    updates_per_file=args.updates_per_file
                )
                
                address_update_results = {
                    'files_generated': len(generated_files),
                    'file_paths': generated_files
                }
                
                print(f"✅ Generated {len(generated_files)} address update files")
                print(f"📁 Address updates directory: {Path(output_dir) / 'address_updates'}")
                
            except Exception as e:
                print(f"\n❌ Address update generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate customer updates if requested
        customer_update_results = None
        if args.generate_customer_updates or args.generate_customer_snapshot:
            try:
                print("\n" + "=" * 80)
                print("CUSTOMER UPDATE GENERATION FOR SCD TYPE 2")
                print("=" * 80)
                
                # Initialize customer update generator
                customer_file_path = str(Path(config.output_directory) / "master_data" / "customers.csv")
                output_dir = str(Path(config.output_directory) / "master_data")
                
                customer_update_generator = CustomerUpdateGenerator(customer_file_path, output_dir, seed=config.random_seed)
                
                generated_files = []
                
                # Generate update files if requested
                if args.generate_customer_updates or args.generate_customer_snapshot:
                    generated_files = customer_update_generator.generate_customer_updates(
                        num_update_files=args.customer_update_files
                    )
                    print(f"✅ Generated {len(generated_files)} customer update files")
                
                customer_update_results = {
                    'update_files_generated': len(generated_files),
                    'update_file_paths': generated_files
                }
                
                print(f"📁 Customer updates directory: {Path(output_dir) / 'customer_updates'}")
                
            except Exception as e:
                print(f"\n❌ Customer update generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate fixed income trades if requested
        fixed_income_results = None
        if args.generate_fixed_income:
            try:
                print("\n" + "=" * 80)
                print("FIXED INCOME TRADE GENERATION (BONDS & SWAPS)")
                print("=" * 80)
                
                # Load customer IDs from results
                customer_ids = [f"CUST_{str(i+1).zfill(5)}" for i in range(results['total_customers'])]
                
                # Load account data from results (use investment accounts for fixed income)
                import csv
                account_file_path = Path(config.output_directory) / "master_data" / "accounts.csv"
                investment_accounts = []
                with open(account_file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row['account_type'] == 'INVESTMENT':
                            investment_accounts.append({
                                'account_id': row['account_id'],
                                'customer_id': row['customer_id'],
                                'base_currency': row['base_currency']
                            })
                
                if not investment_accounts:
                    print("⚠️  No investment accounts found. Skipping fixed income generation.")
                    print("   Tip: Increase --customers to get more investment accounts.")
                    raise ValueError("No investment accounts available for fixed income trading")
                
                # Build FX rates dictionary from all date files
                fx_rates_dict = {'CHF': 1.0}  # Base currency
                fx_rates_dir = Path(config.output_directory) / "fx_rates"
                
                # Read all FX rate files (fx_rates_YYYY-MM-DD.csv)
                fx_files = sorted(fx_rates_dir.glob("fx_rates_*.csv"))
                if not fx_files:
                    # Fallback to old single file format if it exists
                    old_fx_file = fx_rates_dir / "fx_rates.csv"
                    if old_fx_file.exists():
                        fx_files = [old_fx_file]
                
                for fx_file in fx_files:
                    with open(fx_file, 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            if row['to_currency'] == 'CHF':
                                # Keep the most recent rate for each currency
                                fx_rates_dict[row['from_currency']] = float(row['mid_rate'])
                
                # Initialize fixed income generator
                fi_generator = FixedIncomeTradeGenerator(
                    config=config,
                    customers=customer_ids,
                    accounts=investment_accounts,
                    fx_rates=fx_rates_dict,
                    start_date=config.start_date.date(),
                    end_date=config.end_date.date()
                )
                
                # Generate trades
                fi_trades = fi_generator.generate_trades(
                    num_trades=args.fixed_income_trades,
                    bond_swap_ratio=args.bond_swap_ratio
                )
                
                # Save to CSV
                fi_output_dir = Path(config.output_directory) / "fixed_income_trades"
                fi_output_dir.mkdir(parents=True, exist_ok=True)
                
                # Save to separate files by date
                files_created = fi_generator.save_to_csv_by_date(fi_trades, fi_output_dir)
                
                # Calculate statistics
                bond_count = sum(1 for t in fi_trades if t.instrument_type == 'BOND')
                swap_count = sum(1 for t in fi_trades if t.instrument_type == 'IRS')
                total_notional = sum(t.base_gross_amount for t in fi_trades)
                
                fixed_income_results = {
                    'total_trades': len(fi_trades),
                    'bonds': bond_count,
                    'swaps': swap_count,
                    'total_notional_chf': total_notional,
                    'output_dir': str(fi_output_dir),
                    'files_created': len(files_created)
                }
                
                print(f"✅ Generated {len(fi_trades)} fixed income trades")
                print(f"   - Bonds: {bond_count}")
                print(f"   - Interest Rate Swaps: {swap_count}")
                print(f"   - Total Notional: CHF {total_notional:,.2f}")
                print(f"📁 Fixed income directory: {fi_output_dir}")
                print(f"📄 Files created: {len(files_created)}")
                
            except Exception as e:
                print(f"\n❌ Fixed income generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate commodity trades if requested
        commodity_results = None
        if args.generate_commodities:
            try:
                print("\n" + "=" * 80)
                print("COMMODITY TRADE GENERATION (ENERGY, METALS, AGRICULTURAL)")
                print("=" * 80)
                
                # Load customer IDs from results
                customer_ids = [f"CUST_{str(i+1).zfill(5)}" for i in range(results['total_customers'])]
                
                # Load account data from results (use investment accounts for commodities)
                import csv
                account_file_path = Path(config.output_directory) / "master_data" / "accounts.csv"
                investment_accounts = []
                with open(account_file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row['account_type'] == 'INVESTMENT':
                            investment_accounts.append({
                                'account_id': row['account_id'],
                                'customer_id': row['customer_id'],
                                'base_currency': row['base_currency']
                            })
                
                if not investment_accounts:
                    print("⚠️  No investment accounts found. Skipping commodity generation.")
                    print("   Tip: Increase --customers to get more investment accounts.")
                    raise ValueError("No investment accounts available for commodity trading")
                
                # Build FX rates dictionary from all date files
                fx_rates_dict = {'CHF': 1.0}  # Base currency
                fx_rates_dir = Path(config.output_directory) / "fx_rates"
                
                # Read all FX rate files (fx_rates_YYYY-MM-DD.csv)
                fx_files = sorted(fx_rates_dir.glob("fx_rates_*.csv"))
                if not fx_files:
                    # Fallback to old single file format if it exists
                    old_fx_file = fx_rates_dir / "fx_rates.csv"
                    if old_fx_file.exists():
                        fx_files = [old_fx_file]
                
                for fx_file in fx_files:
                    with open(fx_file, 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            if row['to_currency'] == 'CHF':
                                # Keep the most recent rate for each currency
                                fx_rates_dict[row['from_currency']] = float(row['mid_rate'])
                
                # Initialize commodity generator
                commodity_generator = CommodityTradeGenerator(
                    config=config,
                    customers=customer_ids,
                    accounts=investment_accounts,
                    fx_rates=fx_rates_dict,
                    start_date=config.start_date.date(),
                    end_date=config.end_date.date()
                )
                
                # Generate trades
                commodity_trades = commodity_generator.generate_trades(num_trades=args.commodity_trades)
                
                # Save to CSV
                commodity_output_dir = Path(config.output_directory) / "commodity_trades"
                commodity_output_dir.mkdir(parents=True, exist_ok=True)
                
                # Save to separate files by date
                files_created = commodity_generator.save_to_csv_by_date(commodity_trades, commodity_output_dir)
                
                # Calculate statistics
                commodity_types = {}
                for trade in commodity_trades:
                    commodity_types[trade.commodity_type] = commodity_types.get(trade.commodity_type, 0) + 1
                
                total_value = sum(abs(t.base_gross_amount) for t in commodity_trades)
                
                commodity_results = {
                    'total_trades': len(commodity_trades),
                    'commodity_types': commodity_types,
                    'total_value_chf': total_value,
                    'output_dir': str(commodity_output_dir),
                    'files_created': len(files_created)
                }
                
                print(f"✅ Generated {len(commodity_trades)} commodity trades")
                for ctype, count in commodity_types.items():
                    print(f"   - {ctype}: {count}")
                print(f"   - Total Value: CHF {total_value:,.2f}")
                print(f"📁 Commodity directory: {commodity_output_dir}")
                print(f"📄 Files created: {len(files_created)}")
                
            except Exception as e:
                print(f"\n❌ Commodity generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Generate customer lifecycle events if requested
        lifecycle_results = None
        if args.generate_lifecycle:
            try:
                print("\n" + "=" * 80)
                print("CUSTOMER LIFECYCLE EVENT GENERATION")
                print("=" * 80)
                
                # Initialize lifecycle event generator
                customer_file_path = str(Path(config.output_directory) / "master_data" / "customers.csv")
                address_updates_dir = str(Path(config.output_directory) / "master_data" / "address_updates")
                customer_updates_dir = str(Path(config.output_directory) / "master_data" / "customer_updates")
                output_dir = str(Path(config.output_directory) / "master_data")
                
                # Check if customer updates directory exists, otherwise set to None
                if not Path(customer_updates_dir).exists():
                    print(f"⚠️  Customer updates directory not found, will use only address updates and random events")
                    customer_updates_dir = None
                
                lifecycle_generator = CustomerLifecycleGenerator(
                    customer_file=customer_file_path,
                    address_updates_dir=address_updates_dir,
                    output_dir=output_dir,
                    customer_updates_dir=customer_updates_dir,
                    seed=config.random_seed
                )
                
                # Generate all lifecycle events
                lifecycle_generator.generate_all()
                
                # Create results summary
                lifecycle_results = {
                    'output_dir': output_dir,
                    'events_dir': str(Path(output_dir) / 'customer_events'),
                    'status_file': str(Path(output_dir) / 'customer_status.csv')
                }
                
                print(f"✅ Customer lifecycle events generated successfully")
                print(f"📁 Events directory: {lifecycle_results['events_dir']}")
                print(f"📁 Status file: {lifecycle_results['status_file']}")
                
            except Exception as e:
                print(f"\n❌ Customer lifecycle generation failed: {str(e)}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        # Collect additional results for summary
        additional_results = {
            'swift': swift_results,
            'pep': pep_results,
            'mortgage': mortgage_results,
            'address_updates': address_update_results,
            'customer_updates': customer_update_results,
            'fixed_income': fixed_income_results,
            'commodity': commodity_results,
            'lifecycle': lifecycle_results
        }
        
        # Update summary report with additional results
        if additional_results and any(additional_results.values()):
            file_generator.update_summary_with_additional_results(additional_results)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Final summary
        print("\n" + "=" * 80)
        print("🎉 COMPLETE GENERATION SUMMARY")
        print("=" * 80)
        
        # Handle both full and minimal generation modes
        if results.get('minimal_mode', False):
            print(f"✅ Minimal customer data generation: SUCCESS")
            print(f"   - Customers: {results['total_customers']} ({results['anomalous_customers']} anomalous)")
            print(f"   - Mode: Exclusive generation (only requested entities)")
        else:
            print(f"✅ Banking data generation: SUCCESS")
            print(f"   - Customers: {results['total_customers']} ({results['anomalous_customers']} anomalous)")
            print(f"   - Transactions: {results['total_transactions']}")
            print(f"   - Files: {results['daily_file_count']} daily files + 5 master files")
        
        if args.generate_swift and swift_results:
            print(f"✅ SWIFT message generation: SUCCESS")
            print(f"   - SWIFT customers: {swift_results['summary']['configuration']['swift_customers']}")
            print(f"   - Message pairs: {swift_results['successful_pairs']}")
            print(f"   - XML files: {swift_results['successful_pairs'] * 2}")
            print(f"   - Transaction volume: €{swift_results['total_volume']:,.2f}")
            print(f"   - Anomaly customers with SWIFT: {swift_results['summary']['generation_stats']['anomaly_customers_with_swift']}")
        elif args.generate_swift:
            print(f"❌ SWIFT message generation: FAILED")
        else:
            print(f"⏭️  SWIFT message generation: SKIPPED (use --generate-swift to enable)")
        
        if args.generate_pep and pep_results:
            print(f"✅ PEP data generation: SUCCESS")
            print(f"   - PEP records: {pep_results['total_records']}")
            print(f"   - Risk levels: {', '.join([f'{k}:{v}' for k, v in pep_results['risk_levels'].items()])}")
            print(f"   - Categories: {', '.join([f'{k}:{v}' for k, v in pep_results['categories'].items()])}")
        elif args.generate_pep:
            print(f"❌ PEP data generation: FAILED")
        else:
            print(f"⏭️  PEP data generation: SKIPPED (use --generate-pep to enable)")
        
        if args.generate_mortgage_emails and mortgage_results:
            print(f"✅ Mortgage email generation: SUCCESS")
            print(f"   - Customers: {mortgage_results['customers']}")
            print(f"   - Total emails: {mortgage_results['total_emails']} (3 types per customer)")
        elif args.generate_mortgage_emails:
            print(f"❌ Mortgage email generation: FAILED")
        else:
            print(f"⏭️  Mortgage email generation: SKIPPED (use --generate-mortgage-emails to enable)")
        
        if args.generate_address_updates and address_update_results:
            print(f"✅ Address update generation: SUCCESS")
            print(f"   - Update files: {address_update_results['files_generated']}")
            print(f"   - For SCD Type 2 processing")
        elif args.generate_address_updates:
            print(f"❌ Address update generation: FAILED")
        else:
            print(f"⏭️  Address update generation: SKIPPED (use --generate-address-updates to enable)")
        
        if (args.generate_customer_updates or args.generate_customer_snapshot) and customer_update_results:
            print(f"✅ Customer update generation: SUCCESS")
            if customer_update_results.get('snapshot_file'):
                print(f"   - Initial snapshot: customer_snapshot_initial.csv")
            if customer_update_results.get('update_files_generated', 0) > 0:
                print(f"   - Update files: {customer_update_results['update_files_generated']}")
            print(f"   - For SCD Type 2 processing")
        elif args.generate_customer_updates or args.generate_customer_snapshot:
            print(f"❌ Customer update generation: FAILED")
        else:
            print(f"⏭️  Customer update generation: SKIPPED (use --generate-customer-updates to enable)")
        
        if args.generate_fixed_income and fixed_income_results:
            print(f"✅ Fixed income generation: SUCCESS")
            print(f"   - Total trades: {fixed_income_results['total_trades']}")
            print(f"   - Bonds: {fixed_income_results['bonds']}, Swaps: {fixed_income_results['swaps']}")
            print(f"   - Total Notional: CHF {fixed_income_results['total_notional_chf']:,.2f}")
            print(f"   - Files created: {fixed_income_results['files_created']} (one per trade date)")
        elif args.generate_fixed_income:
            print(f"❌ Fixed income generation: FAILED")
        else:
            print(f"⏭️  Fixed income generation: SKIPPED (use --generate-fixed-income to enable)")
        
        if args.generate_commodities and commodity_results:
            print(f"✅ Commodity generation: SUCCESS")
            print(f"   - Total trades: {commodity_results['total_trades']}")
            commodity_summary = ', '.join([f"{k}:{v}" for k, v in commodity_results['commodity_types'].items()])
            print(f"   - Types: {commodity_summary}")
            print(f"   - Total Value: CHF {commodity_results['total_value_chf']:,.2f}")
            print(f"   - Files created: {commodity_results['files_created']} (one per trade date)")
        elif args.generate_commodities:
            print(f"❌ Commodity generation: FAILED")
        else:
            print(f"⏭️  Commodity generation: SKIPPED (use --generate-commodities to enable)")
        
        if args.generate_lifecycle and lifecycle_results:
            print(f"✅ Customer lifecycle generation: SUCCESS")
            print(f"   - Events: customer_events/ (date-based files)")
            print(f"   - Status file: customer_status.csv")
            print(f"   - For churn prediction and lifecycle analytics")
        elif args.generate_lifecycle:
            print(f"❌ Customer lifecycle generation: FAILED")
        else:
            print(f"⏭️  Customer lifecycle generation: SKIPPED (use --generate-lifecycle to enable)")
        
        print(f"\n📁 Output directory: {Path(config.output_directory).absolute()}")
        if args.generate_swift and swift_results:
            print(f"📁 SWIFT directory: {Path(swift_results['summary']['configuration']['output_directory']).absolute()}")
        if args.generate_pep and pep_results:
            print(f"📁 PEP file: {pep_results['output_file']}")
        if args.generate_mortgage_emails and mortgage_results:
            print(f"📁 Email directory: {mortgage_results['output_dir']}")
        if args.generate_fixed_income and fixed_income_results:
            print(f"📁 Fixed income directory: {fixed_income_results['output_dir']}")
        if args.generate_commodities and commodity_results:
            print(f"📁 Commodity directory: {commodity_results['output_dir']}")
        if args.generate_lifecycle and lifecycle_results:
            print(f"📁 Lifecycle files: {lifecycle_results['output_dir']}")
        
        print("\n💡 Next steps:")
        print("   1. Load CSV files into Snowflake using DDL scripts in ./structure/")
        print("   2. Use dynamic tables for real-time analytics")
        if args.generate_swift and swift_results:
            print("   3. Load SWIFT XML files into ICG schema for ISO20022 processing")
            print("   4. Correlate customer anomalies with SWIFT activity patterns")
        if args.generate_pep and pep_results:
            print("   5. Load PEP data into CRMI_PEP table for compliance screening")
            print("   6. Implement name matching algorithms for customer-PEP correlation")
        if args.generate_mortgage_emails and mortgage_results:
            print("   7. Use mortgage emails for loan processing workflow testing")
            print("   8. Integrate with CRM systems for customer communication")
        if args.generate_fixed_income and fixed_income_results:
            print("   9. Load fixed income trades into FII_RAW_001 schema for interest rate risk")
            print("   10. Calculate DV01 and duration metrics for FRTB capital requirements")
        if args.generate_commodities and commodity_results:
            print("   11. Load commodity trades into CMD_RAW_001 schema for commodity risk")
            print("   12. Implement FRTB Standardized Approach (SA) capital calculations")
        if args.generate_lifecycle and lifecycle_results:
            print("   13. Load lifecycle events into CRMI_CUSTOMER_EVENT for churn prediction")
            print("   14. Use CRMA_AGG_DT_CUSTOMER_LIFECYCLE for customer retention analytics")
            print("   15. Query REPP_AGG_DT_LIFECYCLE_ANOMALIES for AML correlation patterns")
        print("   16. Query aggregated data from REPP_AGG_001 schema")
        
        print("\n✅ Generation completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\n❌ Generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error during generation: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
