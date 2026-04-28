"""
File generation module for daily transaction files and customer data
"""
import os
import random
from datetime import datetime, timedelta
from typing import List
from pathlib import Path

from config import GeneratorConfig
from customer_generator import CustomerGenerator
from pay_transaction_generator import TransactionGenerator
from fx_generator import FXRateGenerator, AccountGenerator
from equity_generator import EquityTradeGenerator
from employee_generator import EmployeeGenerator


class FileGenerator:
    """Manages the generation of all output files"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.output_dir = Path(config.output_directory)
        
        # Define subdirectories
        self.master_data_dir = self.output_dir / "master_data"
        self.payment_transactions_dir = self.output_dir / "payment_transactions"
        self.equity_trades_dir = self.output_dir / "equity_trades"
        self.fx_rates_dir = self.output_dir / "fx_rates"
        self.reports_dir = self.output_dir / "reports"
    
    def _create_directory_structure(self):
        """Create organized directory structure for different data types"""
        directories = [
            self.output_dir,
            self.master_data_dir,
            self.payment_transactions_dir,
            self.equity_trades_dir,
            self.fx_rates_dir,
            self.reports_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Created directory structure:")
        print(f"   • {self.master_data_dir.name}/ - Customer and account master data")
        print(f"   • {self.payment_transactions_dir.name}/ - Daily payment transaction files")
        print(f"   • {self.equity_trades_dir.name}/ - Daily equity trade files")
        print(f"   • {self.fx_rates_dir.name}/ - Foreign exchange rate data")
        print(f"   • {self.reports_dir.name}/ - Summary reports and documentation")
        print()
        
    def generate_all_files(self, additional_results: dict = None) -> dict:
        """Generate all customer and transaction files"""
        # Create output directory structure
        self._create_directory_structure()
        
        print(f"Generating files in directory: {self.output_dir.absolute()}")
        print(f"Configuration: {self.config.num_customers} customers, {self.config.anomaly_percentage}% anomalous")
        print(f"Period: {self.config.start_date.strftime('%Y-%m-%d')} to {self.config.end_date.strftime('%Y-%m-%d')}")
        
        # Generate customers and addresses
        print("\nGenerating customer data...")
        customer_generator = CustomerGenerator(self.config)
        customers, customer_addresses = customer_generator.generate_customers()
        
        # Add fuzzy matching test customer for PEP screening testing
        print("Adding fuzzy matching test customer for PEP screening...")
        test_customer, test_address = customer_generator.add_fuzzy_matching_test_customer()
        print(f"Added test customer: {test_customer.first_name} {test_customer.family_name} (ID: {test_customer.customer_id})")
        
        # Save customer master data (including fuzzy matching test customer)
        customer_file = self.master_data_dir / "customers.csv"
        customer_generator.save_customers_to_csv(str(customer_file))
        
        # Save customer address data (SCD Type 2)
        address_file = self.master_data_dir / "customer_addresses.csv"
        customer_generator.save_addresses_to_csv(str(address_file))
        
        anomalous_customers = customer_generator.get_anomalous_customers()
        print(f"Generated {len(customers)} customers ({len(anomalous_customers)} anomalous)")
        print(f"Customer data saved to: {customer_file}")
        print(f"Address data (with insert timestamps) saved to: {address_file}")
        print(f"Total address records: {len(customer_addresses)} (append-only base table)")
        
        # Generate employee hierarchy (dynamic based on customer distribution)
        print("\nGenerating employee hierarchy...")
        employee_generator = EmployeeGenerator(self.config)
        
        # Group customers by current country for employee assignment
        customers_by_country = customer_generator.group_customers_by_current_country()
        
        # Generate employees and assignments dynamically
        employees, assignments = employee_generator.generate_employees_and_assignments(customers_by_country)
        
        # Save employee data
        employee_file = self.master_data_dir / "employees.csv"
        employee_generator.write_employees_to_csv(str(employee_file))
        
        assignment_file = self.master_data_dir / "client_assignments.csv"
        employee_generator.write_assignments_to_csv(str(assignment_file))
        
        print(f"Generated {len(employees)} employees")
        print(f"  - Client Advisors: {sum(1 for e in employees if e.position_level == 'CLIENT_ADVISOR')}")
        print(f"  - Team Leaders: {sum(1 for e in employees if e.position_level == 'TEAM_LEADER')}")
        print(f"  - Super Team Leaders: {sum(1 for e in employees if e.position_level == 'SUPER_TEAM_LEADER')}")
        print(f"Employee data saved to: {employee_file}")
        print(f"Client assignments saved to: {assignment_file}")
        print(f"Total assignments: {len(assignments)}")
        
        # Generate accounts
        print("\nGenerating account master data...")
        account_generator = AccountGenerator(self.config)
        accounts = account_generator.generate_accounts(customers)
        account_file = account_generator.save_accounts_to_csv(accounts, str(self.master_data_dir))
        print(f"Generated {len(accounts)} accounts")
        print(f"Account data saved to: {account_file}")
        
        # Generate FX rates
        print("\nGenerating FX rates...")
        fx_generator = FXRateGenerator(self.config)
        fx_rates = fx_generator.generate_fx_rates()
        files_created = fx_generator.save_fx_rates_to_csv_by_date(fx_rates, str(self.fx_rates_dir))
        print(f"Generated {len(fx_rates)} FX rate records")
        print(f"FX rates saved to: {self.fx_rates_dir} ({len(files_created)} files, one per date)")
        
        # Generate transactions
        print("\nGenerating transaction data...")
        accounts_file = f"{self.output_dir}/master_data/accounts.csv"
        transaction_generator = TransactionGenerator(self.config, customers, fx_rates, accounts_file)
        all_transactions = transaction_generator.generate_all_transactions()
        
        print(f"Generated {len(all_transactions)} total transactions")
        
        # Generate daily files (optimized batch processing)
        print("\nGenerating daily transaction files...")
        result = transaction_generator.save_all_daily_transactions_to_csv(str(self.payment_transactions_dir))
        daily_files = result['files']
        transaction_counts = result['counts']
        
        # Print sample of daily counts (first 12 days)
        for date_str in sorted(list(transaction_counts.keys()))[:12]:
            print(f"  {date_str}: {transaction_counts[date_str]} transactions")
        
        if len(transaction_counts) > 12:
            print(f"  ... ({len(transaction_counts) - 12} more days)")
        
        transaction_count = sum(transaction_counts.values())
        print(f"\n✅ Generated {len(daily_files)} daily files with {transaction_count} transactions")
        
        # Filter investment accounts for equity trading
        print("\nFiltering investment accounts for equity trading...")
        
        # Get all INVESTMENT accounts from the generated accounts
        investment_accounts = [acc for acc in accounts if acc.account_type == 'INVESTMENT']
        
        # Get customers who have investment accounts (these will be our trading customers)
        trading_customer_ids = set(acc.customer_id for acc in investment_accounts)
        trading_customers = [cust for cust in customers if cust.customer_id in trading_customer_ids]
        
        print(f"Found {len(investment_accounts)} investment accounts for {len(trading_customers)} trading customers")
        
        # Generate equity trades
        print("\nGenerating equity trade data...")
        
        # Convert fx_rates list to dict for equity generator
        # Use the latest rate for each currency pair (CHF as base)
        fx_rates_dict = {}
        for fx_rate in fx_rates:
            if fx_rate.to_currency == "CHF":  # We want rates to CHF as base currency
                fx_rates_dict[fx_rate.from_currency] = fx_rate.rate
            elif fx_rate.from_currency == "CHF":  # Inverse rate if CHF is from currency
                fx_rates_dict[fx_rate.to_currency] = 1.0 / fx_rate.rate
        
        # Ensure CHF has rate 1.0
        fx_rates_dict["CHF"] = 1.0
        
        equity_generator = EquityTradeGenerator(trading_customers, investment_accounts, fx_rates_dict, seed=self.config.random_seed)
        equity_summary = equity_generator.generate_period_data(
            self.config.start_date, 
            self.config.end_date, 
            self.equity_trades_dir
        )
        print(f"Generated {equity_summary['total_trades']} equity trades over {equity_summary['trading_days']} trading days")
        print(f"Trading customers: {equity_summary['trading_customers']} (60% of total)")
        print(f"High-volume traders: {equity_summary['high_volume_traders']} (10% of trading customers)")
        print(f"Base currency: {equity_summary['base_currency']}")
        print(f"Markets: {', '.join(equity_summary['markets'])}")
        
        # DDL generation removed - managed manually in structure/ directory
        # Legacy SQL files removed - using new structure/ directory approach
        
        # Generate summary report
        summary_file = self._generate_summary_report(
            customers, anomalous_customers, all_transactions, daily_files, accounts, fx_rates, equity_summary, additional_results,
            employees, assignments
        )
        
        return {
            "customer_file": str(customer_file),
            "address_file": str(address_file),
            "account_file": account_file,
            "employee_file": str(employee_file),
            "assignment_file": str(assignment_file),
            "fx_files": files_created,
            "fx_file_count": len(files_created),
            "daily_files": daily_files,
            "summary_file": summary_file,
            "total_customers": len(customers),
            "total_accounts": len(accounts),
            "total_employees": len(employees),
            "total_assignments": len(assignments),
            "anomalous_customers": len(anomalous_customers),
            "total_transactions": len(all_transactions),
            "total_fx_rates": len(fx_rates),
            "daily_file_count": len(daily_files)
        }
    
    def generate_minimal_files(self) -> dict:
        """Generate minimal customer files required as dependencies for specific generators.
        
        This is used when specific generation flags are provided (e.g., --generate-address-updates)
        to avoid generating all default data (transactions, FX rates, equity trades, etc.).
        
        Generates ONLY:
        - Customer master data (customers.csv)
        - Customer addresses (customer_addresses.csv)
        - Account master data (accounts.csv) - required for fixed income/commodity generators
        
        Returns:
            dict: Summary of generated files with minimal information
        """
        # Create minimal directory structure
        self.master_data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\nGenerating minimal customer data in: {self.output_dir.absolute()}")
        print(f"Configuration: {self.config.num_customers} customers")
        
        # Generate customers and addresses
        print("\nGenerating customer data...")
        customer_generator = CustomerGenerator(self.config)
        customers, customer_addresses = customer_generator.generate_customers()
        
        # Add fuzzy matching test customer for PEP screening testing
        print("Adding fuzzy matching test customer for PEP screening...")
        test_customer, test_address = customer_generator.add_fuzzy_matching_test_customer()
        print(f"Added test customer: {test_customer.first_name} {test_customer.family_name} (ID: {test_customer.customer_id})")
        
        # Save customer master data (including fuzzy matching test customer)
        customer_file = self.master_data_dir / "customers.csv"
        customer_generator.save_customers_to_csv(str(customer_file))
        
        # Save customer address data (SCD Type 2)
        address_file = self.master_data_dir / "customer_addresses.csv"
        customer_generator.save_addresses_to_csv(str(address_file))
        
        anomalous_customers = customer_generator.get_anomalous_customers()
        print(f"✅ Generated {len(customers)} customers ({len(anomalous_customers)} anomalous)")
        print(f"📁 Customer data saved to: {customer_file}")
        print(f"📁 Address data saved to: {address_file}")
        print(f"   Total address records: {len(customer_addresses)} (append-only base table)")
        
        # Generate accounts (required for fixed income and commodity generators)
        print("\nGenerating account master data...")
        account_generator = AccountGenerator(self.config)
        accounts = account_generator.generate_accounts(customers)
        account_file = account_generator.save_accounts_to_csv(accounts, str(self.master_data_dir))
        print(f"✅ Generated {len(accounts)} accounts")
        print(f"📁 Account data saved to: {account_file}")
        
        return {
            "customer_file": str(customer_file),
            "address_file": str(address_file),
            "account_file": str(account_file),
            "total_customers": len(customers),
            "total_accounts": len(accounts),
            "anomalous_customers": len(anomalous_customers),
            "minimal_mode": True
        }
    
    def _generate_summary_report(self, customers: List, anomalous_customers: List, 
                               transactions: List, daily_files: List[str], accounts: List, fx_rates: List,
                               equity_summary: dict, additional_results: dict = None,
                               employees: List = None, assignments: List = None) -> str:
        """Generate a summary report of the generated data"""
        summary_file = self.reports_dir / "generation_summary.txt"
        
        # Calculate statistics
        total_amount = sum(t.amount for t in transactions)
        total_base_amount = sum(t.base_amount for t in transactions)
        avg_transaction_amount = total_amount / len(transactions) if transactions else 0
        avg_base_amount = total_base_amount / len(transactions) if transactions else 0
        
        # Count transactions by direction (based on amount sign)
        incoming_count = len([t for t in transactions if t.amount > 0])
        outgoing_count = len([t for t in transactions if t.amount < 0])
        
        # Count accounts by type and currency
        account_types = {}
        account_currencies = {}
        for account in accounts:
            account_types[account.account_type] = account_types.get(account.account_type, 0) + 1
            account_currencies[account.base_currency] = account_currencies.get(account.base_currency, 0) + 1
        
        # Count anomalous transactions (those with anomaly markers in description)
        anomalous_transactions = [
            t for t in transactions 
            if any(marker in t.description for marker in [
                "[LARGE_TRANSFER]", "[SUSPICIOUS_COUNTERPARTY]", "[ROUND_AMOUNT]",
                "[OFF_HOURS]", "[NEW_LARGE_BENEFICIARY]"
            ])
        ]
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("Synthetic banking Data Generator - Summary Report\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Generation Date: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}\n")
            f.write(f"Period: {self.config.start_date.strftime('%Y-%m-%d')} to {self.config.end_date.strftime('%Y-%m-%d')}\n\n")
            
            f.write("CONFIGURATION:\n")
            f.write(f"  Number of customers: {self.config.num_customers}\n")
            f.write(f"  Anomaly percentage: {self.config.anomaly_percentage}%\n")
            f.write(f"  Generation period: {self.config.generation_period_months} months\n")
            f.write(f"  Avg transactions per customer per month: {self.config.avg_transactions_per_customer_per_month}\n\n")
            
            f.write("GENERATED DATA SUMMARY:\n")
            f.write(f"  Total customers: {len(customers)}\n")
            f.write(f"  Total accounts: {len(accounts)}\n")
            if employees:
                f.write(f"  Total employees: {len(employees)}\n")
            f.write(f"  Anomalous customers: {len(anomalous_customers)} ({len(anomalous_customers)/len(customers)*100:.1f}%)\n")
            f.write(f"  Total transactions: {len(transactions)}\n")
            f.write(f"  Anomalous transactions: {len(anomalous_transactions)} ({len(anomalous_transactions)/len(transactions)*100:.1f}%)\n")
            f.write(f"  FX rate records: {len(fx_rates)}\n")
            f.write(f"  Daily payment files: {len(daily_files)}\n")
            f.write(f"  Equity trades: {equity_summary['total_trades']}\n")
            
            # Add additional generator counts to main summary
            if additional_results:
                if 'swift' in additional_results and additional_results['swift']:
                    swift = additional_results['swift']
                    f.write(f"  SWIFT message pairs: {swift.get('successful_pairs', 0)}\n")
                if 'pep' in additional_results and additional_results['pep']:
                    pep = additional_results['pep']
                    f.write(f"  PEP records: {pep.get('total_records', 0)}\n")
                if 'mortgage' in additional_results and additional_results['mortgage']:
                    mortgage = additional_results['mortgage']
                    f.write(f"  Mortgage customers: {mortgage.get('total_customers', 0)}\n")
                if 'address_updates' in additional_results and additional_results['address_updates']:
                    addr = additional_results['address_updates']
                    f.write(f"  Address update files: {addr.get('update_files', 0)}\n")
                if 'fixed_income' in additional_results and additional_results['fixed_income']:
                    fi = additional_results['fixed_income']
                    f.write(f"  Fixed income trades: {fi.get('total_trades', 0)}\n")
                if 'commodities' in additional_results and additional_results['commodities']:
                    comm = additional_results['commodities']
                    f.write(f"  Commodity trades: {comm.get('total_trades', 0)}\n")
                if 'lifecycle' in additional_results and additional_results['lifecycle']:
                    f.write(f"  Customer lifecycle events: Generated\n")
            
            f.write("\n")
            
            f.write("ACCOUNT DISTRIBUTION:\n")
            for acc_type, count in account_types.items():
                f.write(f"  {acc_type}: {count} accounts\n")
            f.write("\n")
            
            f.write("CURRENCY DISTRIBUTION:\n")
            for currency, count in account_currencies.items():
                f.write(f"  {currency}: {count} accounts\n")
            f.write("\n")
            
            f.write("TRANSACTION STATISTICS:\n")
            f.write(f"  Total transaction amount: ${total_amount:,.2f} (mixed currencies)\n")
            f.write(f"  Total base amount (USD): ${total_base_amount:,.2f}\n")
            f.write(f"  Average transaction amount: ${avg_transaction_amount:,.2f}\n")
            f.write(f"  Average base amount (USD): ${avg_base_amount:,.2f}\n")
            f.write(f"  Incoming transactions: {incoming_count} ({incoming_count/len(transactions)*100:.1f}%)\n")
            f.write(f"  Outgoing transactions: {outgoing_count} ({outgoing_count/len(transactions)*100:.1f}%)\n\n")
            
            f.write("EQUITY TRADE STATISTICS:\n")
            f.write(f"  Total equity trades: {equity_summary['total_trades']}\n")
            f.write(f"  Trading days: {equity_summary['trading_days']}\n")
            f.write(f"  Trading customers: {equity_summary['trading_customers']} (60% of total)\n")
            f.write(f"  High-volume traders: {equity_summary['high_volume_traders']} (10% of trading customers)\n")
            f.write(f"  Base currency: {equity_summary['base_currency']}\n")
            f.write(f"  Markets covered: {', '.join(equity_summary['markets'])}\n\n")
            
            # Add employee hierarchy statistics
            if employees and assignments:
                advisor_count = sum(1 for e in employees if e.position_level == 'CLIENT_ADVISOR')
                tl_count = sum(1 for e in employees if e.position_level == 'TEAM_LEADER')
                stl_count = sum(1 for e in employees if e.position_level == 'SUPER_TEAM_LEADER')
                
                f.write("EMPLOYEE HIERARCHY STATISTICS:\n")
                f.write(f"  Total employees: {len(employees)}\n")
                f.write(f"  Super Team Leaders: {stl_count}\n")
                f.write(f"  Team Leaders: {tl_count}\n")
                f.write(f"  Client Advisors: {advisor_count}\n")
                f.write(f"  Customer assignments: {len(assignments)}\n")
                
                # Calculate average clients per advisor
                if advisor_count > 0:
                    avg_clients = len(assignments) / advisor_count
                    f.write(f"  Avg clients per advisor: {avg_clients:.1f}\n")
                
                # Count countries covered
                countries_covered = len(set(e.country for e in employees if e.position_level == 'CLIENT_ADVISOR'))
                f.write(f"  Countries covered: {countries_covered}\n")
                f.write("\n")
            
            # Add detailed statistics for additional generators
            if additional_results:
                if 'swift' in additional_results and additional_results['swift']:
                    swift = additional_results['swift']
                    f.write("SWIFT MESSAGE STATISTICS:\n")
                    f.write(f"  Message pairs: {swift.get('successful_pairs', 0)}\n")
                    f.write(f"  XML files: {swift.get('successful_pairs', 0) * 2}\n")
                    f.write(f"  Transaction volume: €{swift.get('total_volume', 0):,.2f}\n")
                    f.write(f"  SWIFT customers: {swift.get('swift_customers', 0)}\n")
                    if swift.get('anomaly_customers_with_swift'):
                        f.write(f"  Anomaly customers with SWIFT: {swift.get('anomaly_customers_with_swift', 0)}\n")
                    f.write("\n")
                
                if 'pep' in additional_results and additional_results['pep']:
                    pep = additional_results['pep']
                    f.write("PEP DATA STATISTICS:\n")
                    f.write(f"  PEP records: {pep.get('total_records', 0)}\n")
                    f.write(f"  Risk levels: {', '.join([f'{k}:{v}' for k, v in pep.get('risk_levels', {}).items()])}\n")
                    f.write(f"  Categories: {', '.join([f'{k}:{v}' for k, v in pep.get('categories', {}).items()])}\n")
                    f.write("\n")
                
                if 'fixed_income' in additional_results and additional_results['fixed_income']:
                    fi = additional_results['fixed_income']
                    f.write("FIXED INCOME STATISTICS:\n")
                    f.write(f"  Total trades: {fi.get('total_trades', 0)}\n")
                    f.write(f"  Bonds: {fi.get('bonds', 0)}, Swaps: {fi.get('swaps', 0)}\n")
                    f.write(f"  Total Notional: {fi.get('currency', 'CHF')} {fi.get('total_notional', 0):,.2f}\n")
                    f.write(f"  Files created: {fi.get('files_created', 0)}\n")
                    f.write("\n")
                
                if 'commodities' in additional_results and additional_results['commodities']:
                    comm = additional_results['commodities']
                    f.write("COMMODITY STATISTICS:\n")
                    f.write(f"  Total trades: {comm.get('total_trades', 0)}\n")
                    types_str = ', '.join([f'{k}:{v}' for k, v in comm.get('trade_types', {}).items()])
                    if types_str:
                        f.write(f"  Types: {types_str}\n")
                    f.write(f"  Total Value: {comm.get('currency', 'CHF')} {comm.get('total_value', 0):,.2f}\n")
                    f.write(f"  Files created: {comm.get('files_created', 0)}\n")
                    f.write("\n")
            
            f.write("ANOMALOUS CUSTOMERS:\n")
            for customer in anomalous_customers:
                f.write(f"  {customer.customer_id}: {customer.first_name} {customer.family_name}\n")
            
            f.write("\nFILES GENERATED:\n")
            f.write(f"📁 Master Data (master_data/):\n")
            f.write(f"  customers.csv\n")
            f.write(f"  customer_addresses.csv\n")
            f.write(f"  accounts.csv\n")
            if employees and assignments:
                f.write(f"  employees.csv\n")
                f.write(f"  client_assignments.csv\n")
            
            f.write(f"\n📁 FX Rates (fx_rates/):\n")
            f.write(f"  fx_rates.csv\n")
            
            f.write(f"\n📁 Payment Transactions (payment_transactions/):\n")
            for daily_file in daily_files:
                filename = os.path.basename(daily_file)
                f.write(f"  {filename}\n")
            
            f.write(f"\n📁 Equity Trades (equity_trades/):\n")
            # List equity trade files
            for trade_file in self.equity_trades_dir.glob("trades_*.csv"):
                f.write(f"  {trade_file.name}\n")
            
            
            # Add additional generator files if provided
            if additional_results:
                if 'swift' in additional_results and additional_results['swift']:
                    swift = additional_results['swift']
                    f.write(f"\n📁 SWIFT Messages (swift_messages/):\n")
                    swift_dir = self.output_dir / "swift_messages"
                    if swift_dir.exists():
                        swift_files = sorted(swift_dir.glob("*.xml"))[:10]  # Show first 10
                        for swift_file in swift_files:
                            f.write(f"  {swift_file.name}\n")
                        total_files = len(list(swift_dir.glob("*.xml")))
                        if total_files > 10:
                            f.write(f"  ... ({total_files - 10} more files)\n")
                
                if 'pep' in additional_results and additional_results['pep']:
                    f.write(f"\n📁 PEP Data (master_data/):\n")
                    f.write(f"  pep_data.csv\n")
                
                if 'mortgage' in additional_results and additional_results['mortgage']:
                    f.write(f"\n📁 Mortgage Emails (emails/):\n")
                    email_dir = self.output_dir / "emails"
                    if email_dir.exists():
                        email_files = sorted(email_dir.glob("*.txt"))[:10]  # Show first 10
                        for email_file in email_files:
                            f.write(f"  {email_file.name}\n")
                        total_files = len(list(email_dir.glob("*.txt")))
                        if total_files > 10:
                            f.write(f"  ... ({total_files - 10} more files)\n")
                
                if 'address_updates' in additional_results and additional_results['address_updates']:
                    f.write(f"\n📁 Address Updates (master_data/address_updates/):\n")
                    addr_dir = self.master_data_dir / "address_updates"
                    if addr_dir.exists():
                        for addr_file in sorted(addr_dir.glob("customer_addresses_*.csv")):
                            f.write(f"  {addr_file.name}\n")
                
                if 'fixed_income' in additional_results and additional_results['fixed_income']:
                    f.write(f"\n📁 Fixed Income Trades (fixed_income_trades/):\n")
                    fi_dir = self.output_dir / "fixed_income_trades"
                    if fi_dir.exists():
                        for fi_file in sorted(fi_dir.glob("*.csv")):
                            f.write(f"  {fi_file.name}\n")
                
                if 'commodity' in additional_results and additional_results['commodity']:
                    f.write(f"\n📁 Commodity Trades (commodity_trades/):\n")
                    comm_dir = self.output_dir / "commodity_trades"
                    if comm_dir.exists():
                        for comm_file in sorted(comm_dir.glob("*.csv")):
                            f.write(f"  {comm_file.name}\n")
                
                if 'lifecycle' in additional_results and additional_results['lifecycle']:
                    f.write(f"\n📁 Customer Lifecycle Events (master_data/):\n")
                    f.write(f"  customer_events/ (date-based files)\n")
                    f.write(f"  customer_status.csv\n")
            
            f.write(f"\n📁 Reports (reports/):\n")
            f.write(f"  generation_summary.txt\n")
            
            f.write(f"\n📁 Database Setup:\n")
            f.write(f"  Database schema definitions are managed in the structure/ directory\n")
            f.write(f"  See structure/README_DEPLOYMENT.md for deployment instructions\n")
        
        print(f"Summary report saved to: {summary_file}")
        return str(summary_file)
    
    def update_summary_with_additional_results(self, additional_results: dict) -> None:
        """Update the existing summary report with additional generator results"""
        if not additional_results:
            return
        
        summary_file = self.reports_dir / "generation_summary.txt"
        if not summary_file.exists():
            print("Warning: Summary file not found, cannot update")
            return
        
        # Read the existing summary
        with open(summary_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Find the line with "Equity trades:" and insert additional counts after it
        equity_line_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith("Equity trades:"):
                equity_line_idx = i
                break
        
        if equity_line_idx is not None:
            # Insert additional generator counts
            insert_lines = []
            if 'swift' in additional_results and additional_results['swift']:
                swift = additional_results['swift']
                insert_lines.append(f"  SWIFT message pairs: {swift.get('successful_pairs', 0)}\n")
            if 'pep' in additional_results and additional_results['pep']:
                pep = additional_results['pep']
                insert_lines.append(f"  PEP records: {pep.get('total_records', 0)}\n")
            if 'mortgage' in additional_results and additional_results['mortgage']:
                mortgage = additional_results['mortgage']
                insert_lines.append(f"  Mortgage customers: {mortgage.get('customers', 0)}\n")
            if 'address_updates' in additional_results and additional_results['address_updates']:
                addr = additional_results['address_updates']
                insert_lines.append(f"  Address update files: {addr.get('files_generated', 0)}\n")
            if 'fixed_income' in additional_results and additional_results['fixed_income']:
                fi = additional_results['fixed_income']
                insert_lines.append(f"  Fixed income trades: {fi.get('total_trades', 0)}\n")
            if 'commodity' in additional_results and additional_results['commodity']:
                comm = additional_results['commodity']
                insert_lines.append(f"  Commodity trades: {comm.get('total_trades', 0)}\n")
            if 'lifecycle' in additional_results and additional_results['lifecycle']:
                insert_lines.append(f"  Customer lifecycle events: Generated\n")
            
            # Insert the lines after equity trades line
            lines = lines[:equity_line_idx+1] + insert_lines + lines[equity_line_idx+1:]
        
        # Find where to insert detailed statistics sections (after EQUITY TRADE STATISTICS section)
        insert_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith("ANOMALOUS CUSTOMERS:"):
                insert_idx = i
                break
        
        if insert_idx is not None:
            stat_lines = []
            
            # Add detailed statistics for each generator
            if 'swift' in additional_results and additional_results['swift']:
                swift = additional_results['swift']
                stat_lines.append("SWIFT MESSAGE STATISTICS:\n")
                stat_lines.append(f"  Message pairs: {swift.get('successful_pairs', 0)}\n")
                stat_lines.append(f"  XML files: {swift.get('successful_pairs', 0) * 2}\n")
                stat_lines.append(f"  Transaction volume: €{swift.get('total_volume', 0):,.2f}\n")
                stat_lines.append(f"  SWIFT customers: {swift.get('summary', {}).get('configuration', {}).get('swift_customers', 0)}\n")
                anomaly_count = swift.get('summary', {}).get('generation_stats', {}).get('anomaly_customers_with_swift', 0)
                if anomaly_count:
                    stat_lines.append(f"  Anomaly customers with SWIFT: {anomaly_count}\n")
                stat_lines.append("\n")
            
            if 'pep' in additional_results and additional_results['pep']:
                pep = additional_results['pep']
                stat_lines.append("PEP DATA STATISTICS:\n")
                stat_lines.append(f"  PEP records: {pep.get('total_records', 0)}\n")
                stat_lines.append(f"  Risk levels: {', '.join([f'{k}:{v}' for k, v in pep.get('risk_levels', {}).items()])}\n")
                stat_lines.append(f"  Categories: {', '.join([f'{k}:{v}' for k, v in pep.get('categories', {}).items()])}\n")
                stat_lines.append("\n")
            
            if 'fixed_income' in additional_results and additional_results['fixed_income']:
                fi = additional_results['fixed_income']
                stat_lines.append("FIXED INCOME STATISTICS:\n")
                stat_lines.append(f"  Total trades: {fi.get('total_trades', 0)}\n")
                stat_lines.append(f"  Bonds: {fi.get('bonds', 0)}, Swaps: {fi.get('swaps', 0)}\n")
                stat_lines.append(f"  Total Notional: CHF {fi.get('total_notional_chf', 0):,.2f}\n")
                stat_lines.append(f"  Files created: {fi.get('files_created', 0)}\n")
                stat_lines.append("\n")
            
            if 'commodity' in additional_results and additional_results['commodity']:
                comm = additional_results['commodity']
                stat_lines.append("COMMODITY STATISTICS:\n")
                stat_lines.append(f"  Total trades: {comm.get('total_trades', 0)}\n")
                types_str = ', '.join([f'{k}:{v}' for k, v in comm.get('commodity_types', {}).items()])
                if types_str:
                    stat_lines.append(f"  Types: {types_str}\n")
                stat_lines.append(f"  Total Value: CHF {comm.get('total_value_chf', 0):,.2f}\n")
                stat_lines.append(f"  Files created: {comm.get('files_created', 0)}\n")
                stat_lines.append("\n")
            
            # Insert before ANOMALOUS CUSTOMERS
            lines = lines[:insert_idx] + stat_lines + lines[insert_idx:]
        
        # Write updated summary
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"✅ Summary report updated with additional generator results")
    
    def clean_output_directory(self) -> None:
        """Clean the output directory of previously generated files (selective cleaning)"""
        if self.output_dir.exists():
            # Clean main directory files
            for file in self.output_dir.glob("*.csv"):
                file.unlink()
            for file in self.output_dir.glob("*.txt"):
                file.unlink()
            
            # Clean specific subdirectories that should be regenerated
            subdirs_to_clean = [
                "master_data",
                "payment_transactions", 
                "equity_trades",
                "fx_rates",
                "swift_messages",
                "emails",
                "mortgage_emails",
                "pep_data",
                "reports",
                "fixed_income_trades",
                "commodity_trades"
            ]
            
            # Clean nested subdirectories
            nested_subdirs_to_clean = [
                ("master_data", "address_updates")
            ]
            
            for subdir_name in subdirs_to_clean:
                subdir = self.output_dir / subdir_name
                if subdir.exists():
                    for file in subdir.glob("*.csv"):
                        file.unlink()
                    for file in subdir.glob("*.txt"):
                        file.unlink()
                    for file in subdir.glob("*.xml"):
                        file.unlink()
                    for file in subdir.glob("*.json"):
                        file.unlink()
                    print(f"Cleaned subdirectory: {subdir}")
            
            # Clean nested subdirectories
            for parent_dir, nested_dir in nested_subdirs_to_clean:
                nested_path = self.output_dir / parent_dir / nested_dir
                if nested_path.exists():
                    for file in nested_path.glob("*.csv"):
                        file.unlink()
                    for file in nested_path.glob("*.txt"):
                        file.unlink()
                    for file in nested_path.glob("*.xml"):
                        file.unlink()
                    for file in nested_path.glob("*.json"):
                        file.unlink()
                    print(f"Cleaned nested subdirectory: {nested_path}")
            
            print(f"Cleaned output directory: {self.output_dir}")
        else:
            print(f"Output directory doesn't exist: {self.output_dir}")
