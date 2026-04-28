#!/usr/bin/env python3
"""
SWIFT Message Generator for Synthetic Bank Customers
Generates PACS.008 and corresponding PACS.002 messages for customers
"""

import subprocess
import json
import os
import random
import time
import csv
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional

from base_generator import init_random_seed


# Country to BIC mapping for EMEA regions
COUNTRY_TO_BIC_MAP = {
    'DE': ['DEUTDEFF', 'COBADEFF', 'DRESDEFF', 'HYVEDEMM'],
    'CH': ['UBSWCHZH', 'RBOSGGSX', 'CRESCHZZ', 'ZKBKCHZZ'],
    'NO': ['DNBANOKKXXX', 'SBANNO22', 'NDEANOKK', 'HANSNOKK'],
    'IT': ['BCITITMM', 'UNCRITMM', 'BPIMITRR', 'CITIITMX'],
    'GB': ['NWBKGB2L', 'HBUKGB4B', 'BARCGB22', 'LOYDGB2L'],
    'NL': ['ABNANL2A', 'RABONL2U', 'INGBNL2A', 'SBINNL2X'],
    'ES': ['BBVAESMM', 'CAIXESBB', 'BSCHESMM', 'POPUESMM'],
    'IE': ['BOFIIE2D', 'AIBIIE2D', 'ULSBIE2D', 'BOFAIE2D'],
    'HU': ['OTPVHUHB', 'BUDAHUHB', 'MKKBHUHB', 'ERSDHUHB'],
    'EE': ['EEUHEE2X', 'LHVBEE22', 'RIKOEE22', 'FOREEE2X'],
    'FR': ['BNPAFRPP', 'CRLYFRPP', 'SOGEFRPP', 'AGRIFRPP'],
    'AT': ['BKAUATWW', 'RLNWATWW', 'GIBAATWW', 'EHISATWW'],
    'BE': ['GEBABEBB', 'KRED BEBB', 'CREGBEBB', 'BNAGBEBB'],
    'LU': ['BCEELULL', 'BILLLULL', 'CELLLULL', 'SNBCLULL'],
    'PT': ['CGDIPTPL', 'BESCPTPL', 'BBPIPTPL', 'BPIGPTPL'],
    'FI': ['NDEAFIHH', 'HELSFIHH', 'OKOYFIHH', 'AABAFI22'],
    'SE': ['HANDSESS', 'SWEDSESS', 'NDEASESS', 'FIHBSESS'],
    'DK': ['DABADKKK', 'JYBADKKK', 'NYKBDKKK', 'ANYKDKKK'],
    'Poland': ['PKOPPLPW', 'BPKOPLPW', 'INGBPLPW', 'BREXPLPW'],
    'Norway': ['DNBANOKKXXX', 'SBANNO22', 'NDEANOKK', 'HANSNOKK'],
    'France': ['BNPAFRPP', 'CRLYFRPP', 'SOGEFRPP', 'AGRIFRPP'],
    'Germany': ['DEUTDEFF', 'COBADEFF', 'DRESDEFF', 'HYVEDEMM']
}

# IBAN country code and length mapping
IBAN_STRUCTURE = {
    'DE': {'code': 'DE', 'length': 22, 'pattern': 'DE{check}{bank_code:8}{account:10}'},
    'CH': {'code': 'CH', 'length': 21, 'pattern': 'CH{check}{bank_code:5}{account:12}'},
    'NO': {'code': 'NO', 'length': 15, 'pattern': 'NO{check}{bank_code:4}{account:7}'},
    'IT': {'code': 'IT', 'length': 27, 'pattern': 'IT{check}{cin:1}{abi:5}{cab:5}{account:12}'},
    'GB': {'code': 'GB', 'length': 22, 'pattern': 'GB{check}{bank_code:4}{sort:6}{account:8}'},
    'NL': {'code': 'NL', 'length': 18, 'pattern': 'NL{check}{bank_code:4}{account:10}'},
    'ES': {'code': 'ES', 'length': 24, 'pattern': 'ES{check}{bank_code:4}{branch:4}{ctrl:2}{account:10}'},
    'IE': {'code': 'IE', 'length': 22, 'pattern': 'IE{check}{bank_code:4}{branch:6}{account:8}'},
    'FR': {'code': 'FR', 'length': 27, 'pattern': 'FR{check}{bank_code:5}{branch:5}{account:11}{key:2}'},
    'Poland': {'code': 'PL', 'length': 28, 'pattern': 'PL{check}{bank_code:8}{account:16}'},
    'Norway': {'code': 'NO', 'length': 15, 'pattern': 'NO{check}{bank_code:4}{account:7}'},
    'France': {'code': 'FR', 'length': 27, 'pattern': 'FR{check}{bank_code:5}{branch:5}{account:11}{key:2}'},
    'Germany': {'code': 'DE', 'length': 22, 'pattern': 'DE{check}{bank_code:8}{account:10}'}
}

REMITTANCE_TYPES = [
    "Invoice SWIFT-2025-{:06d}",
    "Contract payment CT-2025-{:06d}",
    "Service fee SF-2025-{:06d}", 
    "Trade settlement TS-2025-{:06d}",
    "Intercompany transfer IC-2025-{:06d}",
    "Supplier payment SP-2025-{:06d}",
    "Professional services PS-2025-{:06d}",
    "Equipment purchase EP-2025-{:06d}",
    "Consulting fee CF-2025-{:06d}",
    "License payment LP-2025-{:06d}",
    "Software license SL-2025-{:06d}",
    "Maintenance contract MC-2025-{:06d}",
    "Insurance premium IP-2025-{:06d}",
    "Rent payment RP-2025-{:06d}",
    "Utility bill UB-2025-{:06d}",
    "Tax payment TX-2025-{:06d}",
    "Dividend payment DV-2025-{:06d}",
    "Interest payment IN-2025-{:06d}",
    "Refund transaction RF-2025-{:06d}",
    "Bonus payment BP-2025-{:06d}"
]


def generate_bic_for_country(country: str) -> str:
    """Generate a realistic BIC for a given country"""
    country_upper = country.upper()
    
    # Handle full country names
    if country in COUNTRY_TO_BIC_MAP:
        bics = COUNTRY_TO_BIC_MAP[country]
    elif country_upper[:2] in COUNTRY_TO_BIC_MAP:
        bics = COUNTRY_TO_BIC_MAP[country_upper[:2]]
    else:
        # Default to German BICs if country not found
        bics = COUNTRY_TO_BIC_MAP['DE']
    
    return random.choice(bics)

def generate_iban_for_country(country: str, account_id: str) -> str:
    """Generate a realistic IBAN for a given country"""
    country_upper = country.upper()
    
    # Handle full country names to country codes
    country_code_mapping = {
        'Poland': 'PL',
        'Norway': 'NO', 
        'France': 'FR',
        'Germany': 'DE'
    }
    
    if country in country_code_mapping:
        country_code = country_code_mapping[country]
    elif country_upper[:2] in IBAN_STRUCTURE:
        country_code = country_upper[:2]
    else:
        country_code = 'DE'  # Default to Germany
    
    if country_code not in IBAN_STRUCTURE:
        country_code = 'DE'  # Fallback
    
    structure = IBAN_STRUCTURE[country_code]
    
    # Generate components based on country
    check_digits = f"{random.randint(10, 99):02d}"
    
    if country_code == 'DE':
        bank_code = f"{random.randint(10000000, 99999999):08d}"
        account = f"{random.randint(1000000000, 9999999999):010d}"
        iban = f"DE{check_digits}{bank_code}{account}"
    elif country_code == 'FR':
        bank_code = f"{random.randint(10000, 99999):05d}"
        branch = f"{random.randint(10000, 99999):05d}"
        account = f"{random.randint(10000000000, 99999999999):011d}"
        key = f"{random.randint(10, 99):02d}"
        iban = f"FR{check_digits}{bank_code}{branch}{account}{key}"
    elif country_code == 'NO':
        bank_code = f"{random.randint(1000, 9999):04d}"
        account = f"{random.randint(1000000, 9999999):07d}"
        iban = f"NO{check_digits}{bank_code}{account}"
    elif country_code == 'PL':
        bank_code = f"{random.randint(10000000, 99999999):08d}"
        account = f"{random.randint(1000000000000000, 9999999999999999):016d}"
        iban = f"PL{check_digits}{bank_code}{account}"
    else:
        # Generic fallback
        bank_code = f"{random.randint(1000, 9999):04d}"
        account = f"{random.randint(1000000000, 9999999999):010d}"
        iban = f"{country_code}{check_digits}{bank_code}{account}"
    
    return iban

class ThreadSafeCounter:
    """Thread-safe counter for tracking generation progress"""
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self):
        with self._lock:
            self._value += 1
            return self._value
    
    @property
    def value(self):
        return self._value


class SWIFTGenerator:
    """SWIFT Message Generator for synthetic bank customers"""
    
    def __init__(self, swift_generator_script: str = "swift_message_generator.py", seed: int = 42):
        """
        Initialize SWIFT generator
        
        Args:
            swift_generator_script: Path to the SWIFT message generator script
            seed: Random seed for deterministic generation
        """
        self.swift_generator_script = swift_generator_script
        self.success_counter = ThreadSafeCounter()
        self.error_counter = ThreadSafeCounter()
        
        # Initialize random state with seed for reproducibility
        self.fake = init_random_seed(seed)
        
        # Currency to country mapping for SWIFT generation
        self.currency_to_country = {
            'EUR': 'Germany',  # Default to Germany for EUR
            'GBP': 'United Kingdom',
            'CHF': 'Switzerland',
            'NOK': 'Norway',
            'SEK': 'Sweden',
            'DKK': 'Denmark'
        }
    
    def _currency_to_country(self, currency: str) -> str:
        """Map currency code to country name for SWIFT generation"""
        return self.currency_to_country.get(currency, 'Germany')
    
    def load_customers_from_csv(self, customer_file_path: str) -> List[Dict[str, Any]]:
        """Load customers from CSV file and convert to SWIFT-compatible format"""
        customers = []
        try:
            with open(customer_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Map reporting_currency to country for SWIFT generation
                    reporting_currency = row.get('reporting_currency', 'EUR')
                    country = self._currency_to_country(reporting_currency)
                    customer_id = row['customer_id']
                    
                    # Use the new functions to generate realistic BIC and IBAN
                    bic = generate_bic_for_country(country)
                    iban = generate_iban_for_country(country, customer_id)
                    
                    # Get country code for SWIFT messages
                    country_code_mapping = {
                        'Poland': 'PL', 'Norway': 'NO', 'France': 'FR', 'Germany': 'DE',
                        'Switzerland': 'CH', 'Sweden': 'SE', 'Italy': 'IT',
                        'United Kingdom': 'GB', 'Netherlands': 'NL', 'Spain': 'ES', 
                        'Ireland': 'IE', 'Hungary': 'HU', 'Estonia': 'EE', 'Denmark': 'DK'
                    }
                    country_code = country_code_mapping.get(country, country[:2].upper())
                    
                    customer = {
                        'customer_id': customer_id,
                        'name': f"{row['first_name']} {row['family_name']}",
                        'bic': bic,
                        'iban': iban,
                        'street': row.get('street_address', f"{self.fake.street_address()}"),
                        'city': row.get('city', f"{self.fake.city()}"),
                        'postcode': row.get('zipcode', f"{self.fake.postcode()}"),
                        'country': country_code,
                        'has_anomaly': row['has_anomaly'].lower() == 'true'
                    }
                    customers.append(customer)
                    
            return customers
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Customer file not found: {customer_file_path}")
        except Exception as e:
            raise Exception(f"Error loading customers: {e}")
    
    def select_swift_customers(self, customers: List[Dict], percentage: float = 30.0) -> List[Dict]:
        """Select a percentage of customers for SWIFT message generation"""
        if not customers:
            return []
        
        num_swift_customers = max(1, int(len(customers) * percentage / 100))
        
        # Prefer customers with anomalies for more interesting SWIFT activity
        anomaly_customers = [c for c in customers if c['has_anomaly']]
        normal_customers = [c for c in customers if not c['has_anomaly']]
        
        selected_customers = []
        
        # Include all anomaly customers if we have space
        if len(anomaly_customers) <= num_swift_customers:
            selected_customers.extend(anomaly_customers)
            remaining = num_swift_customers - len(anomaly_customers)
            selected_customers.extend(random.sample(normal_customers, min(remaining, len(normal_customers))))
        else:
            # Too many anomaly customers, sample from them
            selected_customers = random.sample(anomaly_customers, num_swift_customers)
        
        return selected_customers
    
    def calculate_messages_per_customer(self, num_customers: int, avg_messages: float = 1.2) -> List[int]:
        """Calculate how many SWIFT messages each customer should generate"""
        messages_distribution = []
        total_target = int(num_customers * avg_messages)
        
        # Distribute messages with some randomness
        for i in range(num_customers):
            if random.random() < 0.6:  # 60% get 1 message
                messages = 1
            elif random.random() < 0.8:  # 20% get 2 messages  
                messages = 2
            elif random.random() < 0.95:  # 15% get 0 messages
                messages = 0
            else:  # 5% get 3+ messages (high activity customers)
                messages = random.randint(3, 5)
            
            messages_distribution.append(messages)
        
        # Adjust to hit target average
        current_total = sum(messages_distribution)
        if current_total < total_target:
            # Add messages to random customers
            for _ in range(total_target - current_total):
                idx = random.randint(0, num_customers - 1)
                messages_distribution[idx] += 1
        elif current_total > total_target:
            # Remove messages from customers who have > 1
            for _ in range(current_total - total_target):
                candidates = [i for i, count in enumerate(messages_distribution) if count > 1]
                if candidates:
                    idx = random.choice(candidates)
                    messages_distribution[idx] -= 1
        
        return messages_distribution
    
    def generate_amount(self, is_anomaly_customer: bool = False) -> int:
        """Generate realistic payment amounts with higher amounts for anomaly customers"""
        if is_anomaly_customer:
            # Anomaly customers get larger, more suspicious amounts
            if random.random() < 0.4:  # 40% very large
                return random.randint(50000, 500000)
            else:
                return random.randint(10000, 100000)
        else:
            # Standard amount distribution
            if random.random() < 0.5:  # 50% small payments
                return random.randint(100, 10000)
            elif random.random() < 0.8:  # 30% medium payments
                return random.randint(10000, 100000)
            else:  # 20% large payments
                return random.randint(100000, 1000000)
    
    def run_command(self, cmd: str, working_dir: str) -> tuple[bool, str]:
        """Run shell command efficiently"""
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                cwd=working_dir,
                timeout=30  # 30 second timeout per command
            )
            return result.returncode == 0, result.stdout.strip() if result.returncode == 0 else result.stderr.strip()
        except subprocess.TimeoutExpired:
            return False, "Command timeout"
        except Exception as e:
            return False, str(e)
    
    def generate_message_pair(self, customer: Dict, counterparty: Dict, batch_num: int, 
                            total_batches: int, working_dir: str, output_dir: str) -> Dict[str, Any]:
        """Generate a single PACS.008 and corresponding PACS.002 pair for a synthetic customer"""
        try:
            # Use synthetic customer as debtor and selected counterparty as creditor
            debtor = customer
            creditor = counterparty
            
            # Generate realistic data with higher amounts for anomaly customers
            amount = self.generate_amount(customer.get('has_anomaly', False))
            remittance = random.choice(REMITTANCE_TYPES).format(batch_num)
            
            # Include customer ID in remittance for traceability
            remittance = f"{remittance} - Customer: {customer['customer_id']}"
            
            # Generate production-style message IDs
            date_prefix = datetime.now().strftime("%Y%m%d")
            msg_sequence = f"{random.randint(100, 999):03d}"
            txn_sequence = f"{random.randint(100, 999):03d}"
            
            message_id = f"{date_prefix}-{debtor['bic']}-{msg_sequence}"
            end_to_end_id = f"{date_prefix}-{debtor['bic']}-TXN-{txn_sequence}"
            
            # Generate PACS.008
            pacs008_filename = f"swift_{customer['customer_id']}_{batch_num:06d}_pacs008.xml"
            pacs008_cmd = [
                f"./venv/bin/python {self.swift_generator_script} generate-pacs008",
                f'--message-id "{message_id}"',
                f"--amount {amount:.2f}",
                "--currency EUR",
                f'--debtor-name "{debtor["name"]}"',
                f'--debtor-bic {debtor["bic"]}',
                f'--creditor-name "{creditor["name"]}"', 
                f'--creditor-bic {creditor["bic"]}',
                f'--debtor-iban {debtor["iban"]}',
                f'--creditor-iban {creditor["iban"]}',
                f'--debtor-street "{debtor["street"]}"',
                f'--debtor-city "{debtor["city"]}"',
                f'--debtor-postcode "{debtor["postcode"]}"',
                f'--creditor-street "{creditor["street"]}"',
                f'--creditor-city "{creditor["city"]}"',
                f'--creditor-postcode "{creditor["postcode"]}"',
                f'--remittance-info "{remittance}"',
                f'--output "messages/{pacs008_filename}"'
            ]
            
            success_008, output_008 = self.run_command(" ".join(pacs008_cmd), working_dir)
            
            if not success_008:
                self.error_counter.increment()
                return {
                    "batch": batch_num,
                    "customer_id": customer['customer_id'],
                    "status": "pacs008_failed",
                    "error": output_008,
                    "debtor": debtor['name'],
                    "creditor": creditor['name']
                }
            
            # Generate corresponding PACS.002 using ID extraction
            pacs002_filename = f"swift_{customer['customer_id']}_{batch_num:06d}_pacs002.xml"
            pacs002_cmd = [
                f"./venv/bin/python {self.swift_generator_script} generate-pacs002",
                f'--from-pacs008 "messages/{pacs008_filename}"',  # Read from messages directory
                "--transaction-status ACCP",
                f'--status-reason "Customer {customer["customer_id"]} payment processed successfully"',
                f'--instructing-agent-bic {creditor["bic"]}',
                f'--instructed-agent-bic {debtor["bic"]}',
                f'--delay-minutes "1-45"',  # Simulate 1-45 minute processing delay
                f'--output "messages/{pacs002_filename}"'
            ]
            
            success_002, output_002 = self.run_command(" ".join(pacs002_cmd), working_dir)
            
            if not success_002:
                self.error_counter.increment()
                return {
                    "batch": batch_num,
                    "customer_id": customer['customer_id'],
                    "status": "pacs002_failed", 
                    "error": output_002,
                    "debtor": debtor['name'],
                    "creditor": creditor['name']
                }
            
            # Move generated files to target output directory
            try:
                messages_dir = Path(working_dir) / "messages"
                source_pacs008 = messages_dir / pacs008_filename
                source_pacs002 = messages_dir / pacs002_filename
                target_pacs008 = Path(output_dir) / pacs008_filename
                target_pacs002 = Path(output_dir) / pacs002_filename
                
                if source_pacs008.exists():
                    shutil.move(str(source_pacs008), str(target_pacs008))
                if source_pacs002.exists():
                    shutil.move(str(source_pacs002), str(target_pacs002))
            except Exception as move_error:
                print(f"Warning: Could not move SWIFT files: {move_error}")
            
            # Success
            self.success_counter.increment()
            
            # Progress reporting (adapt frequency based on total count)
            current_success = self.success_counter.value
            report_frequency = max(1, min(100, total_batches // 10))  # Report every 10% or at least every 1
            if current_success % report_frequency == 0 or current_success == total_batches:
                print(f"✅ SWIFT Progress: {current_success}/{total_batches} pairs completed ({current_success/total_batches*100:.1f}%)")
            
            return {
                "batch": batch_num,
                "customer_id": customer['customer_id'],
                "debtor": debtor['name'],
                "creditor": creditor['name'],
                "amount": amount,
                "message_id": message_id,
                "end_to_end_id": end_to_end_id,
                "has_anomaly": customer.get('has_anomaly', False),
                "pacs008": pacs008_filename,
                "pacs002": pacs002_filename,
                "status": "success"
            }
            
        except Exception as e:
            self.error_counter.increment()
            return {
                "batch": batch_num,
                "customer_id": customer.get('customer_id', 'unknown'),
                "status": "exception",
                "error": str(e)
            }
    
    def generate_swift_messages(self, customer_file_path: str, output_dir: str, 
                              customer_percentage: float = 30.0, avg_messages: float = 1.2,
                              max_workers: int = 4, swift_generator_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate SWIFT messages for synthetic bank customers
        
        Args:
            customer_file_path: Path to customer CSV file
            output_dir: Directory to save SWIFT XML files
            customer_percentage: Percentage of customers to select for SWIFT activity
            avg_messages: Average number of messages per selected customer
            max_workers: Number of parallel workers
            swift_generator_dir: Directory containing the SWIFT generator script
            
        Returns:
            Dictionary with generation statistics and results
        """
        print(f"\n🏦 Generating SWIFT messages for synthetic bank customers...")
        print(f"📊 Target: {customer_percentage}% of customers with avg {avg_messages} messages each")
        
        # Load customers
        customers = self.load_customers_from_csv(customer_file_path)
        if not customers:
            raise ValueError("No customers loaded from CSV file")
        
        # Select customers for SWIFT activity
        swift_customers = self.select_swift_customers(customers, customer_percentage)
        if not swift_customers:
            raise ValueError("No customers selected for SWIFT activity")
        
        print(f"📈 Selected {len(swift_customers)} customers for SWIFT activity ({customer_percentage}% of {len(customers)} total)")
        print(f"   - Anomaly customers: {len([c for c in swift_customers if c['has_anomaly']])}")
        print(f"   - Normal customers: {len([c for c in swift_customers if not c['has_anomaly']])}")
        
        # Calculate message distribution
        messages_per_customer = self.calculate_messages_per_customer(len(swift_customers), avg_messages)
        actual_avg = sum(messages_per_customer) / len(swift_customers) if swift_customers else 0
        print(f"💫 Message distribution: avg {actual_avg:.2f} messages/customer (target: {avg_messages})")
        print(f"   - Total messages: {sum(messages_per_customer)}")
        
        # Create message generation jobs
        jobs = []
        batch_num = 1
        
        for i, customer in enumerate(swift_customers):
            message_count = messages_per_customer[i]
            for msg_num in range(message_count):
                # Select random counterparty from all customers (different from current customer)
                available_counterparties = [c for c in customers if c['customer_id'] != customer['customer_id']]
                if available_counterparties:
                    counterparty = random.choice(available_counterparties)
                else:
                    # Fallback: use a different customer from swift_customers
                    counterparty = random.choice([c for c in swift_customers if c['customer_id'] != customer['customer_id']])
                
                jobs.append({
                    'customer': customer,
                    'counterparty': counterparty,
                    'batch_num': batch_num,
                    'total_batches': sum(messages_per_customer)
                })
                batch_num += 1
        
        print(f"📦 Generated {len(jobs)} message generation jobs")
        print(f"🚀 Starting SWIFT message generation with {max_workers} workers...")
        
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Determine working directory for SWIFT generator
        working_dir = swift_generator_dir or str(Path(customer_file_path).parent.absolute())
        
        # Ensure messages directory exists in working directory for SWIFT generator
        messages_dir = Path(working_dir) / "messages"
        messages_dir.mkdir(exist_ok=True)
        
        start_time = time.time()
        results = []
        total_volume = 0.0
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all jobs
                futures = {
                    executor.submit(
                        self.generate_message_pair, 
                        job['customer'], 
                        job['counterparty'], 
                        job['batch_num'], 
                        job['total_batches'],
                        working_dir,
                        output_dir
                    ): job 
                    for job in jobs
                }
                
                # Process completed jobs
                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=60)  # 1 minute timeout per pair
                        results.append(result)
                        
                        if result['status'] == 'success':
                            total_volume += result['amount']
                        
                    except Exception as e:
                        job = futures[future]
                        self.error_counter.increment()
                        results.append({
                            "batch": job['batch_num'],
                            "customer_id": job['customer']['customer_id'],
                            "status": "timeout_or_exception",
                            "error": str(e)
                        })
        
        except KeyboardInterrupt:
            print("\n🛑 SWIFT generation interrupted by user")
            raise
            
        finally:
            # Final statistics
            end_time = time.time()
            duration = end_time - start_time
            
            successful_pairs = self.success_counter.value
            failed_pairs = self.error_counter.value
            total_processed = successful_pairs + failed_pairs
            
            print(f"\n🎉 SWIFT Generation Complete!")
            print(f"✅ Successful pairs: {successful_pairs:,}")
            print(f"❌ Failed pairs: {failed_pairs:,}")
            print(f"📊 Total processed: {total_processed:,}/{len(jobs):,}")
            if total_processed > 0:
                print(f"📈 Success rate: {successful_pairs/total_processed*100:.1f}%")
            print(f"💰 Total transaction volume: €{total_volume:,.2f}")
            print(f"👥 Customers with SWIFT activity: {len(set(r.get('customer_id') for r in results if r['status'] == 'success'))}")
            print(f"🔍 Anomaly customers in results: {len([r for r in results if r.get('has_anomaly', False) and r['status'] == 'success'])}")
            print(f"⏱️  Total duration: {duration:.1f} seconds")
            if duration > 0:
                print(f"🚀 Average speed: {successful_pairs*2/duration:.1f} messages/second")
            print(f"📁 Files generated: {successful_pairs*2:,} XML files")
            
            # Create comprehensive summary
            summary = {
                "generated_at": datetime.now().isoformat(),
                "configuration": {
                    "customer_file": customer_file_path,
                    "total_customers": len(customers),
                    "customer_percentage": customer_percentage,
                    "swift_customers": len(swift_customers),
                    "avg_messages_target": avg_messages,
                    "actual_avg_messages": actual_avg,
                    "output_directory": output_dir,
                    "working_directory": working_dir
                },
                "generation_stats": {
                    "target_jobs": len(jobs),
                    "successful_pairs": successful_pairs,
                    "failed_pairs": failed_pairs,
                    "success_rate_percent": round(successful_pairs/total_processed*100, 2) if total_processed > 0 else 0,
                    "total_transaction_volume_eur": round(total_volume, 2),
                    "duration_seconds": round(duration, 1),
                    "messages_per_second": round(successful_pairs*2/duration, 1) if duration > 0 else 0,
                    "total_xml_files": successful_pairs * 2,
                    "unique_customers_with_swift": len(set(r.get('customer_id') for r in results if r['status'] == 'success')),
                    "anomaly_customers_with_swift": len([r for r in results if r.get('has_anomaly', False) and r['status'] == 'success'])
                },
                "customer_breakdown": {
                    "anomaly_customers": len([c for c in swift_customers if c['has_anomaly']]),
                    "normal_customers": len([c for c in swift_customers if not c['has_anomaly']])
                },
                "sample_results": results[:50]  # Sample for analysis
            }
            
            # Clean up temporary messages directory
            messages_dir = Path(working_dir) / "messages"
            if messages_dir.exists():
                try:
                    import shutil
                    shutil.rmtree(messages_dir)
                    print(f"🧹 Cleaned up temporary messages directory: {messages_dir}")
                except Exception as e:
                    print(f"⚠️  Warning: Could not clean up messages directory: {e}")
            
            return {
                "summary": summary,
                "results": results,
                "swift_customers": swift_customers,
                "total_volume": total_volume,
                "successful_pairs": successful_pairs,
                "failed_pairs": failed_pairs
            }
