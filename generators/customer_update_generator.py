#!/usr/bin/env python3
"""
Customer Update Generator for SCD Type 2 Processing

This module generates customer update files with dates in filenames
to simulate customer data changes over time for SCD Type 2 processing.

CRITICAL: Generates FULL customer records (all 17 attributes) for each update,
          not just field-level changes. This is required for SCD Type 2 loading
          into CRMI_CUSTOMER table.

Update Types:
- EMPLOYMENT_CHANGE: Job, income, employer updates
- ACCOUNT_TIER: Account tier upgrades/downgrades  
- CONTACT_INFO: Email, phone updates
- RISK_PROFILE: Risk classification changes
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

from base_generator import init_random_seed

class CustomerUpdateGenerator:
    """Generates customer update files for SCD Type 2 processing"""
    
    def __init__(self, customer_file: str, output_dir: str, seed: int = 42):
        self.customer_file = customer_file
        self.output_dir = Path(output_dir)
        self.customers = {}  # Store current state of all customers
        
        # Initialize random state with seed for reproducibility
        self.fake = init_random_seed(seed)
        
        # Reference data
        self.account_tiers = ['STANDARD', 'SILVER', 'GOLD', 'PLATINUM', 'PREMIUM']
        self.employment_types = ['FULL_TIME', 'PART_TIME', 'CONTRACT', 'SELF_EMPLOYED', 'RETIRED']
        self.risk_classifications = ['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH']
        self.income_ranges = ['<30K', '30K-50K', '50K-75K', '75K-100K', '100K-150K', '>150K']
        self.credit_score_bands = ['POOR', 'FAIR', 'GOOD', 'VERY_GOOD', 'EXCELLENT']
        self.contact_methods = ['EMAIL', 'SMS', 'POST', 'MOBILE_APP', 'PHONE']
        
    def load_customers(self):
        """Load initial customer data"""
        with open(self.customer_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['customer_id']:  # Skip empty rows
                    self.customers[row['customer_id']] = row
        print(f"📋 Loaded {len(self.customers)} customers from {self.customer_file}")
    
    
    def generate_customer_updates(self, num_update_files: int = 8) -> List[str]:
        """
        Generate multiple customer update files spread over time
        
        Args:
            num_update_files: Number of update files to generate (default: 8)
            
        Returns:
            List of paths to generated update files
        """
        if not self.customers:
            self.load_customers()
        
        # Calculate date range
        # Use 19 months period to match data_generator.sh default
        from datetime import datetime, timedelta
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2025, 7, 31)  # 19 months from start
        
        # Calculate days per file
        total_days = (end_date - start_date).days
        days_per_file = total_days // num_update_files if num_update_files > 0 else 30
        
        print(f"🔄 Generating {num_update_files} customer update files...")
        print(f"   Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"   Frequency: ~{days_per_file} days per file")
        
        # Call the existing generate_updates method
        self.generate_updates(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            updates_per_month=50,  # Average updates per month
            output_frequency_days=days_per_file
        )
        
        # Return list of generated files
        customer_updates_dir = self.output_dir / 'customer_updates'
        generated_files = sorted(customer_updates_dir.glob('customer_updates_*.csv'))
        
        return [str(f) for f in generated_files]
    
    def generate_updates(self, start_date: str, end_date: str, 
                        updates_per_month: int = 50,
                        output_frequency_days: int = 30):
        """
        Generate customer updates over time period
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            updates_per_month: Average number of updates per month
            output_frequency_days: How often to save files (days)
        """
        print(f"\n🔄 Generating customer updates from {start_date} to {end_date}")
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_date = start_dt
        file_date = start_dt
        pending_updates = []
        
        while current_date <= end_dt:
            # Generate updates for this day
            daily_updates = random.randint(0, updates_per_month // 15)  # Vary daily
            
            for _ in range(daily_updates):
                # Pick random customer
                customer_id = random.choice(list(self.customers.keys()))
                
                # Generate update
                update = self._generate_customer_update(customer_id, current_date)
                if update:
                    pending_updates.append(update)
            
            # Save file every N days or at end
            if (current_date - file_date).days >= output_frequency_days or current_date == end_dt:
                if pending_updates:
                    self._save_update_file(pending_updates, file_date)
                    pending_updates = []
                file_date = current_date + timedelta(days=1)
            
            current_date += timedelta(days=1)
        
        print(f"✅ Customer update generation complete")
    
    def _generate_customer_update(self, customer_id: str, update_date: datetime) -> Dict[str, Any]:
        """
        Generate a full customer record with updated fields
        Returns complete customer record with all 17 attributes
        """
        # Get current customer state
        customer = self.customers[customer_id].copy()
        
        # Choose what to update (weighted)
        update_type = random.choices(
            ['EMPLOYMENT_CHANGE', 'ACCOUNT_TIER', 'CONTACT_INFO', 'RISK_PROFILE'],
            weights=[35, 30, 25, 10]
        )[0]
        
        # Apply updates to the customer record
        if update_type == 'EMPLOYMENT_CHANGE':
            # Update employment-related fields
            if random.random() < 0.4:
                customer['employer'] = self.fake.company()
            if random.random() < 0.3:
                customer['position'] = random.choice(['Analyst', 'Manager', 'Director', 'Engineer', 'Consultant', 'Specialist'])
            if random.random() < 0.3:
                customer['employment_type'] = random.choice(self.employment_types)
            if random.random() < 0.4:
                current_idx = self.income_ranges.index(customer.get('income_range', '50K-75K'))
                # Bias towards increases
                if current_idx < len(self.income_ranges) - 1 and random.random() < 0.7:
                    customer['income_range'] = self.income_ranges[current_idx + 1]
                elif current_idx > 0:
                    customer['income_range'] = self.income_ranges[current_idx - 1]
        
        elif update_type == 'ACCOUNT_TIER':
            # Update account tier
            current_tier = customer.get('account_tier', 'STANDARD')
            current_idx = self.account_tiers.index(current_tier) if current_tier in self.account_tiers else 0
            
            # 60% upgrade, 40% downgrade
            if random.random() < 0.6 and current_idx < len(self.account_tiers) - 1:
                customer['account_tier'] = self.account_tiers[current_idx + 1]
            elif current_idx > 0:
                customer['account_tier'] = self.account_tiers[current_idx - 1]
        
        elif update_type == 'CONTACT_INFO':
            # Update contact information
            if random.random() < 0.5:
                customer['email'] = self.fake.email()
            if random.random() < 0.5:
                customer['phone'] = self.fake.phone_number()
            if random.random() < 0.3:
                customer['preferred_contact_method'] = random.choice(self.contact_methods)
        
        elif update_type == 'RISK_PROFILE':
            # Update risk classification or credit score
            if random.random() < 0.5:
                customer['risk_classification'] = random.choice(self.risk_classifications)
            if random.random() < 0.5:
                customer['credit_score_band'] = random.choice(self.credit_score_bands)
        
        # Add timestamp
        customer['insert_timestamp_utc'] = update_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Update the stored customer state
        self.customers[customer_id] = customer.copy()
        
        return customer
    
    def _save_update_file(self, updates: List[Dict], file_date: datetime):
        """Save updates to dated file in customer_updates/ subdirectory"""
        if not updates:
            return
        
        # Create customer_updates subdirectory
        customer_updates_dir = self.output_dir / 'customer_updates'
        customer_updates_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with date
        filename = f"customer_updates_{file_date.strftime('%Y-%m-%d')}.csv"
        filepath = customer_updates_dir / filename
        
        # Write CSV with ALL customer attributes (same as customers.csv)
        fieldnames = [
            'customer_id', 'first_name', 'family_name', 'date_of_birth', 'onboarding_date',
            'reporting_currency', 'has_anomaly', 'employer', 'position', 'employment_type',
            'income_range', 'account_tier', 'email', 'phone', 'preferred_contact_method',
            'risk_classification', 'credit_score_band', 'insert_timestamp_utc'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(updates)
        
        print(f"   💾 Saved {len(updates)} updates to {filename}")

def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate customer updates for SCD Type 2')
    parser.add_argument('--customer-file', default='generated_data/master_data/customers.csv',
                       help='Path to customers.csv')
    parser.add_argument('--output-dir', default='generated_data/master_data/customer_updates',
                       help='Output directory for update files')
    parser.add_argument('--start-date', default='2024-01-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-10-26',
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--updates-per-month', type=int, default=50,
                       help='Average updates per month')
    parser.add_argument('--file-frequency-days', type=int, default=30,
                       help='Save file every N days')
    
    args = parser.parse_args()
    
    # Generate updates
    generator = CustomerUpdateGenerator(args.customer_file, args.output_dir)
    generator.load_customers()
    generator.generate_updates(
        args.start_date,
        args.end_date,
        args.updates_per_month,
        args.file_frequency_days
    )

if __name__ == '__main__':
    main()
