#!/usr/bin/env python3
"""
Address Update Generator for SCD Type 2 Processing

This module generates customer address update files with dates in filenames
to simulate address changes over time for SCD Type 2 processing.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
from faker import Faker

from base_generator import init_random_seed

@dataclass
class AddressUpdate:
    """Address update record structure"""
    customer_id: str
    street_address: str
    city: str
    state: str
    zipcode: str
    country: str
    insert_timestamp_utc: str

class AddressUpdateGenerator:
    """Generates address update files for SCD Type 2 processing"""
    
    def __init__(self, customer_file: str, output_dir: str, seed: int = 42):
        self.customer_file = customer_file
        self.output_dir = Path(output_dir)
        self.customers = []
        
        # Initialize random state with seed for reproducibility (used for locale-specific Faker instances)
        init_random_seed(seed)
        self.emea_locales = [
            'no_NO', 'nl_NL', 'sv_SE', 'de_DE', 'fr_FR', 
            'it_IT', 'en_GB', 'da_DK', 'fr_BE', 'de_AT', 'de_CH'
        ]
        self.locale_to_country = {
            'no_NO': 'Norway',
            'nl_NL': 'Netherlands', 
            'sv_SE': 'Sweden',
            'de_DE': 'Germany',
            'fr_FR': 'France',
            'it_IT': 'Italy',
            'en_GB': 'United Kingdom',
            'da_DK': 'Denmark',
            'fr_BE': 'Belgium',
            'de_AT': 'Austria',
            'de_CH': 'Switzerland'
        }
        
    def load_customers(self):
        """Load existing customers from CSV file"""
        with open(self.customer_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.customers = list(reader)
        print(f"📋 Loaded {len(self.customers)} customers for address updates")
    
    def generate_address_updates(self, num_update_files: int = 6, updates_per_file: int = None) -> List[str]:
        """Generate multiple address update files with dates"""
        if not self.customers:
            self.load_customers()
        
        # Create address_updates directory
        address_updates_dir = self.output_dir / "address_updates"
        address_updates_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate updates per file if not specified
        if updates_per_file is None:
            # Update approximately 5-15% of customers per file
            updates_per_file = max(5, int(len(self.customers) * random.uniform(0.05, 0.15)))
        
        generated_files = []
        
        # Generate update files over the past 12 months
        base_date = datetime.now()
        
        for i in range(num_update_files):
            # Generate dates going backwards in time (most recent first)
            days_back = random.randint(30 + (i * 45), 90 + (i * 45))
            update_date = base_date - timedelta(days=days_back)
            
            # Create filename with date
            filename = f"customer_addresses_{update_date.strftime('%Y-%m-%d')}.csv"
            filepath = address_updates_dir / filename
            
            # Select random customers for this update batch
            customers_to_update = random.sample(self.customers, min(updates_per_file, len(self.customers)))
            
            # Generate address updates
            address_updates = []
            for customer in customers_to_update:
                # Generate new address for this customer
                country = self._get_customer_country(customer['customer_id'])
                locale = self._get_locale_for_country(country)
                fake_local = Faker(locale)
                
                # Generate timestamp for this update (during business hours)
                update_timestamp = self._generate_business_timestamp(update_date)
                
                # Generate new address
                address_data = self._generate_emea_address(fake_local, country)
                
                address_update = AddressUpdate(
                    customer_id=customer['customer_id'],
                    street_address=address_data['street_address'],
                    city=address_data['city'],
                    state=address_data['state'],
                    zipcode=address_data['zipcode'],
                    country=address_data['country'],
                    insert_timestamp_utc=update_timestamp
                )
                address_updates.append(address_update)
            
            # Save to CSV file
            self._save_address_updates_to_csv(address_updates, filepath)
            generated_files.append(str(filepath))
            
            print(f"✅ Generated {len(address_updates)} address updates: {filename}")
        
        return generated_files
    
    def _get_customer_country(self, customer_id: str) -> str:
        """Get country for customer (simplified - random EMEA country)"""
        # In a real scenario, this would look up the customer's current country
        # For now, we'll assign based on customer ID hash for consistency
        countries = list(self.locale_to_country.values())
        hash_value = hash(customer_id) % len(countries)
        return countries[hash_value]
    
    def _get_locale_for_country(self, country: str) -> str:
        """Get locale for country"""
        for locale, country_name in self.locale_to_country.items():
            if country_name == country:
                return locale
        return 'en_GB'  # Default fallback
    
    def _generate_business_timestamp(self, date: datetime) -> str:
        """Generate a business hours timestamp for the given date"""
        # Random time between 9 AM and 5 PM
        hour = random.randint(9, 17)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        timestamp = date.replace(hour=hour, minute=minute, second=second)
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    def _generate_emea_address(self, fake_local: Faker, country: str) -> Dict[str, Any]:
        """Generate EMEA-specific address components"""
        
        # Country-specific address generation
        if country == 'Netherlands':
            street_address = f"{fake_local.street_name()} {random.randint(1, 200)}"
            city = fake_local.city()
            state = fake_local.state() if hasattr(fake_local, 'state') else ''  # Dutch provinces
            zipcode = fake_local.postcode()
            
        elif country == 'Germany':
            street_address = f"{fake_local.street_name()} {random.randint(1, 150)}"
            city = fake_local.city()
            state = fake_local.state() if hasattr(fake_local, 'state') else ''  # German states
            zipcode = fake_local.postcode()
            
        elif country == 'France':
            street_address = f"{random.randint(1, 200)} {fake_local.street_name()}"
            city = fake_local.city()
            state = fake_local.state() if hasattr(fake_local, 'state') else ''  # French regions
            zipcode = fake_local.postcode()
            
        elif country == 'United Kingdom':
            street_address = f"{random.randint(1, 200)} {fake_local.street_name()}"
            city = fake_local.city()
            state = fake_local.county() if hasattr(fake_local, 'county') else ''  # UK counties
            zipcode = fake_local.postcode()
            
        elif country == 'Italy':
            street_address = f"{fake_local.street_name()} {random.randint(1, 200)}"
            city = fake_local.city()
            state = fake_local.state() if hasattr(fake_local, 'state') else ''  # Italian regions
            zipcode = fake_local.postcode()
            
        elif country in ['Sweden', 'Norway', 'Denmark']:
            street_address = f"{fake_local.street_name()} {random.randint(1, 150)}"
            city = fake_local.city()
            state = fake_local.state() if country == 'Sweden' and hasattr(fake_local, 'state') else ''  # Only Sweden has states/counties
            zipcode = fake_local.postcode()
            
        elif country in ['Belgium', 'Austria', 'Switzerland']:
            street_address = f"{fake_local.street_name()} {random.randint(1, 150)}"
            city = fake_local.city()
            state = fake_local.state() if country == 'Austria' and hasattr(fake_local, 'state') else ''  # Only Austria has states
            zipcode = fake_local.postcode()
            
        else:
            # Default fallback
            street_address = f"{fake_local.street_name()} {random.randint(1, 200)}"
            city = fake_local.city()
            state = ''
            zipcode = fake_local.postcode()
        
        return {
            'street_address': street_address,
            'city': city,
            'state': state or '',  # Ensure empty string instead of None
            'zipcode': zipcode,
            'country': country
        }
    
    def _save_address_updates_to_csv(self, address_updates: List[AddressUpdate], filepath: Path):
        """Save address updates to CSV file"""
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['customer_id', 'street_address', 'city', 'state', 'zipcode', 'country', 'insert_timestamp_utc']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for update in address_updates:
                writer.writerow({
                    'customer_id': update.customer_id,
                    'street_address': update.street_address,
                    'city': update.city,
                    'state': update.state,
                    'zipcode': update.zipcode,
                    'country': update.country,
                    'insert_timestamp_utc': update.insert_timestamp_utc
                })

def main():
    """Main function for standalone execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate customer address update files for SCD Type 2 processing')
    parser.add_argument('--customer-file', required=True, help='Path to customers.csv file')
    parser.add_argument('--output-dir', default='generated_data/master_data', help='Output directory for address update files')
    parser.add_argument('--num-files', type=int, default=6, help='Number of address update files to generate')
    parser.add_argument('--updates-per-file', type=int, help='Number of address updates per file (default: 5-15% of customers)')
    
    args = parser.parse_args()
    
    generator = AddressUpdateGenerator(args.customer_file, args.output_dir, seed=42)
    generated_files = generator.generate_address_updates(args.num_files, args.updates_per_file)
    
    print(f"\n🎉 Generated {len(generated_files)} address update files:")
    for filepath in generated_files:
        print(f"   📁 {filepath}")

if __name__ == "__main__":
    main()
