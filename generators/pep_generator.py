#!/usr/bin/env python3
"""
PEP (Politically Exposed Persons) Data Generator
Generates synthetic PEP data for compliance testing and risk management
"""

import csv
import random
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import List, Optional
import argparse
from pathlib import Path
from faker import Faker

from base_generator import init_random_seed


@dataclass
class PEPRecord:
    """PEP (Politically Exposed Person) data structure"""
    pep_id: str
    full_name: str
    first_name: str
    last_name: str
    date_of_birth: Optional[date]
    nationality: str
    position_title: str
    organization: str
    country: str
    pep_category: str
    risk_level: str
    status: str
    start_date: Optional[date]
    end_date: Optional[date]
    reference_link: str
    source: str
    last_updated: date
    created_date: date


class PEPGenerator:
    """Generates synthetic PEP (Politically Exposed Persons) data"""
    
    def __init__(self, customer_file: str = None, seed: int = 42):
        # Initialize random state with seed for reproducibility
        self.fake = init_random_seed(seed)
        self.existing_customers = []
        
        # Load existing customers if file provided
        if customer_file and Path(customer_file).exists():
            self._load_existing_customers(customer_file)
        
        # EMEA countries supported by the bank
        self.countries = [
            'Germany', 'France', 'United Kingdom', 'Italy', 'Spain', 'Netherlands',
            'Belgium', 'Austria', 'Switzerland', 'Sweden', 'Norway', 'Denmark'
        ]
        
        # Country codes for reference links
        self.country_codes = {
            'Germany': 'de', 'France': 'fr', 'United Kingdom': 'uk', 'Italy': 'it',
            'Spain': 'es', 'Netherlands': 'nl', 'Belgium': 'be', 'Austria': 'at',
            'Switzerland': 'ch', 'Sweden': 'se', 'Norway': 'no', 'Denmark': 'dk'
        }
        
        # Political positions by category
        self.positions = {
            'DOMESTIC': [
                'Prime Minister', 'President', 'Minister of Finance', 'Minister of Defense',
                'Minister of Interior', 'Minister of Justice', 'Minister of Foreign Affairs',
                'Deputy Prime Minister', 'State Secretary', 'Parliamentary Secretary',
                'Member of Parliament', 'Senator', 'Regional Governor', 'Mayor',
                'Supreme Court Judge', 'Constitutional Court Judge', 'Central Bank Governor',
                'Deputy Central Bank Governor', 'Financial Regulator', 'Tax Authority Director'
            ],
            'FOREIGN': [
                'Ambassador', 'Consul General', 'Trade Representative', 'Cultural Attaché',
                'Military Attaché', 'Economic Counselor', 'Deputy Ambassador'
            ],
            'INTERNATIONAL_ORG': [
                'UN Secretary-General', 'EU Commissioner', 'ECB Executive Board Member',
                'IMF Managing Director', 'World Bank President', 'NATO Secretary General',
                'OECD Secretary-General', 'WHO Director-General', 'UNESCO Director-General',
                'European Parliament President', 'Council of Europe President'
            ],
            'FAMILY_MEMBER': [
                'Spouse of Prime Minister', 'Child of President', 'Parent of Minister',
                'Sibling of Governor', 'Spouse of Ambassador', 'Child of Judge'
            ],
            'CLOSE_ASSOCIATE': [
                'Business Partner', 'Close Friend', 'Financial Advisor', 'Legal Counsel',
                'Campaign Manager', 'Chief of Staff', 'Personal Assistant'
            ]
        }
        
        # Organizations by country
        self.organizations = {
            'Germany': ['Bundestag', 'Bundesrat', 'Federal Government', 'Bundesbank', 'BaFin'],
            'France': ['Assemblée Nationale', 'Sénat', 'Government of France', 'Banque de France', 'AMF'],
            'United Kingdom': ['House of Commons', 'House of Lords', 'HM Government', 'Bank of England', 'FCA'],
            'Italy': ['Camera dei Deputati', 'Senato', 'Government of Italy', 'Banca d\'Italia', 'CONSOB'],
            'Spain': ['Congreso', 'Senado', 'Government of Spain', 'Banco de España', 'CNMV'],
            'Netherlands': ['Tweede Kamer', 'Eerste Kamer', 'Government of Netherlands', 'DNB', 'AFM'],
            'Belgium': ['Chamber of Representatives', 'Senate', 'Government of Belgium', 'NBB', 'FSMA'],
            'Austria': ['Nationalrat', 'Bundesrat', 'Government of Austria', 'OeNB', 'FMA'],
            'Switzerland': ['National Council', 'Council of States', 'Federal Council', 'SNB', 'FINMA'],
            'Sweden': ['Riksdag', 'Government of Sweden', 'Sveriges Riksbank', 'Finansinspektionen'],
            'Norway': ['Storting', 'Government of Norway', 'Norges Bank', 'Finanstilsynet'],
            'Denmark': ['Folketing', 'Government of Denmark', 'Danmarks Nationalbank', 'Finanstilsynet']
        }
        
        # Risk levels based on position and category
        self.risk_mapping = {
            'DOMESTIC': {'high_risk': ['Prime Minister', 'President', 'Minister'], 'medium_risk': ['Member of Parliament', 'Judge'], 'low_risk': ['Mayor']},
            'FOREIGN': {'medium_risk': ['Ambassador'], 'low_risk': ['Consul', 'Attaché']},
            'INTERNATIONAL_ORG': {'critical_risk': ['Secretary-General', 'President', 'Managing Director'], 'high_risk': ['Commissioner', 'Director']},
            'FAMILY_MEMBER': {'medium_risk': ['Spouse', 'Child'], 'low_risk': ['Parent', 'Sibling']},
            'CLOSE_ASSOCIATE': {'low_risk': ['Business Partner', 'Advisor'], 'medium_risk': ['Campaign Manager', 'Chief of Staff']}
        }
    
    def _load_existing_customers(self, customer_file: str):
        """Load existing customers from CSV file"""
        try:
            with open(customer_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    self.existing_customers.append({
                        'customer_id': row['customer_id'],
                        'first_name': row['first_name'],
                        'family_name': row['family_name'],
                        'date_of_birth': row['date_of_birth']
                    })
            print(f"📋 Loaded {len(self.existing_customers)} existing customers for PEP matching")
        except Exception as e:
            print(f"⚠️ Warning: Could not load customer file {customer_file}: {e}")
    
    def _create_name_variations(self, first_name: str, last_name: str) -> tuple[str, str]:
        """Create slight variations in name spelling for PEP matching scenarios"""
        
        # Create multiple possible variations and pick one that's actually different
        variations = [
            # Nordic character variations
            (first_name.replace('ø', 'o').replace('å', 'a').replace('æ', 'ae'), last_name),
            (first_name, last_name.replace('ø', 'o').replace('å', 'a').replace('æ', 'ae')),
            # French accent variations
            (first_name.replace('é', 'e').replace('è', 'e').replace('ê', 'e'), last_name),
            (first_name, last_name.replace('é', 'e').replace('è', 'e').replace('ê', 'e')),
            # German umlaut variations
            (first_name.replace('ü', 'u').replace('ö', 'o').replace('ä', 'a'), last_name),
            (first_name, last_name.replace('ü', 'u').replace('ö', 'o').replace('ä', 'a')),
            # Add/remove silent letters
            (first_name + 'e' if len(first_name) > 3 and not first_name.endswith('e') else first_name, last_name),
            (first_name, last_name + 'e' if len(last_name) > 3 and not last_name.endswith('e') else last_name),
            (first_name[:-1] if first_name.endswith('e') and len(first_name) > 4 else first_name, last_name),
            (first_name, last_name[:-1] if last_name.endswith('e') and len(last_name) > 4 else last_name),
            # Common letter substitutions
            (first_name.replace('ph', 'f').replace('c', 'k', 1), last_name),
            (first_name, last_name.replace('ph', 'f').replace('c', 'k', 1)),
            # Prefix variations
            (first_name.replace('van ', ''), last_name),
            (first_name, last_name.replace('van ', '')),
            # Double letters
            (first_name.replace('tt', 't').replace('nn', 'n').replace('ll', 'l'), last_name),
            (first_name, last_name.replace('tt', 't').replace('nn', 'n').replace('ll', 'l')),
            # Add double letters
            (first_name.replace('t', 'tt', 1).replace('n', 'nn', 1), last_name),
            (first_name, last_name.replace('t', 'tt', 1).replace('n', 'nn', 1)),
        ]
        
        # Find variations that are actually different from original
        different_variations = [
            (fn, ln) for fn, ln in variations 
            if fn != first_name or ln != last_name
        ]
        
        if different_variations:
            return random.choice(different_variations)
        else:
            # If no variations worked, create a simple one
            if len(first_name) > 3:
                return (first_name + 'e', last_name)
            else:
                return (first_name, last_name + 'e')
    
    def _generate_risk_level(self, position_title: str, pep_category: str) -> str:
        """Determine risk level based on position and category"""
        category_risks = self.risk_mapping.get(pep_category, {})
        
        for risk_level, positions in category_risks.items():
            if any(pos.lower() in position_title.lower() for pos in positions):
                return risk_level.replace('_risk', '').upper()
        
        # Default risk levels by category
        defaults = {
            'DOMESTIC': 'MEDIUM',
            'FOREIGN': 'LOW',
            'INTERNATIONAL_ORG': 'HIGH',
            'FAMILY_MEMBER': 'LOW',
            'CLOSE_ASSOCIATE': 'LOW'
        }
        return defaults.get(pep_category, 'MEDIUM')
    
    def _generate_reference_link(self, country: str, pep_category: str, organization: str, full_name: str) -> str:
        """Generate realistic reference links to official sources"""
        country_code = self.country_codes.get(country, 'eu')
        name_slug = full_name.lower().replace(' ', '-').replace('.', '')
        
        if pep_category == 'DOMESTIC':
            if 'Parliament' in organization or 'Bundestag' in organization or 'Assemblée' in organization:
                return f"https://www.parliament.{country_code}/members/{name_slug}"
            elif 'Government' in organization:
                return f"https://www.government.{country_code}/officials/{name_slug}"
            elif 'Bank' in organization:
                return f"https://www.centralbank.{country_code}/leadership/{name_slug}"
            else:
                return f"https://www.official-register.{country_code}/pep/{name_slug}"
        
        elif pep_category == 'FOREIGN':
            return f"https://www.diplomatic-corps.{country_code}/staff/{name_slug}"
        
        elif pep_category == 'INTERNATIONAL_ORG':
            if 'UN' in organization:
                return f"https://www.un.org/leadership/{name_slug}"
            elif 'EU' in organization or 'European' in organization:
                return f"https://www.europa.eu/officials/{name_slug}"
            elif 'ECB' in organization:
                return f"https://www.ecb.europa.eu/leadership/{name_slug}"
            else:
                return f"https://www.international-org.org/officials/{name_slug}"
        
        elif pep_category in ['FAMILY_MEMBER', 'CLOSE_ASSOCIATE']:
            return f"https://www.pep-database.{country_code}/associates/{name_slug}"
        
        return f"https://www.compliance-database.{country_code}/pep/{name_slug}"
    
    def _generate_dates(self, status: str) -> tuple[Optional[date], Optional[date]]:
        """Generate start and end dates based on status"""
        current_date = date.today()
        
        if status == 'ACTIVE':
            # Active PEPs have start date but no end date
            start_date = self.fake.date_between(start_date='-10y', end_date='-1y')
            return start_date, None
        
        elif status == 'INACTIVE':
            # Inactive PEPs have both start and end dates
            start_date = self.fake.date_between(start_date='-20y', end_date='-2y')
            end_date = self.fake.date_between(start_date=start_date + timedelta(days=365), end_date='-1y')
            return start_date, end_date
        
        elif status == 'DECEASED':
            # Deceased PEPs have both dates, end date is death date
            start_date = self.fake.date_between(start_date='-30y', end_date='-5y')
            end_date = self.fake.date_between(start_date=start_date + timedelta(days=365), end_date='-1y')
            return start_date, end_date
        
        return None, None
    
    def generate_pep_record(self, pep_id: str, from_existing_customer: dict = None) -> PEPRecord:
        """Generate a single PEP record"""
        
        if from_existing_customer:
            # Use existing customer with slight name variations
            original_first = from_existing_customer['first_name']
            original_last = from_existing_customer['family_name']
            first_name, last_name = self._create_name_variations(original_first, original_last)
            full_name = f"{first_name} {last_name}"
            
            # Map customer countries to PEP countries
            country_mapping = {
                'Mette': 'Norway',  # Norwegian name
                'Mirthe': 'Netherlands',  # Dutch name
                'Johan': 'Sweden',  # Swedish name
                'Utz': 'Germany',  # German name
                'Pénélope': 'France'  # French name
            }
            country = country_mapping.get(original_first, random.choice(self.countries))
            
            # Parse date of birth
            try:
                date_of_birth = datetime.strptime(from_existing_customer['date_of_birth'], '%Y-%m-%d').date()
            except:
                date_of_birth = None
            
            # Higher chance of being family member or close associate for existing customers
            pep_category = random.choices(
                ['FAMILY_MEMBER', 'CLOSE_ASSOCIATE', 'DOMESTIC', 'FOREIGN'], 
                weights=[40, 30, 20, 10]
            )[0]
            
            print(f"🔍 Creating PEP record from existing customer: {original_first} {original_last} -> {first_name} {last_name}")
            
        else:
            # Generate completely new PEP record
            country = random.choice(self.countries)
            pep_category = random.choice(['DOMESTIC', 'FOREIGN', 'INTERNATIONAL_ORG', 'FAMILY_MEMBER', 'CLOSE_ASSOCIATE'])
            
            # Generate name based on country
            fake_local = Faker(['de_DE' if country == 'Germany' else
                               'fr_FR' if country == 'France' else
                               'en_GB' if country == 'United Kingdom' else
                               'it_IT' if country == 'Italy' else
                               'es_ES' if country == 'Spain' else
                               'nl_NL' if country == 'Netherlands' else
                               'fr_BE' if country == 'Belgium' else
                               'de_AT' if country == 'Austria' else
                               'de_CH' if country == 'Switzerland' else
                               'sv_SE' if country == 'Sweden' else
                               'no_NO' if country == 'Norway' else
                               'da_DK' if country == 'Denmark' else 'en_US'])
            
            first_name = fake_local.first_name()
            last_name = fake_local.last_name()
            full_name = f"{first_name} {last_name}"
            date_of_birth = fake_local.date_of_birth(minimum_age=25, maximum_age=85) if random.random() > 0.1 else None
        
        # Generate position and organization
        position_title = random.choice(self.positions[pep_category])
        organization = random.choice(self.organizations.get(country, ['Government']))
        
        # Generate other attributes
        risk_level = self._generate_risk_level(position_title, pep_category)
        status = random.choices(['ACTIVE', 'INACTIVE', 'DECEASED'], weights=[60, 30, 10])[0]
        start_date, end_date = self._generate_dates(status)
        
        # Generate reference link
        reference_link = self._generate_reference_link(country, pep_category, organization, full_name)
        
        # Generate audit dates
        created_date = self.fake.date_between(start_date='-2y', end_date='today')
        last_updated = self.fake.date_between(start_date=created_date, end_date='today')
        
        return PEPRecord(
            pep_id=pep_id,
            full_name=full_name,
            first_name=first_name,
            last_name=last_name,
            date_of_birth=date_of_birth,
            nationality=country,
            position_title=position_title,
            organization=organization,
            country=country,
            pep_category=pep_category,
            risk_level=risk_level,
            status=status,
            start_date=start_date,
            end_date=end_date,
            reference_link=reference_link,
            source=f"Official {country} Government Database",
            last_updated=last_updated,
            created_date=created_date
        )
    
    def generate_pep_data(self, num_records: int = 50) -> List[PEPRecord]:
        """Generate multiple PEP records"""
        pep_records = []
        
        # Calculate how many existing customers to include (1% or at least 1)
        num_from_existing = max(1, int(num_records * 0.01)) if self.existing_customers else 0
        
        # Generate PEP records from existing customers first
        existing_used = 0
        for i in range(num_records):
            pep_id = f"PEP_{i+1:05d}"
            
            if existing_used < num_from_existing and existing_used < len(self.existing_customers):
                # Use existing customer with variations
                customer = self.existing_customers[existing_used]
                pep_record = self.generate_pep_record(pep_id, from_existing_customer=customer)
                existing_used += 1
            else:
                # Generate completely new PEP record
                pep_record = self.generate_pep_record(pep_id)
            
            pep_records.append(pep_record)
        
        if num_from_existing > 0:
            print(f"🎯 Generated {num_from_existing} PEP records from existing customers out of {num_records} total")
        
        return pep_records
    
    def save_to_csv(self, pep_records: List[PEPRecord], filename: str):
        """Save PEP records to CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'pep_id', 'full_name', 'first_name', 'last_name', 'date_of_birth', 'nationality',
                'position_title', 'organization', 'country', 'pep_category', 'risk_level', 'status',
                'start_date', 'end_date', 'reference_link', 'source', 'last_updated', 'created_date'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in pep_records:
                writer.writerow({
                    'pep_id': record.pep_id,
                    'full_name': record.full_name,
                    'first_name': record.first_name,
                    'last_name': record.last_name,
                    'date_of_birth': record.date_of_birth.strftime('%Y-%m-%d') if record.date_of_birth else '',
                    'nationality': record.nationality,
                    'position_title': record.position_title,
                    'organization': record.organization,
                    'country': record.country,
                    'pep_category': record.pep_category,
                    'risk_level': record.risk_level,
                    'status': record.status,
                    'start_date': record.start_date.strftime('%Y-%m-%d') if record.start_date else '',
                    'end_date': record.end_date.strftime('%Y-%m-%d') if record.end_date else '',
                    'reference_link': record.reference_link,
                    'source': record.source,
                    'last_updated': record.last_updated.strftime('%Y-%m-%d'),
                    'created_date': record.created_date.strftime('%Y-%m-%d')
                })
        
        print(f"✅ Generated {len(pep_records)} PEP records and saved to {filename}")


def main():
    """Main function to generate PEP data"""
    parser = argparse.ArgumentParser(description="Generate PEP (Politically Exposed Persons) synthetic data")
    parser.add_argument("--num-records", type=int, default=50,
                       help="Number of PEP records to generate (default: 50)")
    parser.add_argument("--output-file", default="generated_data/master_data/pep_data.csv",
                       help="Output CSV file path")
    parser.add_argument("--output-dir", default="generated_data/master_data",
                       help="Output directory for PEP files")
    parser.add_argument("--customer-file", default="generated_data/master_data/customers.csv",
                       help="Path to existing customers CSV file for PEP matching")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate PEP data
    generator = PEPGenerator(customer_file=args.customer_file, seed=42)
    pep_records = generator.generate_pep_data(args.num_records)
    
    # Save to CSV
    output_file = Path(args.output_file)
    generator.save_to_csv(pep_records, str(output_file))
    
    # Display summary
    print(f"\n🎯 **PEP DATA GENERATION SUMMARY**")
    print(f"📊 Total Records: {len(pep_records)}")
    print(f"📁 Output File: {output_file}")
    print(f"")
    
    # Category breakdown
    categories = {}
    risk_levels = {}
    statuses = {}
    
    for record in pep_records:
        categories[record.pep_category] = categories.get(record.pep_category, 0) + 1
        risk_levels[record.risk_level] = risk_levels.get(record.risk_level, 0) + 1
        statuses[record.status] = statuses.get(record.status, 0) + 1
    
    print("📋 **Category Breakdown:**")
    for category, count in sorted(categories.items()):
        print(f"   {category}: {count}")
    
    print(f"")
    print("⚠️ **Risk Level Distribution:**")
    for risk, count in sorted(risk_levels.items()):
        print(f"   {risk}: {count}")
    
    print(f"")
    print("📈 **Status Distribution:**")
    for status, count in sorted(statuses.items()):
        print(f"   {status}: {count}")
    
    print(f"")
    print("🔗 **Sample Reference Links:**")
    for i, record in enumerate(pep_records[:3]):
        print(f"   {record.full_name}: {record.reference_link}")


if __name__ == "__main__":
    main()
