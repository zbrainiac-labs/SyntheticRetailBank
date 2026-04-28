"""
Customer data generation module
"""
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass
from faker import Faker

from config import GeneratorConfig
from base_generator import BaseGenerator


@dataclass
class Customer:
    """Customer master data structure for EMEA retail banking with extended attributes"""
    customer_id: str
    first_name: str
    family_name: str
    date_of_birth: str
    onboarding_date: str
    reporting_currency: str
    has_anomaly: bool
    # Extended attributes (previously in snapshot)
    employer: str = ""
    position: str = ""
    employment_type: str = ""
    income_range: str = ""
    account_tier: str = ""
    email: str = ""
    phone: str = ""
    preferred_contact_method: str = ""
    risk_classification: str = ""
    credit_score_band: str = ""


@dataclass
class CustomerAddress:
    """Customer address data structure with insert timestamp"""
    customer_id: str
    street_address: str
    city: str
    state: str
    zipcode: str
    country: str
    insert_timestamp_utc: str  # UTC timestamp when record was inserted


class CustomerGenerator(BaseGenerator):
    """Generates realistic EMEA customer data with localized information"""
    
    def __init__(self, config: GeneratorConfig):
        super().__init__(config)
        # EMEA locales for realistic customer data
        self.emea_locales = [
            'en_GB',  # United Kingdom
            'de_DE',  # Germany
            'fr_FR',  # France
            'it_IT',  # Italy
            'es_ES',  # Spain
            'nl_NL',  # Netherlands
            'pt_PT',  # Portugal
            'pl_PL',  # Poland
            'sv_SE',  # Sweden
            'no_NO',  # Norway
            'da_DK',  # Denmark
            'fi_FI',  # Finland
        ]
        
        # Country mappings for consistent data
        self.locale_to_country = {
            'en_GB': 'United Kingdom',
            'de_DE': 'Germany', 
            'fr_FR': 'France',
            'it_IT': 'Italy',
            'es_ES': 'Spain',
            'nl_NL': 'Netherlands',
            'pt_PT': 'Portugal',
            'pl_PL': 'Poland',
            'sv_SE': 'Sweden',
            'no_NO': 'Norway',
            'da_DK': 'Denmark',
            'fi_FI': 'Finland',
        }
        
        # Country to currency mappings for reporting currency
        self.country_to_currency = {
            'United Kingdom': 'GBP',
            'Germany': 'EUR',
            'France': 'EUR',
            'Italy': 'EUR',
            'Spain': 'EUR',
            'Netherlands': 'EUR',
            'Portugal': 'EUR',
            'Poland': 'PLN',
            'Sweden': 'SEK',
            'Norway': 'NOK',
            'Denmark': 'DKK',
            'Finland': 'EUR',
        }
        
        # self.fake is already initialized by BaseGenerator._init_random_state()
        self.customers: List[Customer] = []
        self.customer_addresses: List[CustomerAddress] = []
    
    def generate_customers(self) -> tuple[List[Customer], List[CustomerAddress]]:
        """Generate customers and their addresses with SCD Type 2 support"""
        customers = []
        anomalous_customer_ids = self._select_anomalous_customers()
        
        for i in range(self.config.num_customers):
            customer_id = f"CUST_{i+1:05d}"
            has_anomaly = customer_id in anomalous_customer_ids
            
            # Select random EMEA locale for this customer
            locale = random.choice(self.emea_locales)
            country = self.locale_to_country[locale]
            fake_local = Faker(locale)
            
            # Generate random onboarding date within the generation period
            onboarding_date = self._generate_onboarding_date()
            
            # Generate split address components
            address_data = self._generate_emea_address(fake_local, country)
            
            # Get reporting currency based on country
            reporting_currency = self.country_to_currency[country]
            
            # Generate extended attributes
            employment_types = ['FULL_TIME', 'PART_TIME', 'CONTRACT', 'SELF_EMPLOYED', 'RETIRED']
            account_tiers = ['STANDARD', 'SILVER', 'GOLD', 'PLATINUM', 'PREMIUM']
            income_ranges = ['<30K', '30K-50K', '50K-75K', '75K-100K', '100K-150K', '>150K']
            positions = ['Analyst', 'Manager', 'Engineer', 'Consultant', 'Specialist']
            risk_classifications = ['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH']
            credit_score_bands = ['POOR', 'FAIR', 'GOOD', 'VERY_GOOD', 'EXCELLENT']
            contact_methods = ['EMAIL', 'PHONE', 'SMS', 'MOBILE_APP', 'POST']
            
            # Create customer record with all attributes
            customer = Customer(
                customer_id=customer_id,
                first_name=fake_local.first_name(),
                family_name=fake_local.last_name(),
                date_of_birth=fake_local.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
                onboarding_date=onboarding_date.strftime("%Y-%m-%d"),
                reporting_currency=reporting_currency,
                has_anomaly=has_anomaly,
                # Extended attributes
                employer=self.fake.company(),
                position=random.choice(positions),
                employment_type=random.choice(employment_types),
                income_range=random.choice(income_ranges),
                account_tier=random.choices(account_tiers, weights=[30, 30, 20, 15, 5])[0],
                email=self.fake.email(),
                phone=self.fake.phone_number(),
                preferred_contact_method=random.choice(contact_methods),
                risk_classification=random.choices(risk_classifications, weights=[50, 30, 15, 5])[0],
                credit_score_band=random.choices(credit_score_bands, weights=[5, 15, 30, 30, 20])[0]
            )
            
            # Generate address history for this customer (SCD Type 2)
            customer_addresses = self._generate_address_history(customer_id, fake_local, country, onboarding_date, address_data)
            self.customer_addresses.extend(customer_addresses)
            customers.append(customer)
        
        self.customers = customers
        return customers, self.customer_addresses
    
    def _select_anomalous_customers(self) -> set:
        """Select which customers will have anomalous behavior"""
        num_anomalous = self.config.num_anomalous_customers
        customer_indices = random.sample(range(self.config.num_customers), num_anomalous)
        return {f"CUST_{i+1:05d}" for i in customer_indices}
    
    def _generate_onboarding_date(self) -> datetime:
        """Generate a random onboarding date within the generation period"""
        # Most customers should be onboarded before the transaction period starts
        # Some might be onboarded during the period
        days_before_start = random.randint(30, 365 * 3)  # 1 month to 3 years before
        if random.random() < 0.2:  # 20% chance of onboarding during the period
            days_offset = random.randint(0, self.config.generation_period_months * 30)
            return self.config.start_date + timedelta(days=days_offset)
        else:
            return self.config.start_date - timedelta(days=days_before_start)
    
    def _generate_emea_address(self, fake_local: Faker, country: str) -> Dict[str, str]:
        """Generate EMEA-specific address components"""
        try:
            street_address = fake_local.street_address()
            city = fake_local.city()
            # Handle state/region differences across EMEA countries
            if country in ['United Kingdom', 'Germany', 'Spain', 'Italy']:
                state = fake_local.state() if hasattr(fake_local, 'state') and fake_local.state else ''
            else:
                state = fake_local.administrative_unit() if hasattr(fake_local, 'administrative_unit') else ''
            
            # Handle postal code variations
            zipcode = fake_local.postcode()
            
        except AttributeError:
            # Fallback for locales that don't support all address components
            street_address = fake_local.street_address()
            city = fake_local.city() 
            state = ''
            zipcode = fake_local.postcode() if hasattr(fake_local, 'postcode') else fake_local.zipcode()
        
        return {
            'street_address': street_address,
            'city': city, 
            'state': state or '',
            'zipcode': zipcode
        }
    
    def _generate_address_history(self, customer_id: str, fake_local: Faker, country: str, onboarding_date: datetime, initial_address_data: dict) -> List[CustomerAddress]:
        """Generate address history for a customer with insert timestamps"""
        addresses = []
        
        # 20% chance of having address changes during the period
        has_address_changes = random.random() < 0.2
        
        if has_address_changes:
            # Generate 1-3 address changes over time
            num_changes = random.randint(1, 3)
            
            # Calculate dates for address changes (spread over the generation period)
            end_date = datetime.now()
            period_days = (end_date - onboarding_date).days
            
            change_dates = [onboarding_date]  # Start with onboarding date
            for i in range(num_changes):
                # Spread changes over the period, but not too close to previous change
                min_days = max(30, period_days // (num_changes + 1) * (i + 1) - 60)
                max_days = min(period_days - 30, period_days // (num_changes + 1) * (i + 2))
                if min_days < max_days:
                    change_date = onboarding_date + timedelta(days=random.randint(min_days, max_days))
                    change_dates.append(change_date)
            
            change_dates.sort()
            
            # Create address records with insert timestamps
            for i, insert_date in enumerate(change_dates):
                if i == 0:
                    # First address (from onboarding)
                    address_data = initial_address_data
                else:
                    # Subsequent addresses (address changes)
                    address_data = self._generate_emea_address(fake_local, country)
                
                address = CustomerAddress(
                    customer_id=customer_id,
                    street_address=address_data['street_address'],
                    city=address_data['city'],
                    state=address_data['state'],
                    zipcode=address_data['zipcode'],
                    country=country,
                    insert_timestamp_utc=insert_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                )
                addresses.append(address)
        else:
            # Single address from onboarding
            address = CustomerAddress(
                customer_id=customer_id,
                street_address=initial_address_data['street_address'],
                city=initial_address_data['city'],
                state=initial_address_data['state'],
                zipcode=initial_address_data['zipcode'],
                country=country,
                insert_timestamp_utc=onboarding_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            )
            addresses.append(address)
        
        return addresses
    
    def save_customers_to_csv(self, filename: str) -> None:
        """Save customer master data to CSV file with all extended attributes"""
        if not self.customers:
            raise ValueError("No customers generated. Call generate_customers() first.")
        
        fieldnames = [
            "customer_id", "first_name", "family_name", "date_of_birth", "onboarding_date", 
            "reporting_currency", "has_anomaly",
            # Extended attributes
            "employer", "position", "employment_type", "income_range", "account_tier",
            "email", "phone", "preferred_contact_method", 
            "risk_classification", "credit_score_band"
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for customer in self.customers:
                writer.writerow({
                    "customer_id": customer.customer_id,
                    "first_name": customer.first_name,
                    "family_name": customer.family_name,
                    "date_of_birth": customer.date_of_birth,
                    "onboarding_date": customer.onboarding_date,
                    "reporting_currency": customer.reporting_currency,
                    "has_anomaly": customer.has_anomaly,
                    # Extended attributes
                    "employer": customer.employer,
                    "position": customer.position,
                    "employment_type": customer.employment_type,
                    "income_range": customer.income_range,
                    "account_tier": customer.account_tier,
                    "email": customer.email,
                    "phone": customer.phone,
                    "preferred_contact_method": customer.preferred_contact_method,
                    "risk_classification": customer.risk_classification,
                    "credit_score_band": customer.credit_score_band
                })
    
    def save_addresses_to_csv(self, filename: str) -> None:
        """Save customer address data to CSV file with insert timestamps"""
        if not self.customer_addresses:
            raise ValueError("No customer addresses generated. Call generate_customers() first.")
        
        fieldnames = ["customer_id", "street_address", "city", "state", "zipcode", "country", "insert_timestamp_utc"]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for address in self.customer_addresses:
                writer.writerow({
                    "customer_id": address.customer_id,
                    "street_address": address.street_address,
                    "city": address.city,
                    "state": address.state,
                    "zipcode": address.zipcode,
                    "country": address.country,
                    "insert_timestamp_utc": address.insert_timestamp_utc
                })
    
    def save_to_csv(self, filename: str) -> None:
        """Legacy method - saves customers only for backward compatibility"""
        self.save_customers_to_csv(filename)
    
    def get_customer_by_id(self, customer_id: str) -> Customer:
        """Get customer by ID"""
        for customer in self.customers:
            if customer.customer_id == customer_id:
                return customer
        raise ValueError(f"Customer {customer_id} not found")
    
    def get_anomalous_customers(self) -> List[Customer]:
        """Get list of customers marked for anomalous behavior"""
        return [customer for customer in self.customers if customer.has_anomaly]
    
    def generate_fuzzy_matching_test_customer(self) -> Customer:
        """
        Generate a customer with a name similar to 'YURY TOPCHEV' for fuzzy matching testing.
        This creates a test case for PEP screening and fuzzy name matching algorithms.
        
        Name variations used for testing:
        - Character substitution: YURI TOPCHEV (Y→I)
        - Phonetic similarity: YURA TOPCHEV (nickname variation)
        - Character transposition: YUYR TOPCHEV (Y-R swap)
        """
        # Use a variation that's similar but not identical to "YURY TOPCHEV"
        # This tests fuzzy matching algorithms without using the exact target name
        test_first_name = "YURI"  # Character substitution: YURY → YURI
        test_last_name = "TOPCHEV"  # Keep last name identical for testing
        
        # Generate a customer ID that's clearly marked as a test customer
        test_customer_id = f"CUST_{self.config.num_customers + 1:05d}"
        
        # Use a realistic onboarding date
        onboarding_date = self._generate_onboarding_date()
        
        # Use EUR as reporting currency (common in EMEA)
        reporting_currency = "EUR"
        
        # Create the test customer
        test_customer = Customer(
            customer_id=test_customer_id,
            first_name=test_first_name,
            family_name=test_last_name,
            date_of_birth=self.fake.date_of_birth(minimum_age=25, maximum_age=65).strftime("%Y-%m-%d"),
            onboarding_date=onboarding_date.strftime("%Y-%m-%d"),
            reporting_currency=reporting_currency,
            has_anomaly=False  # Not marked as anomalous, just for fuzzy matching test
        )
        
        return test_customer
    
    def generate_fuzzy_matching_test_address(self, customer_id: str) -> CustomerAddress:
        """
        Generate an address for the fuzzy matching test customer.
        Uses a realistic EMEA address pattern.
        """
        # Use a realistic EMEA address (Germany for this test)
        fake_de = Faker('de_DE')
        
        address_data = {
            'street_address': fake_de.street_address(),
            'city': fake_de.city(),
            'state': fake_de.state(),
            'zipcode': fake_de.postcode()
        }
        
        # Use current timestamp for insert
        insert_timestamp = self.get_utc_timestamp()
        
        test_address = CustomerAddress(
            customer_id=customer_id,
            street_address=address_data['street_address'],
            city=address_data['city'],
            state=address_data['state'],
            zipcode=address_data['zipcode'],
            country="Germany",
            insert_timestamp_utc=insert_timestamp
        )
        
        return test_address
    
    def add_fuzzy_matching_test_customer(self) -> tuple[Customer, CustomerAddress]:
        """
        Add a fuzzy matching test customer to the existing customer list.
        This customer has a name similar to 'YURY TOPCHEV' for testing fuzzy matching algorithms.
        
        Returns:
            tuple: (test_customer, test_address)
        """
        # Generate the test customer
        test_customer = self.generate_fuzzy_matching_test_customer()
        test_address = self.generate_fuzzy_matching_test_address(test_customer.customer_id)
        
        # Add to existing lists
        self.customers.append(test_customer)
        self.customer_addresses.append(test_address)
        
        return test_customer, test_address
    
    def group_customers_by_current_country(self) -> Dict[str, List[str]]:
        """
        Group customers by their current country for employee assignment.
        Returns a dictionary mapping country -> list of customer_ids
        """
        customers_by_country = {}
        
        # Get the most recent address for each customer
        customer_current_address = {}
        for addr in self.customer_addresses:
            cust_id = addr.customer_id
            # Keep the most recent address (addresses are generated in chronological order)
            customer_current_address[cust_id] = addr
        
        # Group by country
        for cust_id, addr in customer_current_address.items():
            country = addr.country
            if country not in customers_by_country:
                customers_by_country[country] = []
            customers_by_country[country].append(cust_id)
        
        return customers_by_country
    
    def generate(self) -> Dict[str, Any]:
        """Generate customer data - implementation of abstract method"""
        customers, addresses = self.generate_customers()
        return {
            'customers': customers,
            'addresses': addresses,
            'total_customers': len(customers),
            'anomalous_customers': len([c for c in customers if c.has_anomaly])
        }


