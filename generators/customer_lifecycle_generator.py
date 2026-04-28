#!/usr/bin/env python3
"""
Customer Lifecycle Event Generator

Generates customer lifecycle events including:
- ONBOARDING (data-driven from customer onboarding date)
- ADDRESS_CHANGE (data-driven from address_update_generator.py)
- EMPLOYMENT_CHANGE (randomly generated)
- ACCOUNT_UPGRADE (randomly generated)
- ACCOUNT_CLOSE (randomly generated)
- REACTIVATION (randomly generated for churned customers)
- CHURN (randomly generated for 5% of customers)

Key Principles:
- ADDRESS_CHANGE events MUST match address update files timestamp
- Dormant customers have NO lifecycle events (by definition)
- Closed customers can only have REACTIVATION events
- Transaction-based dormancy (> 180 days no transactions) != lifecycle events
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass

from base_generator import init_random_seed

@dataclass
class LifecycleEvent:
    """Customer lifecycle event structure"""
    event_id: str
    customer_id: str
    event_type: str
    event_date: str  # YYYY-MM-DD
    event_timestamp_utc: str  # YYYY-MM-DD HH:MM:SS
    channel: str
    event_details: str  # JSON string
    previous_value: str
    new_value: str
    triggered_by: str
    requires_review: bool
    review_status: str
    review_date: str
    notes: str

@dataclass
class CustomerStatus:
    """Customer status history (SCD Type 2)"""
    status_id: str
    customer_id: str
    status: str
    status_reason: str
    status_start_date: str
    status_end_date: str
    is_current: bool
    linked_event_id: str

class CustomerLifecycleGenerator:
    """Generates customer lifecycle events and status history"""
    
    def __init__(self, customer_file: str, address_updates_dir: str, output_dir: str, customer_updates_dir: str = None, seed: int = 42):
        self.customer_file = customer_file
        self.address_updates_dir = Path(address_updates_dir)
        self.customer_updates_dir = Path(customer_updates_dir) if customer_updates_dir else None
        self.output_dir = Path(output_dir)
        self.customers = []
        self.address_changes = []  # Will be loaded from address update files
        self.customer_updates = []  # Will be loaded from customer update files
        
        # Initialize random state with seed for reproducibility
        self.fake = init_random_seed(seed)
        
        # Event type probabilities (for randomly generated events)
        # NOTE: EMPLOYMENT_CHANGE, ACCOUNT_UPGRADE, ACCOUNT_DOWNGRADE are now primarily data-driven
        # These weights apply only when customer_update files are not available
        self.event_type_weights = {
            'EMPLOYMENT_CHANGE': 25,  # Reduced - mostly data-driven now
            'ACCOUNT_UPGRADE': 20,    # Reduced - mostly data-driven now
            'ACCOUNT_DOWNGRADE': 15,  # NEW - mostly data-driven now
            'ACCOUNT_CLOSE': 15,
            'REACTIVATION': 15,
            'CHURN': 10
        }
        
        # Channel distribution
        self.channels = ['ONLINE', 'BRANCH', 'MOBILE', 'PHONE', 'SYSTEM']
        self.channel_weights = [35, 25, 30, 5, 5]
        
        # Triggered by options
        self.triggered_by_options = {
            'ONLINE': ['CUSTOMER_SELF_SERVICE', 'WEB_PORTAL'],
            'BRANCH': [f'BRANCH_OFFICER_{i:03d}' for i in range(1, 11)],
            'MOBILE': ['MOBILE_APP', 'CUSTOMER_SELF_SERVICE'],
            'PHONE': [f'CALL_CENTER_AGENT_{i:03d}' for i in range(1, 6)],
            'SYSTEM': ['SYSTEM_AUTO', 'BATCH_PROCESSOR', 'COMPLIANCE_ENGINE']
        }
        
    def load_customers(self):
        """Load existing customers from CSV file"""
        with open(self.customer_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.customers = list(reader)
        print(f"📋 Loaded {len(self.customers)} customers for lifecycle event generation")
    
    def load_address_changes(self):
        """
        Load address changes from address update files
        CRITICAL: These will be used to generate ADDRESS_CHANGE events
        with exact timestamp matching
        
        First loads initial addresses from customer_addresses.csv, then loads
        all updates from address_updates/ directory
        """
        # Step 1: Load initial addresses from customer_addresses.csv
        initial_addresses_file = self.output_dir / 'customer_addresses.csv'
        initial_count = 0
        
        if initial_addresses_file.exists():
            with open(initial_addresses_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.address_changes.append({
                        'customer_id': row['customer_id'],
                        'timestamp': row['insert_timestamp_utc'],
                        'street_address': row['street_address'],
                        'city': row['city'],
                        'state': row['state'],
                        'zipcode': row['zipcode'],
                        'country': row['country']
                    })
                    initial_count += 1
            print(f"📋 Loaded {initial_count} initial addresses from customer_addresses.csv")
        else:
            print(f"⚠️  Initial addresses file not found: {initial_addresses_file}")
        
        # Step 2: Load address updates from address_updates/ directory
        if not self.address_updates_dir.exists():
            print(f"⚠️  Address updates directory not found: {self.address_updates_dir}")
            return
        
        address_files = sorted(self.address_updates_dir.glob("customer_addresses_*.csv"))
        update_count = 0
        
        for address_file in address_files:
            with open(address_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.address_changes.append({
                        'customer_id': row['customer_id'],
                        'timestamp': row['insert_timestamp_utc'],
                        'street_address': row['street_address'],
                        'city': row['city'],
                        'state': row['state'],
                        'zipcode': row['zipcode'],
                        'country': row['country']
                    })
                    update_count += 1
        
        print(f"📋 Loaded {update_count} address updates from {len(address_files)} update files")
        print(f"📊 Total addresses: {len(self.address_changes)} (initial + updates)")
    
    def load_customer_updates(self):
        """
        Load customer updates from customer update files (full customer records)
        CRITICAL: These will be used to generate ACCOUNT_UPGRADE/DOWNGRADE and EMPLOYMENT_CHANGE events
        with exact timestamp matching
        
        New format: Full customer records with all 17 attributes + insert_timestamp_utc
        We detect changes by comparing with previous state
        """
        if not self.customer_updates_dir or not self.customer_updates_dir.exists():
            print(f"⚠️  Customer updates directory not found or not specified: {self.customer_updates_dir}")
            print(f"   Skipping data-driven customer update events...")
            return
        
        update_files = sorted(self.customer_updates_dir.glob("customer_updates_*.csv"))
        
        if not update_files:
            print(f"⚠️  No customer update files found in {self.customer_updates_dir}")
            return
        
        # Track previous state of each customer to detect changes
        customer_states = {}
        
        # Load initial state from customers.csv
        for customer in self.customers:
            customer_states[customer['customer_id']] = {
                'account_tier': customer.get('account_tier', ''),
                'employment_type': customer.get('employment_type', ''),
                'employer': customer.get('employer', ''),
                'position': customer.get('position', '')
            }
        
        # Process update files in chronological order
        for update_file in update_files:
            with open(update_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    customer_id = row['customer_id']
                    timestamp = row['insert_timestamp_utc']
                    
                    if customer_id not in customer_states:
                        continue  # Skip unknown customers
                    
                    prev_state = customer_states[customer_id]
                    
                    # Detect account tier changes (UPGRADE/DOWNGRADE)
                    if row['account_tier'] != prev_state['account_tier']:
                        old_tier = prev_state['account_tier']
                        new_tier = row['account_tier']
                        
                        # Determine if upgrade or downgrade
                        tier_rank = {'STANDARD': 1, 'SILVER': 2, 'GOLD': 3, 'PLATINUM': 4, 'PREMIUM': 5}
                        old_rank = tier_rank.get(old_tier, 0)
                        new_rank = tier_rank.get(new_tier, 0)
                        
                        event_type = 'ACCOUNT_UPGRADE' if new_rank > old_rank else 'ACCOUNT_DOWNGRADE'
                        
                        self.customer_updates.append({
                            'customer_id': customer_id,
                            'event_type': event_type,
                            'timestamp': timestamp,
                            'old_value': old_tier,
                            'new_value': new_tier
                        })
                    
                    # Detect employment changes
                    if (row['employment_type'] != prev_state['employment_type'] or
                        row['employer'] != prev_state['employer'] or
                        row['position'] != prev_state['position']):
                        
                        self.customer_updates.append({
                            'customer_id': customer_id,
                            'event_type': 'EMPLOYMENT_CHANGE',
                            'timestamp': timestamp,
                            'old_value': f"{prev_state['employer']} ({prev_state['position']})",
                            'new_value': f"{row['employer']} ({row['position']})"
                        })
                    
                    # Update state for next comparison
                    customer_states[customer_id] = {
                        'account_tier': row['account_tier'],
                        'employment_type': row['employment_type'],
                        'employer': row['employer'],
                        'position': row['position']
                    }
        
        print(f"📋 Loaded {len(self.customer_updates)} customer update events from {len(update_files)} files")
    
    def generate_event_id(self, counter: int) -> str:
        """Generate unique event ID"""
        return f"EVT_{counter:06d}"
    
    def generate_status_id(self, counter: int) -> str:
        """Generate unique status ID"""
        return f"STAT_{counter:06d}"
    
    def generate_onboarding_events(self) -> List[LifecycleEvent]:
        """
        Generate ONBOARDING events for all customers
        One event per customer at their onboarding date
        """
        events = []
        
        for idx, customer in enumerate(self.customers, start=1):
            onboarding_date = datetime.strptime(customer['onboarding_date'], '%Y-%m-%d')
            
            # Generate event timestamp (assume 10 AM UTC on onboarding day)
            event_timestamp = onboarding_date.replace(hour=10, minute=0, second=0)
            
            event_details = {
                "account_types": ["CHECKING"],
                "initial_deposit": round(random.uniform(100, 5000), 2),
                "referral_source": random.choice(['ONLINE_AD', 'BRANCH_VISIT', 'REFERRAL', 'PARTNER']),
                "kyc_verified": True,
                "welcome_package": True
            }
            
            event = LifecycleEvent(
                event_id=self.generate_event_id(idx),
                customer_id=customer['customer_id'],
                event_type='ONBOARDING',
                event_date=onboarding_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                channel=random.choices(['ONLINE', 'BRANCH', 'MOBILE'], weights=[40, 40, 20])[0],
                event_details=json.dumps(event_details),
                previous_value='PROSPECT',
                new_value='ACTIVE',
                triggered_by=random.choice(['CUSTOMER_SELF_SERVICE', 'BRANCH_OFFICER_001']),
                requires_review=False,
                review_status='NOT_REQUIRED',
                review_date='',
                notes='Initial customer onboarding'
            )
            events.append(event)
        
        print(f"✅ Generated {len(events)} ONBOARDING events")
        return events
    
    def generate_address_change_events(self, event_counter_start: int) -> List[LifecycleEvent]:
        """
        Generate ADDRESS_CHANGE events from address update data
        CRITICAL: Uses exact timestamps from address_update_generator.py
        """
        events = []
        
        # Group address changes by customer to track old/new addresses
        customer_addresses = {}
        for change in self.address_changes:
            cust_id = change['customer_id']
            if cust_id not in customer_addresses:
                customer_addresses[cust_id] = []
            customer_addresses[cust_id].append(change)
        
        # Sort by timestamp for each customer
        for cust_id in customer_addresses:
            customer_addresses[cust_id].sort(key=lambda x: x['timestamp'])
        
        event_counter = event_counter_start
        
        for cust_id, addresses in customer_addresses.items():
            # Skip first address (that's the initial address, not a change)
            for i in range(1, len(addresses)):
                old_addr = addresses[i-1]
                new_addr = addresses[i]
                
                # Parse timestamp (handle ISO 8601 format with T and Z)
                timestamp_str = new_addr['timestamp'].replace('T', ' ').replace('Z', '')
                # Remove microseconds if present
                if '.' in timestamp_str:
                    timestamp_str = timestamp_str.split('.')[0]
                event_dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                event_details = {
                    "old_address": {
                        "street": old_addr['street_address'],
                        "city": old_addr['city'],
                        "state": old_addr['state'],
                        "zipcode": old_addr['zipcode'],
                        "country": old_addr['country']
                    },
                    "new_address": {
                        "street": new_addr['street_address'],
                        "city": new_addr['city'],
                        "state": new_addr['state'],
                        "zipcode": new_addr['zipcode'],
                        "country": new_addr['country']
                    },
                    "reason": random.choice(['RELOCATION', 'MOVING', 'ADDRESS_CORRECTION']),
                    "verified": True
                }
                
                old_value = f"{old_addr['street_address']}, {old_addr['city']}"
                new_value = f"{new_addr['street_address']}, {new_addr['city']}"
                
                event = LifecycleEvent(
                    event_id=self.generate_event_id(event_counter),
                    customer_id=cust_id,
                    event_type='ADDRESS_CHANGE',
                    event_date=event_dt.strftime('%Y-%m-%d'),
                    event_timestamp_utc=new_addr['timestamp'],  # EXACT timestamp from address file
                    channel=random.choices(self.channels, weights=self.channel_weights)[0],
                    event_details=json.dumps(event_details),
                    previous_value=old_value[:500],  # Truncate to fit field
                    new_value=new_value[:500],
                    triggered_by='CUSTOMER_SELF_SERVICE',
                    requires_review=True if old_addr['country'] != new_addr['country'] else False,
                    review_status='PENDING' if old_addr['country'] != new_addr['country'] else 'NOT_REQUIRED',
                    review_date='',
                    notes='Address change notification received'
                )
                events.append(event)
                event_counter += 1
        
        print(f"✅ Generated {len(events)} ADDRESS_CHANGE events (data-driven from address updates)")
        return events
    
    def generate_customer_update_events(self, event_counter_start: int) -> List[LifecycleEvent]:
        """
        Generate lifecycle events from customer update data
        CRITICAL: Uses exact timestamps from customer_update_generator.py
        Generates: ACCOUNT_UPGRADE, ACCOUNT_DOWNGRADE, EMPLOYMENT_CHANGE (data-driven)
        
        New format: Simplified event-based format with event_type already determined
        """
        events = []
        event_counter = event_counter_start
        
        if not self.customer_updates:
            print("⚠️  No customer updates loaded, skipping data-driven customer update events")
            return events
        
        for update in self.customer_updates:
            # Parse timestamp (handle ISO 8601 format with T and Z)
            timestamp_str = update['timestamp'].replace('T', ' ').replace('Z', '')
            # Remove microseconds if present
            if '.' in timestamp_str:
                timestamp_str = timestamp_str.split('.')[0]
            event_dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            
            event_type = update['event_type']
            
            # Build event details based on type
            if event_type in ['ACCOUNT_UPGRADE', 'ACCOUNT_DOWNGRADE']:
                tier_change = 'UPGRADE' if event_type == 'ACCOUNT_UPGRADE' else 'DOWNGRADE'
                event_details = {
                    'old_tier': update['old_value'],
                    'new_tier': update['new_value'],
                    'tier_change_type': tier_change
                }
                
                event = LifecycleEvent(
                    event_id=self.generate_event_id(event_counter),
                    customer_id=update['customer_id'],
                    event_type=event_type,
                    event_date=event_dt.strftime('%Y-%m-%d'),
                    event_timestamp_utc=update['timestamp'],  # EXACT timestamp from update file
                    channel=random.choices(self.channels, weights=self.channel_weights)[0],
                    event_details=json.dumps(event_details),
                    previous_value=update['old_value'],
                    new_value=update['new_value'],
                    triggered_by='SYSTEM',
                    requires_review=False,
                    review_status='NOT_REQUIRED',
                    review_date='',
                    notes=f"Account tier {tier_change.lower()} from {update['old_value']} to {update['new_value']}"
                )
            
            elif event_type == 'EMPLOYMENT_CHANGE':
                event_details = {
                    'previous_employment': update['old_value'],
                    'new_employment': update['new_value'],
                    'change_type': 'EMPLOYMENT_CHANGE'
                }
                
                event = LifecycleEvent(
                    event_id=self.generate_event_id(event_counter),
                    customer_id=update['customer_id'],
                    event_type='EMPLOYMENT_CHANGE',
                    event_date=event_dt.strftime('%Y-%m-%d'),
                    event_timestamp_utc=update['timestamp'],  # EXACT timestamp from update file
                    channel=random.choices(self.channels, weights=self.channel_weights)[0],
                    event_details=json.dumps(event_details),
                    previous_value=update['old_value'][:200] if update['old_value'] else '',
                    new_value=update['new_value'][:200] if update['new_value'] else '',
                    triggered_by='SYSTEM',
                    requires_review=False,
                    review_status='NOT_REQUIRED',
                    review_date='',
                    notes=f"Employment details changed"
                )
            else:
                # Unknown event type, skip
                continue
            
            events.append(event)
            event_counter += 1
        
        print(f"✅ Generated {len(events)} lifecycle events from customer updates (data-driven)")
        return events
    
    def generate_random_events(self, event_counter_start: int) -> List[LifecycleEvent]:
        """
        Generate random lifecycle events for customers
        Constraints:
        - No events for dormant customers (they're inactive by definition)
        - Closed customers can only have REACTIVATION
        - Event sequencing with realistic time deltas
        """
        events = []
        event_counter = event_counter_start
        
        # We'll generate 0-3 events per customer (weighted towards 1-2)
        num_events_distribution = [0] * 30 + [1] * 40 + [2] * 25 + [3] * 5  # Percentages
        
        for customer in self.customers:
            customer_id = customer['customer_id']
            onboarding_date = datetime.strptime(customer['onboarding_date'], '%Y-%m-%d')
            
            # Decide number of random events for this customer
            num_events = random.choice(num_events_distribution)
            
            if num_events == 0:
                continue
            
            # Generate event sequence with time deltas
            current_date = onboarding_date
            customer_events = []
            
            for _ in range(num_events):
                # Time delta between events: 30-900 days, normal distribution around 180
                delta_days = int(random.gauss(180, 90))
                delta_days = max(30, min(900, delta_days))  # Clamp to reasonable range
                
                current_date = current_date + timedelta(days=delta_days)
                
                # Don't generate events in the future
                if current_date > datetime.now():
                    break
                
                # Select event type (weighted random)
                event_types = list(self.event_type_weights.keys())
                weights = list(self.event_type_weights.values())
                event_type = random.choices(event_types, weights=weights)[0]
                
                # Generate event based on type
                event = self._generate_specific_event(
                    event_counter, customer_id, event_type, current_date
                )
                
                if event:
                    customer_events.append(event)
                    event_counter += 1
            
            events.extend(customer_events)
        
        print(f"✅ Generated {len(events)} random lifecycle events")
        return events
    
    def _generate_specific_event(self, event_id_num: int, customer_id: str, 
                                  event_type: str, event_date: datetime) -> LifecycleEvent:
        """Generate a specific event type"""
        
        channel = random.choices(self.channels, weights=self.channel_weights)[0]
        triggered_by = random.choice(self.triggered_by_options[channel])
        
        if event_type == 'EMPLOYMENT_CHANGE':
            event_details = {
                "old_employer": self.fake.company(),
                "new_employer": self.fake.company(),
                "old_position": random.choice(['Analyst', 'Manager', 'Engineer', 'Consultant']),
                "new_position": random.choice(['Senior Analyst', 'Director', 'Senior Engineer', 'Lead Consultant']),
                "income_change_percent": round(random.uniform(-10, 40), 1),
                "employment_type": random.choice(['FULL_TIME', 'PART_TIME', 'CONTRACT'])
            }
            return LifecycleEvent(
                event_id=self.generate_event_id(event_id_num),
                customer_id=customer_id,
                event_type=event_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_date.strftime('%Y-%m-%d %H:%M:%S'),
                channel=channel,
                event_details=json.dumps(event_details),
                previous_value=event_details['old_employer'],
                new_value=event_details['new_employer'],
                triggered_by=triggered_by,
                requires_review=False,
                review_status='NOT_REQUIRED',
                review_date='',
                notes='Employment status updated'
            )
        
        elif event_type == 'ACCOUNT_UPGRADE':
            event_details = {
                "old_tier": random.choice(['STANDARD', 'SILVER']),
                "new_tier": random.choice(['GOLD', 'PLATINUM', 'PREMIUM']),
                "upgrade_reason": random.choice(['BALANCE_THRESHOLD', 'RELATIONSHIP_VALUE', 'CUSTOMER_REQUEST']),
                "new_benefits": random.sample(['FREE_TRANSFERS', 'INTEREST_RATE_BONUS', 'PRIORITY_SUPPORT', 'TRAVEL_INSURANCE'], 2),
                "annual_fee": round(random.uniform(0, 100), 2)
            }
            return LifecycleEvent(
                event_id=self.generate_event_id(event_id_num),
                customer_id=customer_id,
                event_type=event_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_date.strftime('%Y-%m-%d %H:%M:%S'),
                channel=channel,
                event_details=json.dumps(event_details),
                previous_value=event_details['old_tier'],
                new_value=event_details['new_tier'],
                triggered_by=triggered_by,
                requires_review=False,
                review_status='NOT_REQUIRED',
                review_date='',
                notes='Account tier upgraded'
            )
        
        elif event_type == 'ACCOUNT_DOWNGRADE':
            event_details = {
                "old_tier": random.choice(['GOLD', 'PLATINUM', 'PREMIUM']),
                "new_tier": random.choice(['STANDARD', 'SILVER']),
                "downgrade_reason": random.choice(['BALANCE_BELOW_THRESHOLD', 'CUSTOMER_REQUEST', 'FEE_REDUCTION', 'INACTIVITY']),
                "removed_benefits": random.sample(['FREE_TRANSFERS', 'INTEREST_RATE_BONUS', 'PRIORITY_SUPPORT', 'TRAVEL_INSURANCE'], 2),
                "annual_fee": 0.00
            }
            return LifecycleEvent(
                event_id=self.generate_event_id(event_id_num),
                customer_id=customer_id,
                event_type=event_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_date.strftime('%Y-%m-%d %H:%M:%S'),
                channel=channel,
                event_details=json.dumps(event_details),
                previous_value=event_details['old_tier'],
                new_value=event_details['new_tier'],
                triggered_by=triggered_by,
                requires_review=False,
                review_status='NOT_REQUIRED',
                review_date='',
                notes='Account tier downgraded'
            )
        
        elif event_type == 'ACCOUNT_CLOSE':
            event_details = {
                "closure_reason": random.choice(['VOLUNTARY', 'DUPLICATE_ACCOUNT', 'MOVING_ABROAD', 'DISSATISFACTION']),
                "final_balance": round(random.uniform(0, 1000), 2),
                "outstanding_items": random.randint(0, 2),
                "survey_completed": random.choice([True, False])
            }
            return LifecycleEvent(
                event_id=self.generate_event_id(event_id_num),
                customer_id=customer_id,
                event_type=event_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_date.strftime('%Y-%m-%d %H:%M:%S'),
                channel=channel,
                event_details=json.dumps(event_details),
                previous_value='ACTIVE',
                new_value='CLOSED',
                triggered_by=triggered_by,
                requires_review=True,
                review_status='APPROVED',
                review_date=(event_date + timedelta(days=1)).strftime('%Y-%m-%d'),
                notes='Account closure processed'
            )
        
        elif event_type == 'REACTIVATION':
            event_details = {
                "reactivation_reason": random.choice(['RETURNING_CUSTOMER', 'SERVICE_IMPROVEMENT', 'PROMOTIONAL_OFFER']),
                "dormant_period_days": random.randint(200, 500),
                "reactivation_offer": random.choice(['NO_FEE_3_MONTHS', 'BONUS_INTEREST', 'GIFT_CARD'])
            }
            return LifecycleEvent(
                event_id=self.generate_event_id(event_id_num),
                customer_id=customer_id,
                event_type=event_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_date.strftime('%Y-%m-%d %H:%M:%S'),
                channel=channel,
                event_details=json.dumps(event_details),
                previous_value='CLOSED',
                new_value='REACTIVATED',
                triggered_by=triggered_by,
                requires_review=True,
                review_status='APPROVED',
                review_date=(event_date + timedelta(days=1)).strftime('%Y-%m-%d'),
                notes='Customer reactivation approved'
            )
        
        elif event_type == 'CHURN':
            event_details = {
                "churn_reason": random.choice(['COMPETITOR_OFFER', 'POOR_SERVICE', 'FEES_TOO_HIGH', 'MOVED_ABROAD']),
                "retention_attempted": random.choice([True, False]),
                "final_survey_score": random.randint(1, 5)
            }
            return LifecycleEvent(
                event_id=self.generate_event_id(event_id_num),
                customer_id=customer_id,
                event_type=event_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_timestamp_utc=event_date.strftime('%Y-%m-%d %H:%M:%S'),
                channel=channel,
                event_details=json.dumps(event_details),
                previous_value='ACTIVE',
                new_value='CHURNED',
                triggered_by=triggered_by,
                requires_review=False,
                review_status='NOT_REQUIRED',
                review_date='',
                notes='Customer churned'
            )
        
        return None
    
    def generate_customer_status_history(self, events: List[LifecycleEvent]) -> List[CustomerStatus]:
        """
        Generate customer status history (SCD Type 2) based on lifecycle events
        """
        statuses = []
        status_counter = 1
        
        # Group events by customer
        customer_events = {}
        for event in events:
            if event.customer_id not in customer_events:
                customer_events[event.customer_id] = []
            customer_events[event.customer_id].append(event)
        
        # Sort events by date for each customer
        for cust_id in customer_events:
            customer_events[cust_id].sort(key=lambda e: e.event_timestamp_utc)
        
        for customer in self.customers:
            cust_id = customer['customer_id']
            events_for_customer = customer_events.get(cust_id, [])
            
            # Initial status (ACTIVE at onboarding)
            onboarding_date = customer['onboarding_date']
            
            # Find onboarding event
            onboarding_event = next((e for e in events_for_customer if e.event_type == 'ONBOARDING'), None)
            
            current_status = CustomerStatus(
                status_id=self.generate_status_id(status_counter),
                customer_id=cust_id,
                status='ACTIVE',
                status_reason='INITIAL_ONBOARDING',
                status_start_date=onboarding_date,
                status_end_date='',
                is_current=True,
                linked_event_id=onboarding_event.event_id if onboarding_event else ''
            )
            statuses.append(current_status)
            status_counter += 1
            
            # Process status-changing events
            for event in events_for_customer:
                if event.event_type in ['ACCOUNT_CLOSE', 'CHURN', 'REACTIVATION']:
                    # Close previous status
                    if statuses:
                        statuses[-1].status_end_date = event.event_date
                        statuses[-1].is_current = False
                    
                    # Create new status
                    new_status_value = 'CLOSED' if event.event_type in ['ACCOUNT_CLOSE', 'CHURN'] else 'REACTIVATED'
                    reason = f'{event.event_type}_EVENT'
                    
                    new_status = CustomerStatus(
                        status_id=self.generate_status_id(status_counter),
                        customer_id=cust_id,
                        status=new_status_value,
                        status_reason=reason,
                        status_start_date=event.event_date,
                        status_end_date='',
                        is_current=True,
                        linked_event_id=event.event_id
                    )
                    statuses.append(new_status)
                    status_counter += 1
        
        print(f"✅ Generated {len(statuses)} customer status records")
        return statuses
    
    def save_events(self, events: List[LifecycleEvent]):
        """Save lifecycle events to CSV files grouped by date (for consistency with other transactional data)"""
        events_dir = self.output_dir / 'customer_events'
        events_dir.mkdir(parents=True, exist_ok=True)
        
        # Group events by date
        events_by_date = {}
        for event in events:
            date = event.event_date  # Already in YYYY-MM-DD format
            if date not in events_by_date:
                events_by_date[date] = []
            events_by_date[date].append(event)
        
        fieldnames = [
            'EVENT_ID', 'CUSTOMER_ID', 'EVENT_TYPE', 'EVENT_DATE', 'EVENT_TIMESTAMP_UTC',
            'CHANNEL', 'EVENT_DETAILS', 'PREVIOUS_VALUE', 'NEW_VALUE', 'TRIGGERED_BY',
            'REQUIRES_REVIEW', 'REVIEW_STATUS', 'REVIEW_DATE', 'NOTES'
        ]
        
        # Save each date's events to a separate file
        for date, date_events in sorted(events_by_date.items()):
            output_file = events_dir / f'customer_events_{date}.csv'
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                # Use QUOTE_NONNUMERIC to properly quote all text fields (fixes comma-in-name issues)
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
                writer.writeheader()
                
                for event in date_events:
                    # Replace double quotes with single quotes in JSON for CSV compatibility
                    event_details = event.event_details.replace('"', "'") if event.event_details else ''
                    
                    writer.writerow({
                        'EVENT_ID': event.event_id,
                        'CUSTOMER_ID': event.customer_id,
                        'EVENT_TYPE': event.event_type,
                        'EVENT_DATE': event.event_date,
                        'EVENT_TIMESTAMP_UTC': event.event_timestamp_utc,
                        'CHANNEL': event.channel,
                        'EVENT_DETAILS': event_details,
                        'PREVIOUS_VALUE': event.previous_value,
                        'NEW_VALUE': event.new_value,
                        'TRIGGERED_BY': event.triggered_by,
                        'REQUIRES_REVIEW': event.requires_review,
                        'REVIEW_STATUS': event.review_status,
                        'REVIEW_DATE': event.review_date if event.review_date else '',
                        'NOTES': event.notes
                    })
        
        print(f"✅ Saved {len(events)} events to {len(events_by_date)} date-based files in {events_dir}")
    
    def save_status_history(self, statuses: List[CustomerStatus], filename: str = 'customer_status.csv'):
        """Save customer status history to CSV file"""
        output_file = self.output_dir / filename
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'STATUS_ID', 'CUSTOMER_ID', 'STATUS', 'STATUS_REASON',
                'STATUS_START_DATE', 'STATUS_END_DATE', 'IS_CURRENT', 'LINKED_EVENT_ID'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            
            for status in statuses:
                writer.writerow({
                    'STATUS_ID': status.status_id,
                    'CUSTOMER_ID': status.customer_id,
                    'STATUS': status.status,
                    'STATUS_REASON': status.status_reason,
                    'STATUS_START_DATE': status.status_start_date,
                    'STATUS_END_DATE': status.status_end_date,
                    'IS_CURRENT': status.is_current,
                    'LINKED_EVENT_ID': status.linked_event_id
                })
        
        print(f"✅ Saved {len(statuses)} status records to {output_file}")
    
    def generate_all(self):
        """Main generation method - orchestrates all event generation"""
        print("\n🎯 Starting Customer Lifecycle Event Generation")
        print("=" * 60)
        
        # Load data
        self.load_customers()
        self.load_address_changes()
        self.load_customer_updates()
        
        # Phase 1: Data-driven events
        print("\n📊 Phase 1: Generating data-driven events...")
        onboarding_events = self.generate_onboarding_events()
        
        address_change_events = self.generate_address_change_events(
            event_counter_start=len(onboarding_events) + 1
        )
        
        customer_update_events = self.generate_customer_update_events(
            event_counter_start=len(onboarding_events) + len(address_change_events) + 1
        )
        
        # Phase 2: Random events (only for event types not covered by data-driven events)
        print("\n🎲 Phase 2: Generating random lifecycle events...")
        random_events = self.generate_random_events(
            event_counter_start=len(onboarding_events) + len(address_change_events) + len(customer_update_events) + 1
        )
        
        # Combine all events
        all_events = onboarding_events + address_change_events + customer_update_events + random_events
        
        # Sort by timestamp
        all_events.sort(key=lambda e: e.event_timestamp_utc)
        
        # Generate status history
        print("\n📋 Generating customer status history...")
        status_history = self.generate_customer_status_history(all_events)
        
        # Save results
        print("\n💾 Saving generated data...")
        self.save_events(all_events)
        self.save_status_history(status_history)
        
        print("\n" + "=" * 60)
        print("✅ Customer Lifecycle Event Generation Complete!")
        print(f"   Total Events: {len(all_events)}")
        print(f"   - ONBOARDING: {len(onboarding_events)}")
        print(f"   - ADDRESS_CHANGE: {len(address_change_events)}")
        print(f"   - CUSTOMER UPDATES (data-driven): {len(customer_update_events)}")
        print(f"   - Other Events (random): {len(random_events)}")
        print(f"   Status Records: {len(status_history)}")
        print("=" * 60)

def main():
    """Main entry point for standalone execution"""
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python customer_lifecycle_generator.py <customer_file> <address_updates_dir> <output_dir> [customer_updates_dir]")
        print("Example: python customer_lifecycle_generator.py generated_data/master_data/customers.csv generated_data/master_data/address_updates generated_data/master_data [generated_data/master_data/customer_updates]")
        sys.exit(1)
    
    customer_file = sys.argv[1]
    address_updates_dir = sys.argv[2]
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "generated_data/master_data"
    customer_updates_dir = sys.argv[4] if len(sys.argv) > 4 else None
    
    generator = CustomerLifecycleGenerator(customer_file, address_updates_dir, output_dir, customer_updates_dir)
    generator.generate_all()

if __name__ == '__main__':
    main()

