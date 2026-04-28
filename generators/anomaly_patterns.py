"""
Anomaly pattern definitions for suspicious transaction detection
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from enum import Enum

from config import GeneratorConfig


class AnomalyType(Enum):
    """Types of anomalies that can be generated"""
    LARGE_AMOUNT = "large_amount"
    HIGH_FREQUENCY = "high_frequency"
    UNUSUAL_COUNTERPARTY = "unusual_counterparty"
    ROUND_AMOUNT = "round_amount"
    OFF_HOURS = "off_hours"
    RAPID_SUCCESSION = "rapid_succession"
    NEW_BENEFICIARY_LARGE = "new_beneficiary_large"


class AnomalyPatternGenerator:
    """Generates various anomaly patterns for suspicious transactions"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        
    def generate_anomaly_characteristics(self, customer_id: str) -> Dict[str, Any]:
        """Generate anomaly characteristics for a specific customer"""
        # Each anomalous customer gets 1-3 different anomaly types
        num_anomaly_types = random.randint(1, 3)
        anomaly_types = random.sample(list(AnomalyType), num_anomaly_types)
        
        characteristics = {
            "customer_id": customer_id,
            "anomaly_types": anomaly_types,
            "anomaly_start_date": self._generate_anomaly_start_date(),
            "anomaly_duration_days": random.randint(1, 90),  # Anomalies last 1-90 days
            "suspicious_counterparties": self._generate_suspicious_counterparties(),
            "large_amount_threshold": self._calculate_large_amount_threshold(),
            "high_frequency_threshold": random.randint(10, 25),  # transactions per day
        }
        
        return characteristics
    
    def _generate_anomaly_start_date(self) -> datetime:
        """Generate when anomalous behavior starts"""
        # Anomalies can start anywhere in the period, with some buffer
        total_days = self.config.generation_period_months * 30
        
        # For short periods, start anomalies in the first half
        if total_days <= 60:  # 2 months or less
            min_days = 1
            max_days = max(2, total_days // 2)
        else:
            # For longer periods, start after first quarter but before last month
            min_days = total_days // 4
            max_days = total_days - 30
        
        # Ensure valid range
        min_days = max(1, min_days)
        max_days = max(min_days + 1, max_days)
        
        days_from_start = random.randint(min_days, max_days)
        return self.config.start_date + timedelta(days=days_from_start)
    
    def _generate_suspicious_counterparties(self) -> List[str]:
        """Generate suspicious counterparty accounts"""
        suspicious_patterns = [
            "OFF_SHORE_",
            "SHELL_CORP_",
            "CRYPTO_EX_",
            "CASH_SERV_",
            "MONEY_TRANS_"
        ]
        
        counterparties = []
        for i in range(random.randint(1, 3)):
            pattern = random.choice(suspicious_patterns)
            account_num = f"{random.randint(1000000, 9999999):07d}"
            counterparties.append(f"{pattern}{account_num}")
        
        return counterparties
    
    def _calculate_large_amount_threshold(self) -> float:
        """Calculate threshold for large amounts based on customer's normal behavior"""
        # Large amounts are typically 5-20x the normal transaction size
        normal_amount = (self.config.min_transaction_amount + self.config.max_transaction_amount) / 2
        multiplier = random.uniform(
            self.config.anomaly_multiplier_min,
            self.config.anomaly_multiplier_max
        )
        return normal_amount * multiplier
    
    def should_apply_anomaly(self, customer_id: str, anomaly_chars: Dict[str, Any], 
                           transaction_date: datetime) -> Tuple[bool, List[AnomalyType]]:
        """Determine if anomaly should be applied to a transaction on a given date"""
        anomaly_start = anomaly_chars["anomaly_start_date"]
        anomaly_end = anomaly_start + timedelta(days=anomaly_chars["anomaly_duration_days"])
        
        # Check if we're in the anomaly period
        if not (anomaly_start <= transaction_date <= anomaly_end):
            return False, []
        
        # Randomly apply anomalies (not every transaction in the period is anomalous)
        if random.random() > 0.3:  # 30% chance of anomaly during anomaly period
            return False, []
        
        # Select which anomaly types to apply (can be multiple)
        applicable_anomalies = []
        for anomaly_type in anomaly_chars["anomaly_types"]:
            if random.random() < 0.7:  # 70% chance each type applies
                applicable_anomalies.append(anomaly_type)
        
        return len(applicable_anomalies) > 0, applicable_anomalies
    
    def apply_anomaly_to_transaction(self, base_transaction: Dict[str, Any], 
                                   anomaly_types: List[AnomalyType],
                                   anomaly_chars: Dict[str, Any]) -> Dict[str, Any]:
        """Apply anomaly patterns to a base transaction"""
        transaction = base_transaction.copy()
        
        for anomaly_type in anomaly_types:
            if anomaly_type == AnomalyType.LARGE_AMOUNT:
                transaction["amount"] = self._apply_large_amount_anomaly(
                    transaction["amount"], anomaly_chars
                )
                transaction["description"] += " [LARGE_TRANSFER]"
            
            elif anomaly_type == AnomalyType.UNUSUAL_COUNTERPARTY:
                transaction["counterparty_account"] = random.choice(
                    anomaly_chars["suspicious_counterparties"]
                )
                transaction["description"] += " [SUSPICIOUS_COUNTERPARTY]"
            
            elif anomaly_type == AnomalyType.ROUND_AMOUNT:
                transaction["amount"] = self._apply_round_amount_anomaly(
                    transaction["amount"]
                )
                transaction["description"] += " [ROUND_AMOUNT]"
            
            elif anomaly_type == AnomalyType.OFF_HOURS:
                new_booking_date = self._apply_off_hours_anomaly(
                    transaction["booking_date"]
                )
                transaction["booking_date"] = new_booking_date
                # Recalculate value date based on new booking date
                # Note: This is a simplified approach - in practice you'd want to import
                # the value date calculation logic or pass it as a parameter
                if hasattr(transaction["value_date"], 'date'):
                    # value_date is a datetime object
                    transaction["value_date"] = max(new_booking_date, transaction["value_date"])
                else:
                    # value_date is already a date object
                    transaction["value_date"] = max(new_booking_date.date(), transaction["value_date"])
                transaction["description"] += " [OFF_HOURS]"
            
            elif anomaly_type == AnomalyType.NEW_BENEFICIARY_LARGE:
                transaction["counterparty_account"] = f"NEW_BENEF_{random.randint(100000, 999999)}"
                transaction["amount"] = max(
                    transaction["amount"] * random.uniform(3, 8),
                    anomaly_chars["large_amount_threshold"] * 0.5
                )
                transaction["description"] += " [NEW_LARGE_BENEFICIARY]"
        
        return transaction
    
    def _apply_large_amount_anomaly(self, base_amount: float, 
                                  anomaly_chars: Dict[str, Any]) -> float:
        """Apply large amount anomaly"""
        threshold = anomaly_chars["large_amount_threshold"]
        return max(threshold, base_amount * random.uniform(2, 5))
    
    def _apply_round_amount_anomaly(self, base_amount: float) -> float:
        """Apply round amount anomaly (suspicious round numbers)"""
        round_amounts = [1000, 5000, 10000, 25000, 50000, 100000]
        # Choose a round amount close to the base amount
        suitable_amounts = [amt for amt in round_amounts if amt >= base_amount * 0.5]
        if suitable_amounts:
            return random.choice(suitable_amounts)
        return round(base_amount, -3)  # Round to nearest thousand
    
    def _apply_off_hours_anomaly(self, booking_date: datetime) -> datetime:
        """Apply off-hours transaction timing"""
        # Generate time between 11 PM and 6 AM, or during weekends
        if random.random() < 0.5:  # Night transactions
            hour = random.choice([23, 0, 1, 2, 3, 4, 5])
            minute = random.randint(0, 59)
        else:  # Weekend transactions
            # Move to weekend if not already
            days_to_weekend = (5 - booking_date.weekday()) % 7
            if days_to_weekend == 0:
                days_to_weekend = 1
            booking_date = booking_date + timedelta(days=days_to_weekend)
            hour = random.randint(9, 18)
            minute = random.randint(0, 59)
        
        return booking_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
