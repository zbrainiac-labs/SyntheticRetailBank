"""
Configuration module for Synthetic banking Data Generator - Summary Report
"""
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List
import os


@dataclass
class GeneratorConfig:
    """Configuration class for the payment statement generator with comprehensive validation"""
    
    # Random seed configuration
    random_seed: int = 42  # Master seed for deterministic data generation
    
    # Customer configuration
    num_customers: int = 10
    anomaly_percentage: float = 2.0  # Percentage of customers with anomalies
    
    # Time period configuration
    generation_period_months: int = 24
    start_date: Optional[datetime] = None
    
    # Transaction configuration
    avg_transactions_per_customer_per_month: float = 3.5
    
    # Currency configuration
    default_currency: str = "USD"
    available_currencies: List[str] = None
    
    # Amount configuration
    min_transaction_amount: float = 10.0
    max_transaction_amount: float = 50000.0
    
    # Anomaly configuration
    anomaly_multiplier_min: float = 5.0  # Minimum multiplier for anomalous amounts
    anomaly_multiplier_max: float = 20.0  # Maximum multiplier for anomalous amounts
    
    # Output configuration
    output_directory: str = "generated_data"
    
    def __post_init__(self):
        """Initialize derived attributes with comprehensive validation"""
        # Validate customer configuration
        if not isinstance(self.num_customers, int) or self.num_customers <= 0:
            raise ValueError(f"num_customers must be a positive integer, got: {self.num_customers}")
        
        if not isinstance(self.anomaly_percentage, (int, float)) or not 0 <= self.anomaly_percentage <= 100:
            raise ValueError(f"anomaly_percentage must be between 0 and 100, got: {self.anomaly_percentage}")
        
        # Validate time period configuration
        if not isinstance(self.generation_period_months, int) or self.generation_period_months <= 0:
            raise ValueError(f"generation_period_months must be a positive integer, got: {self.generation_period_months}")
        
        if self.generation_period_months > 120:  # 10 years max
            raise ValueError(f"generation_period_months cannot exceed 120 months (10 years), got: {self.generation_period_months}")
        
        # Validate transaction configuration
        if not isinstance(self.avg_transactions_per_customer_per_month, (int, float)) or self.avg_transactions_per_customer_per_month <= 0:
            raise ValueError(f"avg_transactions_per_customer_per_month must be positive, got: {self.avg_transactions_per_customer_per_month}")
        
        if self.avg_transactions_per_customer_per_month > 1000:
            raise ValueError(f"avg_transactions_per_customer_per_month cannot exceed 1000, got: {self.avg_transactions_per_customer_per_month}")
        
        # Validate currency configuration
        if not isinstance(self.default_currency, str) or len(self.default_currency) != 3:
            raise ValueError(f"default_currency must be a 3-character currency code, got: {self.default_currency}")
        
        if self.available_currencies is None:
            self.available_currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
        else:
            if not isinstance(self.available_currencies, list):
                raise ValueError(f"available_currencies must be a list, got: {type(self.available_currencies)}")
            
            for currency in self.available_currencies:
                if not isinstance(currency, str) or len(currency) != 3:
                    raise ValueError(f"All currencies must be 3-character codes, got: {currency}")
        
        # Validate amount configuration
        if not isinstance(self.min_transaction_amount, (int, float)) or self.min_transaction_amount <= 0:
            raise ValueError(f"min_transaction_amount must be positive, got: {self.min_transaction_amount}")
        
        if not isinstance(self.max_transaction_amount, (int, float)) or self.max_transaction_amount <= 0:
            raise ValueError(f"max_transaction_amount must be positive, got: {self.max_transaction_amount}")
        
        if self.min_transaction_amount >= self.max_transaction_amount:
            raise ValueError(f"min_transaction_amount ({self.min_transaction_amount}) must be less than max_transaction_amount ({self.max_transaction_amount})")
        
        if self.max_transaction_amount > 10000000:  # 10M max
            raise ValueError(f"max_transaction_amount cannot exceed 10,000,000, got: {self.max_transaction_amount}")
        
        # Validate anomaly configuration
        if not isinstance(self.anomaly_multiplier_min, (int, float)) or self.anomaly_multiplier_min <= 0:
            raise ValueError(f"anomaly_multiplier_min must be positive, got: {self.anomaly_multiplier_min}")
        
        if not isinstance(self.anomaly_multiplier_max, (int, float)) or self.anomaly_multiplier_max <= 0:
            raise ValueError(f"anomaly_multiplier_max must be positive, got: {self.anomaly_multiplier_max}")
        
        if self.anomaly_multiplier_min >= self.anomaly_multiplier_max:
            raise ValueError(f"anomaly_multiplier_min ({self.anomaly_multiplier_min}) must be less than anomaly_multiplier_max ({self.anomaly_multiplier_max})")
        
        if self.anomaly_multiplier_max > 1000:
            raise ValueError(f"anomaly_multiplier_max cannot exceed 1000, got: {self.anomaly_multiplier_max}")
        
        # Validate output configuration
        if not isinstance(self.output_directory, str) or not self.output_directory.strip():
            raise ValueError(f"output_directory must be a non-empty string, got: {self.output_directory}")
        
        # Check if output directory is writable
        try:
            os.makedirs(self.output_directory, exist_ok=True)
            # Test write access
            test_file = os.path.join(self.output_directory, '.write_test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except (OSError, PermissionError) as e:
            raise ValueError(f"output_directory must be writable: {e}")
        
        # Initialize derived attributes after validation
        if self.start_date is None:
            self.start_date = datetime.now() - timedelta(days=self.generation_period_months * 30)
    
    @property
    def end_date(self) -> datetime:
        """Calculate the end date based on start date and period"""
        return self.start_date + timedelta(days=self.generation_period_months * 30)
    
    @property
    def num_anomalous_customers(self) -> int:
        """Calculate number of customers that should have anomalies"""
        return max(1, int(self.num_customers * self.anomaly_percentage / 100))

