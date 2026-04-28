"""
Employee hierarchy data generation module with dynamic scaling
Generates client advisors, team leaders, and super team leaders based on actual customer distribution
"""
import csv
import random
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, field
from faker import Faker

from config import GeneratorConfig
from base_generator import BaseGenerator


@dataclass
class Employee:
    """Employee master data structure"""
    employee_id: str
    first_name: str
    family_name: str
    email: str
    phone: str
    date_of_birth: str
    hire_date: str
    employment_status: str
    country: str
    office_location: str
    position_level: str  # CLIENT_ADVISOR, TEAM_LEADER, SUPER_TEAM_LEADER
    manager_employee_id: str
    region: str
    performance_rating: float
    languages_spoken: str
    certifications: str
    insert_timestamp_utc: str
    assigned_customers: List[str] = field(default_factory=list)  # Temporary field for generation


@dataclass
class ClientAssignment:
    """Client-Advisor assignment data structure"""
    assignment_id: str
    customer_id: str
    advisor_employee_id: str
    assignment_start_date: str
    assignment_end_date: str
    assignment_reason: str
    is_current: bool
    insert_timestamp_utc: str


class EmployeeGenerator(BaseGenerator):
    """
    Generate hierarchical employee structure dynamically based on customer distribution
    
    Business Rules:
    - Each client advisor handles up to 200 customers
    - Each team leader manages up to 10 client advisors
    - Each super team leader oversees up to 10 team leaders
    - Structure scales automatically with customer count
    """
    
    def __init__(self, config: GeneratorConfig):
        super().__init__(config)
        self.employees: List[Employee] = []
        self.assignments: List[ClientAssignment] = []
        self.employee_counter = 1
        
        # Country-to-locale mapping for realistic employee names
        self.locale_map = {
            'Germany': 'de_DE',
            'France': 'fr_FR',
            'Italy': 'it_IT',
            'Spain': 'es_ES',
            'Netherlands': 'nl_NL',
            'Poland': 'pl_PL',
            'Sweden': 'sv_SE',
            'Norway': 'no_NO',
            'Denmark': 'da_DK',
            'Finland': 'fi_FI',
            'United Kingdom': 'en_GB',
            'Portugal': 'pt_PT',
            'Switzerland': 'de_CH'
        }
        
        # Regional groupings for team leader assignments
        self.regional_groupings = [
            {"region": "NORDIC", "country": "Sweden"},
            {"region": "CENTRAL_EUROPE", "country": "Germany"},
            {"region": "WESTERN_EUROPE", "country": "France"},
            {"region": "SOUTHERN_EUROPE", "country": "Italy"}
        ]
    
    def generate_employees_and_assignments(
        self, 
        customers_by_country: Dict[str, List[str]]
    ) -> Tuple[List[Employee], List[ClientAssignment]]:
        """
        Main entry point: Generate complete employee hierarchy and customer assignments
        
        Args:
            customers_by_country: Dict mapping country name -> list of customer IDs
        
        Returns:
            Tuple of (employees list, assignments list)
        """
        print("Starting dynamic employee hierarchy generation...")
        
        # Step 1: Calculate dynamic employee structure
        structure = self._calculate_employee_needs(customers_by_country)
        self._log_structure(structure)
        
        # Step 2: Generate employee hierarchy
        super_leaders = self._create_super_leaders(structure['super_leaders'])
        team_leaders = self._create_team_leaders(super_leaders, structure['team_leaders'])
        advisors = self._create_advisors_by_country(
            team_leaders,
            structure['advisors_by_country'],
            customers_by_country
        )
        
        # Step 3: Assign customers to advisors
        self._assign_customers_to_advisors(advisors)
        
        print(f"✅ Generated {len(self.employees)} employees and {len(self.assignments)} assignments")
        
        return self.employees, self.assignments
    
    def _calculate_employee_needs(self, customers_by_country: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Dynamically calculate employee hierarchy requirements based on customer distribution
        
        Args:
            customers_by_country: Dict mapping country -> list of customer_ids
        
        Returns:
            Dictionary with calculated structure requirements
        """
        advisors_by_country = {}
        
        for country, customer_list in customers_by_country.items():
            customer_count = len(customer_list)
            # Each advisor handles up to 200 customers
            advisors_needed = math.ceil(customer_count / 200)
            
            advisors_by_country[country] = {
                'count': advisors_needed,
                'customers': customer_count,
                'customer_ids': customer_list
            }
        
        total_advisors = sum(info['count'] for info in advisors_by_country.values())
        
        # Each team leader manages up to 10 advisors
        team_leaders_needed = math.ceil(total_advisors / 10)
        
        # Each super team leader oversees up to 10 team leaders
        super_leaders_needed = math.ceil(team_leaders_needed / 10)
        
        return {
            'super_leaders': super_leaders_needed,
            'team_leaders': team_leaders_needed,
            'total_advisors': total_advisors,
            'advisors_by_country': advisors_by_country,
            'countries': sorted(customers_by_country.keys()),
            'total_customers': sum(len(cust) for cust in customers_by_country.values())
        }
    
    def _log_structure(self, structure: Dict[str, Any]):
        """Log the calculated employee structure"""
        print("📊 Dynamic Employee Structure Calculated:")
        print(f"   Total Customers: {structure['total_customers']}")
        print(f"   Countries: {len(structure['countries'])}")
        print(f"   Client Advisors: {structure['total_advisors']}")
        print(f"   Team Leaders: {structure['team_leaders']}")
        print(f"   Super Team Leaders: {structure['super_leaders']}")
        print(f"   Total Employees: {structure['super_leaders'] + structure['team_leaders'] + structure['total_advisors']}")
        print("")
        print("   Advisors by Country:")
        for country in structure['countries']:
            info = structure['advisors_by_country'][country]
            print(f"      {country:20s}: {info['customers']:4d} customers → {info['count']} advisor(s)")
    
    def _create_super_leaders(self, count: int) -> List[Employee]:
        """Create super team leaders (top of hierarchy)"""
        print(f"\n👔 Creating {count} Super Team Leader(s)...")
        super_leaders = []
        
        for i in range(count):
            emp = self._create_employee(
                position_level="SUPER_TEAM_LEADER",
                country="Switzerland",  # Corporate HQ
                region="EMEA",
                manager_id=None
            )
            super_leaders.append(emp)
            print(f"   ✓ {emp.employee_id}: {emp.first_name} {emp.family_name} (EMEA HQ)")
        
        return super_leaders
    
    def _create_team_leaders(self, super_leaders: List[Employee], count: int) -> List[Employee]:
        """Create team leaders and distribute under super leaders"""
        print(f"\n👥 Creating {count} Team Leader(s)...")
        team_leaders = []
        
        for i in range(count):
            # Distribute team leaders evenly across super leaders
            manager = super_leaders[i % len(super_leaders)]
            
            # Assign region cyclically
            region_info = self.regional_groupings[i % len(self.regional_groupings)]
            
            emp = self._create_employee(
                position_level="TEAM_LEADER",
                country=region_info['country'],
                region=region_info['region'],
                manager_id=manager.employee_id
            )
            team_leaders.append(emp)
            print(f"   ✓ {emp.employee_id}: {emp.first_name} {emp.family_name} ({emp.region})")
        
        return team_leaders
    
    def _create_advisors_by_country(
        self,
        team_leaders: List[Employee],
        advisors_by_country: Dict[str, Dict],
        customers_by_country: Dict[str, List[str]]
    ) -> List[Employee]:
        """
        Create client advisors dynamically based on customer distribution per country
        
        Args:
            team_leaders: List of team leader employees
            advisors_by_country: Dict with advisor counts per country
            customers_by_country: Dict with customer lists per country
        
        Returns:
            List of advisor employees with customers pre-assigned
        """
        print(f"\n💼 Creating Client Advisors...")
        advisors = []
        
        # Sort countries by customer count (largest first) for even distribution
        countries_sorted = sorted(
            advisors_by_country.items(),
            key=lambda x: x[1]['customers'],
            reverse=True
        )
        
        # Round-robin assignment to team leaders
        current_tl_idx = 0
        
        for country, info in countries_sorted:
            advisors_needed = info['count']
            customers_in_country = customers_by_country[country]
            
            print(f"   Country: {country} ({len(customers_in_country)} customers → {advisors_needed} advisor(s))")
            
            # Create multiple advisors for this country if needed (>200 customers)
            for advisor_num in range(advisors_needed):
                # Assign to team leader in round-robin fashion
                team_leader = team_leaders[current_tl_idx % len(team_leaders)]
                
                emp = self._create_employee(
                    position_level="CLIENT_ADVISOR",
                    country=country,
                    region=team_leader.region,
                    manager_id=team_leader.employee_id
                )
                
                # Assign customers to this advisor (up to 200)
                start_idx = advisor_num * 200
                end_idx = min(start_idx + 200, len(customers_in_country))
                emp.assigned_customers = customers_in_country[start_idx:end_idx]
                
                advisors.append(emp)
                print(f"      ✓ {emp.employee_id}: {emp.first_name} {emp.family_name} ({len(emp.assigned_customers)} clients)")
                
                current_tl_idx += 1
        
        return advisors
    
    def _create_employee(
        self,
        position_level: str,
        country: str,
        region: str,
        manager_id: str
    ) -> Employee:
        """
        Create a single employee with realistic attributes
        
        Args:
            position_level: SUPER_TEAM_LEADER, TEAM_LEADER, or CLIENT_ADVISOR
            country: Employee's home country
            region: Regional assignment
            manager_id: Manager's employee_id (None for top level)
        
        Returns:
            Employee object with all attributes populated
        """
        emp_id = f"EMP_{self.employee_counter:05d}"
        self.employee_counter += 1
        
        # Use locale-appropriate faker for realistic names
        locale = self.locale_map.get(country, 'en_GB')
        faker = Faker(locale)
        
        first_name = faker.first_name()
        family_name = faker.last_name()
        
        # Generate realistic dates
        hire_date = faker.date_between(start_date='-15y', end_date='today')
        dob = faker.date_of_birth(minimum_age=25, maximum_age=65)
        
        employee = Employee(
            employee_id=emp_id,
            first_name=first_name,
            family_name=family_name,
            email=f"{first_name.lower()}.{family_name.lower()}@syntheticbank.com"[:50],
            phone=faker.phone_number(),
            date_of_birth=dob.strftime('%Y-%m-%d'),
            hire_date=hire_date.strftime('%Y-%m-%d'),
            employment_status="ACTIVE",
            country=country,
            office_location=f"{faker.city()}, {country}",
            position_level=position_level,
            manager_employee_id=manager_id if manager_id else "",
            region=region,
            performance_rating=round(random.uniform(2.5, 5.0), 2),
            languages_spoken=self._get_languages(country),
            certifications=self._get_certifications(position_level),
            insert_timestamp_utc=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        )
        
        self.employees.append(employee)
        return employee
    
    def _assign_customers_to_advisors(self, advisors: List[Employee]):
        """Create assignment records for customer-advisor relationships"""
        print(f"\n📝 Creating customer-advisor assignments...")
        assignment_counter = 1
        
        for advisor in advisors:
            for customer_id in advisor.assigned_customers:
                # Assignment starts on the advisor's hire date
                assignment = ClientAssignment(
                    assignment_id=f"ASSGN_{assignment_counter:06d}",
                    customer_id=customer_id,
                    advisor_employee_id=advisor.employee_id,
                    assignment_start_date=advisor.hire_date,
                    assignment_end_date="",  # Current assignment (no end date)
                    assignment_reason="INITIAL_ONBOARDING",
                    is_current=True,
                    insert_timestamp_utc=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                )
                self.assignments.append(assignment)
                assignment_counter += 1
        
        print(f"   ✓ Created {len(self.assignments)} assignments")
    
    def _get_languages(self, country: str) -> str:
        """Get typical languages spoken for employees in this country"""
        lang_map = {
            'Germany': 'German, English',
            'France': 'French, English',
            'Italy': 'Italian, English',
            'Spain': 'Spanish, English',
            'Netherlands': 'Dutch, English, German',
            'Sweden': 'Swedish, English',
            'Norway': 'Norwegian, English',
            'Denmark': 'Danish, English',
            'Finland': 'Finnish, Swedish, English',
            'Poland': 'Polish, English',
            'United Kingdom': 'English',
            'Portugal': 'Portuguese, English, Spanish',
            'Switzerland': 'German, French, Italian, English'
        }
        return lang_map.get(country, 'English')
    
    def _get_certifications(self, position_level: str) -> str:
        """Get relevant professional certifications by position level"""
        cert_map = {
            'CLIENT_ADVISOR': 'CFA Level I, Financial Planning Certification',
            'TEAM_LEADER': 'CFA Level II, Leadership Certification, Risk Management',
            'SUPER_TEAM_LEADER': 'CFA Charter, MBA, Executive Leadership Program'
        }
        return cert_map.get(position_level, '')
    
    def generate(self) -> Dict[str, Any]:
        """
        Generate employee data - implementation of abstract method.
        Note: This method is not typically called directly. Use generate_employees_and_assignments() instead.
        """
        return {
            'employees': self.employees,
            'assignments': self.assignments,
            'total_employees': len(self.employees),
            'total_assignments': len(self.assignments)
        }
    
    def write_employees_to_csv(self, filename: str):
        """Write employee data to CSV file"""
        print(f"\n💾 Writing {len(self.employees)} employees to {filename}...")
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'employee_id', 'first_name', 'family_name', 'email', 'phone',
                'date_of_birth', 'hire_date', 'employment_status', 'country',
                'office_location', 'position_level', 'manager_employee_id',
                'region', 'performance_rating', 'languages_spoken',
                'certifications', 'insert_timestamp_utc'
            ])
            
            # Data rows
            for emp in self.employees:
                writer.writerow([
                    emp.employee_id,
                    emp.first_name,
                    emp.family_name,
                    emp.email,
                    emp.phone,
                    emp.date_of_birth,
                    emp.hire_date,
                    emp.employment_status,
                    emp.country,
                    emp.office_location,
                    emp.position_level,
                    emp.manager_employee_id,
                    emp.region,
                    emp.performance_rating,
                    emp.languages_spoken,
                    emp.certifications,
                    emp.insert_timestamp_utc
                ])
        
        print(f"   ✓ Employees written successfully")
    
    def write_assignments_to_csv(self, filename: str):
        """Write client-advisor assignments to CSV file"""
        print(f"\n💾 Writing {len(self.assignments)} assignments to {filename}...")
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'assignment_id', 'customer_id', 'advisor_employee_id',
                'assignment_start_date', 'assignment_end_date',
                'assignment_reason', 'is_current', 'insert_timestamp_utc'
            ])
            
            # Data rows
            for assignment in self.assignments:
                writer.writerow([
                    assignment.assignment_id,
                    assignment.customer_id,
                    assignment.advisor_employee_id,
                    assignment.assignment_start_date,
                    assignment.assignment_end_date,
                    assignment.assignment_reason,
                    assignment.is_current,
                    assignment.insert_timestamp_utc
                ])
        
        print(f"   ✓ Assignments written successfully")

