#!/usr/bin/env python3
"""
Mortgage Request Email Generator

Generates professional email communications for mortgage applications
using synthetic customer data. Creates both customer-facing and internal emails.
"""

import csv
import random
import smtplib
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import json


@dataclass
class Customer:
    customer_id: str
    first_name: str
    family_name: str
    date_of_birth: str
    onboarding_date: str
    has_anomaly: bool


@dataclass
class CustomerAddress:
    customer_id: str
    street_address: str
    city: str
    state: str
    zipcode: str
    country: str
    insert_timestamp_utc: str


@dataclass
class MortgageRequest:
    application_id: str
    customer: Customer
    address: CustomerAddress
    loan_amount: float
    property_value: float
    down_payment: float
    loan_term_years: int
    interest_rate: float
    monthly_income: float
    employment_type: str
    employment_years: int
    property_type: str
    application_date: datetime
    purpose: str
    credit_score: int
    debt_to_income_ratio: float


class MortgageEmailGenerator:
    """Generates mortgage request emails for synthetic customers"""
    
    def __init__(self, output_dir: str = "generated_data/emails"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Country-specific mortgage data
        self.country_mortgage_data = {
            'Norway': {
                'avg_property_value': 450000,
                'max_loan_to_value': 0.85,
                'typical_rate': 3.25,
                'currency': 'NOK',
                'email_domain': 'norbank.no',
                'phone': '47-555-BANK'
            },
            'Netherlands': {
                'avg_property_value': 380000,
                'max_loan_to_value': 0.90,
                'typical_rate': 2.85,
                'currency': 'EUR',
                'email_domain': 'nlbank.nl',
                'phone': '31-555-BANK'
            },
            'Sweden': {
                'avg_property_value': 420000,
                'max_loan_to_value': 0.85,
                'typical_rate': 3.15,
                'currency': 'SEK',
                'email_domain': 'svbank.se',
                'phone': '46-555-BANK'
            },
            'Germany': {
                'avg_property_value': 320000,
                'max_loan_to_value': 0.80,
                'typical_rate': 3.45,
                'currency': 'EUR',
                'email_domain': 'debank.de',
                'phone': '49-555-BANK'
            },
            'France': {
                'avg_property_value': 290000,
                'max_loan_to_value': 0.80,
                'typical_rate': 2.95,
                'currency': 'EUR',
                'email_domain': 'frbank.fr',
                'phone': '33-555-BANK'
            },
            'Italy': {
                'avg_property_value': 275000,
                'max_loan_to_value': 0.80,
                'typical_rate': 3.15,
                'currency': 'EUR',
                'email_domain': 'itbank.it',
                'phone': '39-555-BANK'
            },
            'United Kingdom': {
                'avg_property_value': 350000,
                'max_loan_to_value': 0.85,
                'typical_rate': 4.25,
                'currency': 'GBP',
                'email_domain': 'ukbank.co.uk',
                'phone': '44-555-BANK'
            },
            'Denmark': {
                'avg_property_value': 400000,
                'max_loan_to_value': 0.80,
                'typical_rate': 3.85,
                'currency': 'DKK',
                'email_domain': 'dkbank.dk',
                'phone': '45-555-BANK'
            },
            'Belgium': {
                'avg_property_value': 310000,
                'max_loan_to_value': 0.80,
                'typical_rate': 3.05,
                'currency': 'EUR',
                'email_domain': 'bebank.be',
                'phone': '32-555-BANK'
            },
            'Austria': {
                'avg_property_value': 340000,
                'max_loan_to_value': 0.80,
                'typical_rate': 3.35,
                'currency': 'EUR',
                'email_domain': 'atbank.at',
                'phone': '43-555-BANK'
            },
            'Switzerland': {
                'avg_property_value': 650000,
                'max_loan_to_value': 0.80,
                'typical_rate': 2.75,
                'currency': 'CHF',
                'email_domain': 'chbank.ch',
                'phone': '41-555-BANK'
            }
        }

    def load_customers_and_addresses(self, customer_file: str, address_file: str) -> Dict[str, tuple]:
        """Load customer and address data from CSV files"""
        customers = {}
        addresses = {}
        
        # Load customers
        with open(customer_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                customer = Customer(
                    customer_id=row['customer_id'],
                    first_name=row['first_name'],
                    family_name=row['family_name'],
                    date_of_birth=row['date_of_birth'],
                    onboarding_date=row['onboarding_date'],
                    has_anomaly=row['has_anomaly'].lower() == 'true'
                )
                customers[customer.customer_id] = customer
        
        # Load addresses (get the most recent for each customer)
        with open(address_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                address = CustomerAddress(
                    customer_id=row['customer_id'],
                    street_address=row['street_address'],
                    city=row['city'],
                    state=row['state'] if row['state'] else '',
                    zipcode=row['zipcode'],
                    country=row['country'],
                    insert_timestamp_utc=row['insert_timestamp_utc']
                )
                # Keep the most recent address for each customer
                if (address.customer_id not in addresses or 
                    address.insert_timestamp_utc > addresses[address.customer_id].insert_timestamp_utc):
                    addresses[address.customer_id] = address
        
        # Combine customers with their addresses
        result = {}
        for customer_id, customer in customers.items():
            if customer_id in addresses:
                result[customer_id] = (customer, addresses[customer_id])
        
        return result

    def generate_mortgage_data(self, customer: Customer, address: CustomerAddress) -> MortgageRequest:
        """Generate realistic mortgage request data for a customer"""
        
        # Get country-specific data
        country_data = self.country_mortgage_data.get(address.country, self.country_mortgage_data['Germany'])
        
        # Calculate customer age
        birth_date = datetime.strptime(customer.date_of_birth, '%Y-%m-%d')
        age = datetime.now().year - birth_date.year
        
        # Generate property value (varies by age and country)
        base_value = country_data['avg_property_value']
        age_factor = 0.8 + (min(age - 25, 40) / 40) * 0.4  # 0.8 to 1.2 based on age
        property_value = base_value * age_factor * random.uniform(0.7, 1.5)
        
        # Calculate loan amount
        max_ltv = country_data['max_loan_to_value']
        down_payment_pct = random.uniform(0.15, 0.35)  # 15-35% down payment
        down_payment = property_value * down_payment_pct
        loan_amount = min(property_value - down_payment, property_value * max_ltv)
        
        # Generate income (based on loan amount and typical ratios)
        monthly_payment_estimate = loan_amount * 0.005  # Rough estimate
        monthly_income = monthly_payment_estimate / random.uniform(0.25, 0.35)  # 25-35% of income
        
        # Generate other data
        employment_types = ["Full-time Employee", "Self-employed", "Contract Worker", "Government Employee"]
        property_types = ["Single Family Home", "Apartment/Condo", "Townhouse", "Multi-family"]
        purposes = ["Purchase Primary Residence", "Refinance Existing Mortgage", "Investment Property"]
        
        # Credit score based on anomaly flag
        if customer.has_anomaly:
            credit_score = random.randint(580, 680)  # Lower for anomaly customers
        else:
            credit_score = random.randint(650, 850)  # Higher for normal customers
        
        application_date = datetime.now() - timedelta(days=random.randint(1, 30))
        
        return MortgageRequest(
            application_id=f"MTG_{customer.customer_id}_{application_date.strftime('%Y%m%d')}",
            customer=customer,
            address=address,
            loan_amount=loan_amount,
            property_value=property_value,
            down_payment=down_payment,
            loan_term_years=random.choice([15, 20, 25, 30]),
            interest_rate=country_data['typical_rate'] + random.uniform(-0.5, 1.0),
            monthly_income=monthly_income,
            employment_type=random.choice(employment_types),
            employment_years=min(age - 18, random.randint(2, 25)),
            property_type=random.choice(property_types),
            application_date=application_date,
            purpose=random.choice(purposes),
            credit_score=credit_score,
            debt_to_income_ratio=random.uniform(0.20, 0.45)
        )

    def generate_customer_email(self, mortgage: MortgageRequest) -> str:
        """Generate customer-facing email for mortgage application confirmation"""
        
        country_data = self.country_mortgage_data.get(mortgage.address.country, self.country_mortgage_data['Germany'])
        currency = country_data['currency']
        
        # Calculate additional values
        birth_date = datetime.strptime(mortgage.customer.date_of_birth, '%Y-%m-%d')
        age = datetime.now().year - birth_date.year
        
        # Calculate estimated monthly payment
        monthly_rate = mortgage.interest_rate / 100 / 12
        num_payments = mortgage.loan_term_years * 12
        if monthly_rate > 0:
            monthly_payment = mortgage.loan_amount * (monthly_rate * (1 + monthly_rate)**num_payments) / ((1 + monthly_rate)**num_payments - 1)
        else:
            monthly_payment = mortgage.loan_amount / num_payments
        
        # Risk assessment
        ltv_ratio = mortgage.loan_amount / mortgage.property_value
        risk_factors = []
        if ltv_ratio > 0.85:
            risk_factors.append("High loan-to-value ratio")
        if mortgage.credit_score < 650:
            risk_factors.append("Credit score requires additional documentation")
        if mortgage.customer.has_anomaly:
            risk_factors.append("Account requires enhanced verification")
        
        status = "Under Review" if risk_factors else "Pre-approved"
        
        email_content = f"""Subject: Mortgage Application Confirmation - {mortgage.application_id}

Dear {mortgage.customer.first_name} {mortgage.customer.family_name},

Thank you for your mortgage application submitted on {mortgage.application_date.strftime('%B %d, %Y')}. We have received your request and want to confirm the details of your application.

APPLICATION SUMMARY
===================
Application ID: {mortgage.application_id}
Application Date: {mortgage.application_date.strftime('%Y-%m-%d')}
Status: {status}

APPLICANT INFORMATION
=====================
Name: {mortgage.customer.first_name} {mortgage.customer.family_name}
Customer ID: {mortgage.customer.customer_id}
Date of Birth: {mortgage.customer.date_of_birth} (Age: {age})
Customer Since: {mortgage.customer.onboarding_date}

PROPERTY DETAILS
================
Address: {mortgage.address.street_address}
         {mortgage.address.city}{', ' + mortgage.address.state if mortgage.address.state else ''}
         {mortgage.address.zipcode}, {mortgage.address.country}
Property Type: {mortgage.property_type}
Property Value: {mortgage.property_value:,.0f} {currency}

LOAN DETAILS
============
Loan Amount Requested: {mortgage.loan_amount:,.0f} {currency}
Down Payment: {mortgage.down_payment:,.0f} {currency} ({mortgage.down_payment/mortgage.property_value*100:.1f}%)
Loan Term: {mortgage.loan_term_years} years
Interest Rate: {mortgage.interest_rate:.2f}%
Estimated Monthly Payment: {monthly_payment:,.0f} {currency}
Loan Purpose: {mortgage.purpose}

FINANCIAL INFORMATION
======================
Monthly Income: {mortgage.monthly_income:,.0f} {currency}
Employment: {mortgage.employment_type} ({mortgage.employment_years} years)
Credit Score: {mortgage.credit_score}
Debt-to-Income Ratio: {mortgage.debt_to_income_ratio*100:.1f}%
Loan-to-Value Ratio: {ltv_ratio*100:.1f}%

NEXT STEPS
=========="""

        if status == "Pre-approved":
            email_content += f"""
🎉 Congratulations! Your application shows strong financial indicators and is pre-approved for further processing.

Next steps:
1. Property appraisal will be scheduled within 5-7 business days
2. Final documentation review
3. Loan approval and closing coordination

Your dedicated loan officer will contact you within 2 business days to discuss the next steps.
"""
        else:
            email_content += f"""
Your application is currently under review. We may need additional documentation due to:
{chr(10).join('• ' + factor for factor in risk_factors)}

Our underwriting team will review your application and contact you within 3-5 business days with:
1. Any additional documentation requirements
2. Updated loan terms if applicable
3. Next steps in the approval process
"""

        email_content += f"""
IMPORTANT INFORMATION
=====================
• Your application is protected by banking privacy regulations
• Interest rates are subject to final approval and market conditions
• Property appraisal will be required for final approval
• All estimates are preliminary and subject to final underwriting

CONTACT INFORMATION
===================
Loan Officer: Sarah Mitchell
Direct Line: +{country_data.get('phone', '1-555-BANK')}
Email: sarah.mitchell@aaasyntheticbank.com
Customer Email: {mortgage.customer.first_name.lower()}.{mortgage.customer.family_name.lower()}@email.com
Application Portal: https://portal.aaasyntheticbank.com

Thank you for choosing AAA Synthetic Bank for your mortgage needs. We appreciate your business and look forward to helping you secure your home financing.

Best regards,

Sarah Mitchell
Senior Loan Officer
AAA Synthetic Bank
Mortgage Division

---
This email was generated on {datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')} UTC
Application Reference: {mortgage.application_id}
Customer ID: {mortgage.customer.customer_id}

CONFIDENTIAL: This email contains sensitive financial information. Please do not forward or share with unauthorized parties.
"""
        
        return email_content

    def generate_internal_email(self, mortgage: MortgageRequest) -> str:
        """Generate internal bank email for mortgage application processing"""
        
        country_data = self.country_mortgage_data.get(mortgage.address.country, self.country_mortgage_data['Germany'])
        currency = country_data['currency']
        
        # Calculate ratios and risk factors
        ltv_ratio = mortgage.loan_amount / mortgage.property_value
        monthly_rate = mortgage.interest_rate / 100 / 12
        num_payments = mortgage.loan_term_years * 12
        if monthly_rate > 0:
            monthly_payment = mortgage.loan_amount * (monthly_rate * (1 + monthly_rate)**num_payments) / ((1 + monthly_rate)**num_payments - 1)
        else:
            monthly_payment = mortgage.loan_amount / num_payments
        
        payment_to_income = monthly_payment / mortgage.monthly_income
        
        # Risk assessment
        risk_factors = []
        risk_score = 0
        
        if ltv_ratio > 0.85:
            risk_factors.append("HIGH LTV: Exceeds 85% threshold")
            risk_score += 2
        elif ltv_ratio > 0.80:
            risk_factors.append("ELEVATED LTV: Above 80%")
            risk_score += 1
            
        if mortgage.credit_score < 650:
            risk_factors.append("LOW CREDIT: Below 650 FICO")
            risk_score += 3
        elif mortgage.credit_score < 700:
            risk_factors.append("FAIR CREDIT: Below 700 FICO")
            risk_score += 1
            
        if payment_to_income > 0.35:
            risk_factors.append("HIGH PTI: Payment exceeds 35% of income")
            risk_score += 2
            
        if mortgage.debt_to_income_ratio > 0.40:
            risk_factors.append("HIGH DTI: Debt exceeds 40% of income")
            risk_score += 2
            
        if mortgage.customer.has_anomaly:
            risk_factors.append("CUSTOMER ALERT: Flagged for unusual activity")
            risk_score += 3
            
        if risk_score == 0:
            risk_level = "LOW"
            recommendation = "APPROVE - Standard processing"
        elif risk_score <= 3:
            risk_level = "MEDIUM"
            recommendation = "CONDITIONAL APPROVAL - Additional documentation required"
        else:
            risk_level = "HIGH"
            recommendation = "MANUAL REVIEW - Underwriter escalation required"

        email_content = f"""Subject: [INTERNAL] Mortgage Application Review - {mortgage.application_id} - {risk_level} RISK

To: underwriting@aaasyntheticbank.com
Cc: sarah.mitchell@aaasyntheticbank.com, risk@aaasyntheticbank.com
From: system@aaasyntheticbank.com

MORTGAGE APPLICATION REVIEW
============================
Application ID: {mortgage.application_id}
Review Date: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')} UTC
Risk Level: {risk_level}
Risk Score: {risk_score}/10

RECOMMENDATION: {recommendation}

APPLICANT OVERVIEW
==================
Customer: {mortgage.customer.first_name} {mortgage.customer.family_name}
Customer ID: {mortgage.customer.customer_id}
Customer Since: {mortgage.customer.onboarding_date}
Age: {datetime.now().year - datetime.strptime(mortgage.customer.date_of_birth, '%Y-%m-%d').year}
Account Status: {'⚠️ FLAGGED' if mortgage.customer.has_anomaly else '✅ NORMAL'}

LOAN SUMMARY
============
Property Value: {mortgage.property_value:,.0f} {currency}
Loan Amount: {mortgage.loan_amount:,.0f} {currency}
Down Payment: {mortgage.down_payment:,.0f} {currency} ({mortgage.down_payment/mortgage.property_value*100:.1f}%)
Loan Term: {mortgage.loan_term_years} years
Interest Rate: {mortgage.interest_rate:.2f}%
Est. Monthly Payment: {monthly_payment:,.0f} {currency}

FINANCIAL RATIOS
================
Loan-to-Value (LTV): {ltv_ratio*100:.1f}% {'❌' if ltv_ratio > 0.85 else '⚠️' if ltv_ratio > 0.80 else '✅'}
Payment-to-Income (PTI): {payment_to_income*100:.1f}% {'❌' if payment_to_income > 0.35 else '⚠️' if payment_to_income > 0.28 else '✅'}
Debt-to-Income (DTI): {mortgage.debt_to_income_ratio*100:.1f}% {'❌' if mortgage.debt_to_income_ratio > 0.40 else '⚠️' if mortgage.debt_to_income_ratio > 0.36 else '✅'}
Credit Score: {mortgage.credit_score} {'❌' if mortgage.credit_score < 650 else '⚠️' if mortgage.credit_score < 700 else '✅'}

EMPLOYMENT DETAILS
==================
Type: {mortgage.employment_type}
Experience: {mortgage.employment_years} years
Monthly Income: {mortgage.monthly_income:,.0f} {currency}

PROPERTY INFORMATION
====================
Address: {mortgage.address.street_address}, {mortgage.address.city}, {mortgage.address.country}
Type: {mortgage.property_type}
Purpose: {mortgage.purpose}

RISK ASSESSMENT
==============="""

        if risk_factors:
            email_content += f"""
Risk Factors Identified:
{chr(10).join('• ' + factor for factor in risk_factors)}
"""
        else:
            email_content += "\n✅ No significant risk factors identified"

        email_content += f"""

PROCESSING REQUIREMENTS
======================="""

        if risk_level == "LOW":
            email_content += """
□ Standard income verification
□ Property appraisal
□ Title search
□ Final credit check
□ Loan documentation

Estimated Processing Time: 5-7 business days
"""
        elif risk_level == "MEDIUM":
            email_content += """
□ Enhanced income verification (2 years tax returns)
□ Property appraisal (may require second opinion)
□ Employment verification letter
□ Bank statements (3 months)
□ Debt verification letters
□ Title search
□ Final credit check

Estimated Processing Time: 10-14 business days
"""
        else:
            email_content += """
□ Manual underwriter review REQUIRED
□ Full financial audit (3 years tax returns)
□ Property appraisal + second opinion
□ Employment verification + reference check
□ Complete bank statements (6 months)
□ All debt verification
□ Character references
□ Possible co-signer requirement

Estimated Processing Time: 15-21 business days
ESCALATION: Senior Underwriter approval required
"""

        email_content += f"""
COMPLIANCE NOTES
================
• Customer due diligence {'⚠️ ENHANCED' if mortgage.customer.has_anomaly else '✅ STANDARD'}
• Anti-money laundering check: Pending
• Regulatory compliance: {mortgage.address.country} banking regulations apply
• Documentation retention: 7 years minimum

SYSTEM INFORMATION
==================
Generated: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')} UTC
Source: Automated Underwriting System v2.1
Processor: MTGPROC-{random.randint(100,999)}

---
This is an automated system-generated email for internal processing only.
Not for customer distribution.
"""
        
        return email_content

    def generate_loan_officer_email(self, mortgage: MortgageRequest) -> str:
        """Generate loan officer assignment and guidance email"""
        
        country_data = self.country_mortgage_data.get(mortgage.address.country, self.country_mortgage_data['Germany'])
        currency = country_data['currency']
        
        # Calculate values
        ltv_ratio = mortgage.loan_amount / mortgage.property_value
        risk_score = 0
        
        if ltv_ratio > 0.85: risk_score += 2
        if mortgage.credit_score < 650: risk_score += 3
        if mortgage.customer.has_anomaly: risk_score += 3
        
        complexity = "COMPLEX" if risk_score >= 5 else "STANDARD" if risk_score <= 2 else "MODERATE"
        
        email_content = f"""Subject: Loan Assignment - {mortgage.application_id} - {complexity} Case

To: sarah.mitchell@aaasyntheticbank.com
From: assignments@aaasyntheticbank.com

LOAN ASSIGNMENT NOTIFICATION
=============================
Dear Sarah,

You have been assigned a new mortgage application. Please review the details below and contact the customer within 24 hours.

APPLICATION DETAILS
====================
Application ID: {mortgage.application_id}
Customer: {mortgage.customer.first_name} {mortgage.customer.family_name}
Phone: +{country_data.get('phone', '1-555-0000')} (to be provided)
Email: {mortgage.customer.first_name.lower()}.{mortgage.customer.family_name.lower()}@email.com
Complexity Level: {complexity}

QUICK SUMMARY
=============
Loan Amount: {mortgage.loan_amount:,.0f} {currency}
Property Value: {mortgage.property_value:,.0f} {currency}
Credit Score: {mortgage.credit_score}
LTV Ratio: {ltv_ratio*100:.1f}%
Customer Since: {mortgage.customer.onboarding_date}

CUSTOMER COMMUNICATION SCRIPT
==============================
"Hello {mortgage.customer.first_name}, this is Sarah Mitchell from AAA Synthetic Bank. 

I'm calling regarding your mortgage application {mortgage.application_id} that you submitted on {mortgage.application_date.strftime('%B %d')}. 

I wanted to personally introduce myself as your dedicated loan officer and answer any questions you might have about the mortgage process.

Based on your application for {mortgage.loan_amount:,.0f} {currency}, I can see you're looking to {mortgage.purpose.lower()} for the property at {mortgage.address.street_address} in {mortgage.address.city}.

{f'I do want to mention that we may need some additional documentation due to your credit score being below our preferred threshold. This is quite common and nothing to be concerned about - we just want to ensure we have the complete picture of your financial situation.' if mortgage.credit_score < 650 else 'Your application looks very strong, and I expect a smooth approval process.'}

When would be a convenient time for you to discuss the next steps? I have availability this week on..."

KEY DISCUSSION POINTS
======================
□ Confirm property details and purchase timeline
□ Verify income and employment information
□ Explain the appraisal process
□ Review loan terms and interest rate
□ Discuss closing timeline (typically 30-45 days)
{f'□ Address credit score concerns tactfully' if mortgage.credit_score < 650 else ''}
{f'□ Explain enhanced verification process due to account history' if mortgage.customer.has_anomaly else ''}

DOCUMENTATION CHECKLIST
========================
□ Photo ID verification
□ Income verification (pay stubs, tax returns)
□ Employment verification letter
□ Bank statements (3 months minimum)
□ Property information and purchase agreement
□ Homeowner's insurance quote
□ Additional debt information

EXPECTED CHALLENGES
==================="""

        if complexity == "COMPLEX":
            email_content += """
⚠️ This is a complex case requiring extra attention:
• Below-average credit score may require manual underwriting
• Enhanced documentation will be needed
• Customer may have concerns about approval
• Expect longer processing time (15-21 days)
• Consider offering consultation on credit improvement
"""
        elif complexity == "MODERATE":
            email_content += """
• Standard processing with some additional documentation
• May need to explain slightly higher rates or longer timeline
• Good opportunity for relationship building
"""
        else:
            email_content += """
✅ Straightforward case:
• Strong financial profile
• Standard processing expected
• Excellent relationship building opportunity
• Consider cross-selling other bank products
"""

        email_content += f"""
CONTACT SCHEDULE
================
□ Initial contact: Within 24 hours
□ Follow-up: 3 days after initial contact
□ Weekly updates until closing
□ Post-closing follow-up: 30 days

INTERNAL RESOURCES
==================
• Underwriting team: ext. 2301
• Risk assessment: ext. 2405
• Legal/compliance: ext. 2501
• Customer service backup: ext. 2200

Best regards,

Loan Assignment System
AAA Synthetic Bank
Mortgage Division

---
Assignment Date: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')} UTC
Priority: {complexity}
Follow-up Required By: {(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')}
"""
        
        return email_content

    def save_emails_to_files(self, mortgage: MortgageRequest) -> Dict[str, str]:
        """Save all generated emails to files"""
        
        # Generate all email types
        customer_email = self.generate_customer_email(mortgage)
        internal_email = self.generate_internal_email(mortgage)
        loan_officer_email = self.generate_loan_officer_email(mortgage)
        
        # Create filenames
        base_name = f"mortgage_emails_{mortgage.customer.customer_id}_{mortgage.application_date.strftime('%Y%m%d')}"
        
        files = {
            'customer': f"{base_name}_customer.txt",
            'internal': f"{base_name}_internal.txt",
            'loan_officer': f"{base_name}_loan_officer.txt"
        }
        
        # Save files
        saved_files = {}
        
        for email_type, filename in files.items():
            filepath = self.output_dir / filename
            
            if email_type == 'customer':
                content = customer_email
            elif email_type == 'internal':
                content = internal_email
            else:
                content = loan_officer_email
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            saved_files[email_type] = str(filepath)
        
        return saved_files

    def generate_mortgage_emails(self, customer_file: str, address_file: str, num_customers: int = 3) -> List[Dict]:
        """Generate mortgage emails for specified number of customers"""
        
        # Load customer and address data
        customer_data = self.load_customers_and_addresses(customer_file, address_file)
        
        if not customer_data:
            raise ValueError("No customer data found")
        
        # Select customers for mortgage applications
        selected_customers = random.sample(list(customer_data.items()), min(num_customers, len(customer_data)))
        
        generated_emails = []
        
        for customer_id, (customer, address) in selected_customers:
            # Generate mortgage data
            mortgage = self.generate_mortgage_data(customer, address)
            
            # Generate and save emails
            saved_files = self.save_emails_to_files(mortgage)
            
            result = {
                'customer_name': f"{customer.first_name} {customer.family_name}",
                'customer_id': customer.customer_id,
                'application_id': mortgage.application_id,
                'files': saved_files
            }
            
            generated_emails.append(result)
            
            print(f"✅ Generated mortgage emails for {customer.first_name} {customer.family_name}")
            print(f"   📧 Customer email: {saved_files['customer']}")
            print(f"   📧 Internal email: {saved_files['internal']}")
            print(f"   📧 Loan officer email: {saved_files['loan_officer']}")
            print()
        
        return generated_emails


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate mortgage request emails for synthetic customers")
    parser.add_argument("--customer-file", default="generated_data/master_data/customers.csv",
                       help="Path to customer CSV file")
    parser.add_argument("--address-file", default="generated_data/master_data/customer_addresses.csv",
                       help="Path to customer address CSV file")
    parser.add_argument("--output-dir", default="generated_data/emails",
                       help="Output directory for email files")
    parser.add_argument("--num-customers", type=int, default=3,
                       help="Number of customers to generate mortgage emails for")
    
    args = parser.parse_args()
    
    # Create generator
    generator = MortgageEmailGenerator(args.output_dir)
    
    try:
        # Generate emails
        generated_emails = generator.generate_mortgage_emails(
            args.customer_file,
            args.address_file,
            args.num_customers
        )
        
        print(f"🎉 Successfully generated mortgage emails for {len(generated_emails)} customers!")
        print(f"📁 Output directory: {args.output_dir}")
        print()
        print("📧 Generated email files:")
        for email_data in generated_emails:
            print(f"   👤 {email_data['customer_name']} ({email_data['customer_id']})")
            print(f"      📝 Application: {email_data['application_id']}")
            for email_type, filepath in email_data['files'].items():
                print(f"      📧 {email_type.title()}: {Path(filepath).name}")
            print()
        
    except Exception as e:
        print(f"❌ Error generating mortgage emails: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
