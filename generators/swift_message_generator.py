#!/usr/bin/env python3
"""
SWIFT Message Generator for PACS messages
Supports PACS.008 (interbank payment instruction) and PACS.002 (payment status report)
"""

import json
import uuid
import click
import random
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass, field, asdict
from xml.etree.ElementTree import Element, SubElement, tostring, parse, fromstring
from xml.dom import minidom


def get_next_business_day() -> date:
    """Get next business day (skip weekends)"""
    today = date.today()
    days_ahead = 1
    
    # If today is Friday (4) or Saturday (5), add more days
    if today.weekday() >= 4:  # Friday or later
        days_ahead = 7 - today.weekday() + 1  # Go to next Monday
    
    return today + timedelta(days=days_ahead)


def extract_country_from_bic(bic: str) -> str:
    """Extract country code from BIC (positions 5-6)"""
    if len(bic) >= 6:
        return bic[4:6]
    return "XX"  # Unknown


def generate_cbpr_compliant_id() -> str:
    """Generate CBPR+ compliant ID (lowercase, no special chars except hyphens)"""
    # Generate UUID and ensure lowercase format for CBPR+ compliance
    return str(uuid.uuid4()).lower()


def generate_production_message_id(sender_bic: str = None) -> str:
    """Generate production-style message ID with date and system prefix
    
    Format: YYYYMMDD-{SENDER_BIC}-{sequence}
    Example: 20250816-DEUTDEFF-001
    
    Args:
        sender_bic: Sender BIC code for prefix (optional)
        
    Returns:
        Production-ready message ID
    """
    from datetime import datetime
    import random
    
    date_prefix = datetime.now().strftime("%Y%m%d")
    bic_prefix = sender_bic[:8] if sender_bic else "SYSTEM"
    sequence = f"{random.randint(1, 999):03d}"  # 3-digit sequence
    
    return f"{date_prefix}-{bic_prefix}-{sequence}"


def generate_production_transaction_id(sender_bic: str = None) -> str:
    """Generate production-style transaction ID
    
    Format: YYYYMMDD-{SENDER_BIC}-TXN-{sequence}
    Example: 20250816-DEUTDEFF-TXN-001
    
    Args:
        sender_bic: Sender BIC code for prefix (optional)
        
    Returns:
        Production-ready transaction ID
    """
    from datetime import datetime
    import random
    
    date_prefix = datetime.now().strftime("%Y%m%d")
    bic_prefix = sender_bic[:8] if sender_bic else "SYSTEM"
    sequence = f"{random.randint(1, 999):03d}"  # 3-digit sequence
    
    return f"{date_prefix}-{bic_prefix}-TXN-{sequence}"


def extract_end_to_end_id_from_pacs008(xml_content: str) -> Optional[str]:
    """Extract EndToEndId from PACS.008 XML content
    
    Args:
        xml_content: The XML content as string
        
    Returns:
        The EndToEndId value or None if not found
    """
    try:
        root = fromstring(xml_content)
        # Look for EndToEndId in the namespace
        namespaces = {'ns': 'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08'}
        end_to_end_element = root.find('.//ns:EndToEndId', namespaces)
        if end_to_end_element is not None:
            return end_to_end_element.text
        return None
    except Exception as e:
        print(f"Warning: Could not extract EndToEndId from XML: {e}")
        return None


def extract_message_id_from_pacs008(xml_content: str) -> Optional[str]:
    """Extract Message ID from PACS.008 XML content
    
    Args:
        xml_content: The XML content as string
        
    Returns:
        The Message ID value or None if not found
    """
    try:
        root = fromstring(xml_content)
        # Look for MsgId in the namespace
        namespaces = {'ns': 'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08'}
        msg_id_element = root.find('.//ns:MsgId', namespaces)
        if msg_id_element is not None:
            return msg_id_element.text
        return None
    except Exception as e:
        print(f"Warning: Could not extract Message ID from XML: {e}")
        return None


def generate_delayed_timestamp(base_time: datetime = None, min_delay_minutes: int = 1, max_delay_minutes: int = 30) -> datetime:
    """Generate a timestamp with random delay to simulate processing time
    
    Args:
        base_time: Base timestamp (defaults to now)
        min_delay_minutes: Minimum delay in minutes
        max_delay_minutes: Maximum delay in minutes
        
    Returns:
        Delayed timestamp
    """
    if base_time is None:
        base_time = datetime.now()
    
    delay_minutes = random.randint(min_delay_minutes, max_delay_minutes)
    delay_seconds = random.randint(0, 59)  # Add random seconds for more realism
    delay_microseconds = random.randint(0, 999) * 1000  # Add random milliseconds
    
    return base_time + timedelta(minutes=delay_minutes, seconds=delay_seconds, microseconds=delay_microseconds)





def validate_xsd_compliance_notes() -> Dict[str, str]:
    """Return validation notes for strict XSD compliance
    
    Returns:
        Dict containing validation guidelines for ISO 20022 XSD compliance
    """
    return {
        "decimal_formatting": "All amounts must use exactly 2 decimal places (e.g., '25000.00', never '25000.0' or '25000')",
        "timestamp_format": "CreDtTm uses milliseconds precision (3 fractional digits) for better compatibility: YYYY-MM-DDTHH:MM:SS.sss",
        "bic_codes": "BIC codes must be exactly 8 or 11 characters (8 for main office, 11 for branch)",
        "iban_format": "IBAN must follow country-specific format and pass MOD-97 check digit validation",
        "currency_codes": "Use ISO 4217 3-letter currency codes (EUR, USD, GBP, etc.)",
        "element_order": "XML elements must appear in exact order defined by XSD schema",
        "namespace": "Must use correct namespace: urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08",
        "cbpr_compliance": "For cross-border payments, use ChrgBr=SLEV (service level default) and proper settlement method",
        "target2_compliance": "For TARGET2, use SttlmMtd=RTGS and ClrSys/Cd=TARGET2 (TARGET2 is an RTGS clearing system)"
    }


def generate_xsd_validation_report(xml_content: str) -> Dict[str, Any]:
    """Generate basic validation report for XSD compliance
    
    Args:
        xml_content: The XML content to validate
        
    Returns:
        Dict containing validation results and recommendations
    """
    import re
    
    report = {
        "timestamp_format": "unknown",
        "decimal_format": "unknown", 
        "bic_format": "unknown",
        "namespace_correct": False,
        "recommendations": []
    }
    
    # Check namespace
    if 'xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"' in xml_content:
        report["namespace_correct"] = True
    else:
        report["recommendations"].append("Use correct namespace for pacs.008.001.08")
    
    # Check timestamp format
    timestamp_pattern = r'<CreDtTm>(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)</CreDtTm>'
    timestamp_match = re.search(timestamp_pattern, xml_content)
    if timestamp_match:
        timestamp = timestamp_match.group(1)
        fractional_part = timestamp.split('.')[-1]
        if len(fractional_part) == 3:
            report["timestamp_format"] = "optimal_milliseconds"
        elif len(fractional_part) == 6:
            report["timestamp_format"] = "microseconds_may_cause_issues"
            report["recommendations"].append("Consider using milliseconds (3 digits) instead of microseconds (6 digits) for better compatibility")
        else:
            report["timestamp_format"] = "non_standard"
            report["recommendations"].append("Use standard timestamp format with 3 or 6 fractional digits")
    
    # Check decimal formatting
    amount_pattern = r'Ccy="[A-Z]{3}">([0-9]+\.?[0-9]*)</[^>]*Amt>'
    amount_matches = re.findall(amount_pattern, xml_content)
    decimal_issues = []
    for amount in amount_matches:
        if '.' not in amount:
            decimal_issues.append(f"Amount '{amount}' missing decimal point")
        elif len(amount.split('.')[-1]) != 2:
            decimal_issues.append(f"Amount '{amount}' doesn't have exactly 2 decimal places")
    
    if not decimal_issues:
        report["decimal_format"] = "compliant"
    else:
        report["decimal_format"] = "issues_found"
        report["recommendations"].extend(decimal_issues)
    
    # Check BIC format
    bic_pattern = r'<BICFI>([A-Z0-9]{8,11})</BICFI>'
    bic_matches = re.findall(bic_pattern, xml_content)
    bic_issues = []
    for bic in bic_matches:
        if len(bic) not in [8, 11]:
            bic_issues.append(f"BIC '{bic}' must be 8 or 11 characters")
    
    if not bic_issues:
        report["bic_format"] = "compliant"
    else:
        report["bic_format"] = "issues_found"
        report["recommendations"].extend(bic_issues)
    
    return report


def generate_operational_compliance_report(xml_content: str) -> Dict[str, Any]:
    """Generate operational compliance report for real banking systems
    
    Args:
        xml_content: The XML content to validate
        
    Returns:
        Dict containing operational compliance results and recommendations
    """
    import re
    
    report = {
        "settlement_method_compliance": "unknown",
        "address_completeness": "unknown",
        "agent_structure": "unknown",
        "payment_type_info": "unknown",
        "operational_recommendations": []
    }
    
    # Check settlement method vs clearing system compatibility
    settlement_method_match = re.search(r'<SttlmMtd>([^<]+)</SttlmMtd>', xml_content)
    clearing_system_match = re.search(r'<ClrSys>.*?<Cd>([^<]+)</Cd>.*?</ClrSys>', xml_content, re.DOTALL)
    
    if settlement_method_match and clearing_system_match:
        settlement_method = settlement_method_match.group(1)
        clearing_system = clearing_system_match.group(1)
        
        if clearing_system == "TARGET2" and settlement_method == "RTGS":
            report["settlement_method_compliance"] = "optimal"
        elif clearing_system == "TARGET2" and settlement_method in ["CLRG", "INDA"]:
            report["settlement_method_compliance"] = "problematic"
            if settlement_method == "CLRG":
                report["operational_recommendations"].append("TARGET2 is an RTGS system - use SttlmMtd=RTGS instead of CLRG")
            elif settlement_method == "INDA":
                report["operational_recommendations"].append("TARGET2 is an RTGS clearing system - use SttlmMtd=RTGS instead of INDA (INDA is for bilateral settlement outside clearing systems)")
        else:
            report["settlement_method_compliance"] = "acceptable"
    
    # Check address completeness
    debtor_address_match = re.search(r'<Dbtr>.*?<PstlAdr>(.*?)</PstlAdr>.*?</Dbtr>', xml_content, re.DOTALL)
    creditor_address_match = re.search(r'<Cdtr>.*?<PstlAdr>(.*?)</PstlAdr>.*?</Cdtr>', xml_content, re.DOTALL)
    
    address_issues = []
    for party, addr_match in [("Debtor", debtor_address_match), ("Creditor", creditor_address_match)]:
        if addr_match:
            addr_content = addr_match.group(1)
            has_street = '<StrtNm>' in addr_content
            has_postcode = '<PstCd>' in addr_content
            has_city = '<TwnNm>' in addr_content
            
            if not (has_street or has_postcode or has_city):
                address_issues.append(f"{party} address only has country - banks may reject incomplete addresses")
        else:
            address_issues.append(f"{party} missing address information")
    
    if not address_issues:
        report["address_completeness"] = "compliant"
    else:
        report["address_completeness"] = "issues_found"
        report["operational_recommendations"].extend(address_issues)
    
    # Check Remittance Information
    if '<RmtInf>' in xml_content and '<Ustrd>' in xml_content:
        report["remittance_info"] = "present"
    else:
        report["remittance_info"] = "missing"
        report["operational_recommendations"].append("Add remittance information (<RmtInf><Ustrd>) for payment identification in TARGET2 flows")
    
    # Check Payment Type Information
    if '<PmtTpInf>' in xml_content:
        # Check for TARGET2-optimized payment type
        if ('<InstrPrty>HIGH</InstrPrty>' in xml_content and 
            '<SvcLvl>' in xml_content and '<Cd>URGP</Cd>' in xml_content and
            '<LclInstrm>' in xml_content and '<Cd>INST</Cd>' in xml_content):
            report["payment_type_info"] = "target2_optimized"
        # Check for SEPA compliance
        elif '<SvcLvl>' in xml_content and '<Cd>SEPA</Cd>' in xml_content:
            report["payment_type_info"] = "sepa_compliant"
        else:
            report["payment_type_info"] = "present"
    else:
        report["payment_type_info"] = "missing"
        report["operational_recommendations"].append("Consider adding PmtTpInf with SvcLvl=SEPA and CtgyPurp for better routing")
    
    # Check agent structure
    has_instg_agt = '<InstgAgt>' in xml_content
    has_instd_agt = '<InstdAgt>' in xml_content
    has_dbtr_agt = '<DbtrAgt>' in xml_content
    has_cdtr_agt = '<CdtrAgt>' in xml_content
    
    if has_instg_agt and has_instd_agt and has_dbtr_agt and has_cdtr_agt:
        report["agent_structure"] = "complete_but_redundant"
        report["operational_recommendations"].append("InstgAgt often same as DbtrAgt in two-party flows - consider simplifying agent structure")
    elif has_dbtr_agt and has_cdtr_agt:
        report["agent_structure"] = "minimal_compliant"
    else:
        report["agent_structure"] = "incomplete"
        report["operational_recommendations"].append("Ensure DbtrAgt and CdtrAgt are present for proper routing")
    
    return report


@dataclass
class MessageIdentification:
    """Message identification and creation details"""
    message_id: str = field(default_factory=generate_cbpr_compliant_id)
    creation_date_time: datetime = field(default_factory=datetime.now)
    
    def get_xsd_compliant_timestamp(self) -> str:
        """Return XSD-compliant timestamp with 3 fractional digits (milliseconds)
        
        ISO 20022 XSD allows fractional seconds, but many systems prefer
        3 digits (milliseconds) over 6 digits (microseconds) for better compatibility.
        Format: YYYY-MM-DDTHH:MM:SS.sss
        """
        # Round to milliseconds (3 decimal places) for better system compatibility
        timestamp = self.creation_date_time
        # Convert to milliseconds precision
        milliseconds = int(timestamp.microsecond / 1000)
        timestamp_ms = timestamp.replace(microsecond=milliseconds * 1000)
        return timestamp_ms.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3]  # Remove last 3 digits to get milliseconds


@dataclass
class Amount:
    """Currency and amount representation"""
    currency: str
    amount: Decimal
    
    def __post_init__(self):
        if isinstance(self.amount, (int, float, str)):
            self.amount = Decimal(str(self.amount))
        # Ensure proper decimal formatting for ISO 20022 XSD validation
        # Always use exactly 2 decimal places for EUR, USD, and most major currencies
        self.amount = self.amount.quantize(Decimal('0.01'))
    
    def __str__(self):
        """Return amount with exactly 2 decimal places for XSD compliance"""
        # Format with exactly 2 decimal places - never 1 or 0 decimal places
        # This ensures strict XSD validation compliance
        return f"{self.amount:.2f}"
    
    def to_xml_string(self):
        """Return XSD-compliant amount string with exactly 2 decimal places"""
        return f"{self.amount:.2f}"


@dataclass
class PostalAddress:
    """Postal address information - enhanced for CBPR+ compliance"""
    country: str
    address_type: Optional[str] = None
    department: Optional[str] = None
    sub_department: Optional[str] = None
    street_name: Optional[str] = None
    building_number: Optional[str] = None
    building_name: Optional[str] = None
    floor: Optional[str] = None
    post_box: Optional[str] = None
    room: Optional[str] = None
    post_code: Optional[str] = None
    town_name: Optional[str] = None
    town_location_name: Optional[str] = None
    district_name: Optional[str] = None
    country_sub_division: Optional[str] = None
    address_line: Optional[List[str]] = None  # Free format address lines


@dataclass
class OrganisationIdentification:
    """Organisation identification details"""
    bic_or_bei: Optional[str] = None
    other_id: Optional[str] = None
    scheme_name: Optional[str] = None
    issuer: Optional[str] = None


@dataclass
class PartyIdentification:
    """Party identification information"""
    name: Optional[str] = None
    postal_address: Optional[PostalAddress] = None
    identification: Optional[OrganisationIdentification] = None


@dataclass
class FinancialInstitutionIdentification:
    """Financial institution identification"""
    bicfi: Optional[str] = None
    clearing_system_member_id: Optional[str] = None
    name: Optional[str] = None
    postal_address: Optional[PostalAddress] = None


@dataclass
class BranchAndFinancialInstitutionIdentification:
    """Branch and financial institution identification"""
    financial_institution_identification: FinancialInstitutionIdentification
    branch_identification: Optional[str] = None


@dataclass
class CashAccount:
    """Cash account information"""
    identification: str  # IBAN or other account identifier
    account_type: Optional[str] = None
    currency: Optional[str] = None
    name: Optional[str] = None


@dataclass
class PaymentIdentification:
    """Payment identification details"""
    instruction_identification: Optional[str] = None
    end_to_end_identification: str = field(default_factory=generate_cbpr_compliant_id)
    transaction_identification: str = field(default_factory=generate_cbpr_compliant_id)  # Mandatory for CBPR+
    clearing_system_reference: Optional[str] = None


@dataclass
class ClearingSystemIdentification:
    """Clearing system identification"""
    code: Optional[str] = None
    proprietary: Optional[str] = None


@dataclass
class SettlementInstruction:
    """Settlement instruction information"""
    settlement_method: str = "RTGS"  # RTGS for TARGET2 (real-time gross settlement), CLRG for clearing systems
    settlement_account: Optional[CashAccount] = None
    clearing_system: Optional[ClearingSystemIdentification] = None
    
    def __post_init__(self):
        """Validate settlement method and clearing system compatibility"""
        # TARGET2 is an RTGS system, should use RTGS settlement method
        if (self.clearing_system and 
            self.clearing_system.code == "TARGET2" and 
            self.settlement_method in ["CLRG", "INDA"]):
            self.settlement_method = "RTGS"  # Auto-correct for TARGET2 RTGS compatibility


@dataclass
class GroupHeader:
    """Group header for PACS.008 message"""
    message_identification: str = field(default_factory=generate_cbpr_compliant_id)
    creation_date_time: datetime = field(default_factory=datetime.now)
    number_of_transactions: str = "1"
    total_interbank_settlement_amount: Optional[Amount] = None
    interbank_settlement_date: Optional[date] = None
    settlement_information: SettlementInstruction = field(default_factory=SettlementInstruction)
    instructing_agent: Optional[BranchAndFinancialInstitutionIdentification] = None
    instructed_agent: Optional[BranchAndFinancialInstitutionIdentification] = None


@dataclass
class RemittanceInformation:
    """Remittance information"""
    unstructured: Optional[List[str]] = None
    structured: Optional[Dict[str, Any]] = None


@dataclass
class ServiceLevel:
    """Service level information"""
    code: Optional[str] = None
    proprietary: Optional[str] = None


@dataclass
class LocalInstrument:
    """Local instrument information"""
    code: Optional[str] = None
    proprietary: Optional[str] = None


@dataclass
class CategoryOfPurpose:
    """Category of purpose information"""
    code: Optional[str] = None
    proprietary: Optional[str] = None


@dataclass
class PaymentTypeInformation:
    """Payment type information for CBPR+ compliance"""
    instruction_priority: Optional[str] = None  # HIGH, NORM
    service_level: Optional[ServiceLevel] = None
    local_instrument: Optional[LocalInstrument] = None
    category_of_purpose: Optional[CategoryOfPurpose] = None


def create_target2_payment_type_info() -> PaymentTypeInformation:
    """Create TARGET2-optimized payment type information
    
    Returns PaymentTypeInformation configured for TARGET2 high-priority processing:
    - InstrPrty: HIGH (urgent processing)
    - SvcLvl: URGP (urgent payment service level)  
    - LclInstrm: INST (instant/high-priority local instrument)
    """
    return PaymentTypeInformation(
        instruction_priority="HIGH",
        service_level=ServiceLevel(code="URGP"),
        local_instrument=LocalInstrument(code="INST")
    )


def get_messages_path(filename: str) -> str:
    """Get the full path for a message file in the messages folder
    
    Args:
        filename: Name of the file
        
    Returns:
        Full path to the file in the messages folder
    """
    import os
    messages_dir = "messages"
    if not os.path.exists(messages_dir):
        os.makedirs(messages_dir)
    return os.path.join(messages_dir, filename)


@dataclass
class CreditTransferTransaction:
    """Credit transfer transaction information for PACS.008"""
    payment_identification: PaymentIdentification
    interbank_settlement_amount: Amount
    debtor: PartyIdentification
    debtor_agent: BranchAndFinancialInstitutionIdentification
    creditor_agent: BranchAndFinancialInstitutionIdentification
    creditor: PartyIdentification
    
    # Mandatory fields in ISO 20022 and CBPR+
    payment_type_information: Optional[PaymentTypeInformation] = None
    interbank_settlement_date: date = field(default_factory=get_next_business_day)
    settlement_priority: Optional[str] = None
    charge_bearer: str = "SLEV"  # SLEV (service level default) recommended for TARGET2 cross-border flows
    debtor_account: Optional[CashAccount] = None
    creditor_account: Optional[CashAccount] = None
    ultimate_debtor: Optional[PartyIdentification] = None
    ultimate_creditor: Optional[PartyIdentification] = None
    instructing_agent: BranchAndFinancialInstitutionIdentification = None  # Mandatory for CBPR+ routing
    instructed_agent: BranchAndFinancialInstitutionIdentification = None   # Mandatory for CBPR+ routing  
    purpose: Optional[str] = None
    remittance_information: Optional[RemittanceInformation] = None


@dataclass
class PACS008Message:
    """PACS.008 - Financial Institution to Financial Institution Customer Credit Transfer"""
    group_header: GroupHeader
    credit_transfer_transaction_information: List[CreditTransferTransaction]


# PACS.002 Message Structures

@dataclass
class OriginalGroupInformation:
    """Original group information for status report"""
    original_message_identification: str
    original_message_name_identification: str = "pacs.008.001.08"
    original_creation_date_time: Optional[datetime] = None
    original_number_of_transactions: Optional[str] = None
    original_control_sum: Optional[Decimal] = None
    group_status: Optional[str] = None  # ACCP, RJCT, PDNG, etc.
    status_reason_information: Optional[List[str]] = None


@dataclass
class OriginalTransactionReference:
    """Original transaction reference information"""
    interbank_settlement_amount: Optional[Amount] = None
    interbank_settlement_date: Optional[date] = None
    payment_identification: Optional[PaymentIdentification] = None
    payment_type_information: Optional[Dict[str, Any]] = None
    settlement_information: Optional[SettlementInstruction] = None
    debtor: Optional[PartyIdentification] = None
    debtor_account: Optional[CashAccount] = None
    debtor_agent: Optional[BranchAndFinancialInstitutionIdentification] = None
    creditor_agent: Optional[BranchAndFinancialInstitutionIdentification] = None
    creditor: Optional[PartyIdentification] = None
    creditor_account: Optional[CashAccount] = None


@dataclass
class TransactionInformationAndStatus:
    """Transaction information and status for PACS.002"""
    status_identification: Optional[str] = None
    original_instruction_identification: Optional[str] = None
    original_end_to_end_identification: str = field(default_factory=lambda: str(uuid.uuid4()))
    original_transaction_identification: Optional[str] = None
    transaction_status: str = "ACCP"  # ACCP, RJCT, PDNG, ACSC, ACWC, ACCC
    status_reason_information: Optional[List[str]] = None
    charges_information: Optional[List[Dict[str, Any]]] = None
    acceptance_date_time: Optional[datetime] = None
    account_servicer_reference: Optional[str] = None
    clearing_system_reference: Optional[str] = None
    instructing_agent: Optional[BranchAndFinancialInstitutionIdentification] = None
    instructed_agent: Optional[BranchAndFinancialInstitutionIdentification] = None
    original_transaction_reference: Optional[OriginalTransactionReference] = None


@dataclass
class PACS002GroupHeader:
    """Group header for PACS.002 message"""
    message_identification: str = field(default_factory=generate_cbpr_compliant_id)
    creation_date_time: datetime = field(default_factory=datetime.now)
    instructing_agent: Optional[BranchAndFinancialInstitutionIdentification] = None
    instructed_agent: Optional[BranchAndFinancialInstitutionIdentification] = None


@dataclass
class PACS002Message:
    """PACS.002 - Financial Institution to Financial Institution Payment Status Report"""
    group_header: PACS002GroupHeader
    original_group_information_and_status: OriginalGroupInformation
    transaction_information_and_status: Optional[List[TransactionInformationAndStatus]] = None


@dataclass
class BusinessApplicationHeader:
    """Business Application Header (BAH) for production SWIFT messages"""
    character_set: str = "UTF-8"
    from_party: str = ""  # BIC of sending institution
    to_party: str = ""    # BIC of receiving institution
    business_message_identifier: str = field(default_factory=generate_cbpr_compliant_id)
    message_definition_identifier: str = "pacs.008.001.08"
    business_service: str = "swift.cbprplus.01"
    creation_date: datetime = field(default_factory=datetime.now)
    copy_duplicate: str = "CODU"  # COPY or DUPL
    possible_duplicate: bool = False


class SWIFTMessageGenerator:
    """Generator for SWIFT PACS messages"""
    
    def __init__(self):
        self.namespace = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"
        self.pacs002_namespace = "urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10"
        self.bah_namespace = "urn:iso:std:iso:20022:tech:xsd:head.001.001.01"
    
    def generate_pacs008(self, message: PACS008Message) -> str:
        """Generate PACS.008 XML message"""
        root = Element("Document")
        root.set("xmlns", self.namespace)
        
        fit_to_fi_cct = SubElement(root, "FIToFICstmrCdtTrf")
        
        # Group Header
        grp_hdr = SubElement(fit_to_fi_cct, "GrpHdr")
        SubElement(grp_hdr, "MsgId").text = message.group_header.message_identification
        # Use XSD-compliant timestamp with milliseconds for better compatibility
        msg_id_obj = MessageIdentification(creation_date_time=message.group_header.creation_date_time)
        SubElement(grp_hdr, "CreDtTm").text = msg_id_obj.get_xsd_compliant_timestamp()
        SubElement(grp_hdr, "NbOfTxs").text = message.group_header.number_of_transactions
        
        if message.group_header.total_interbank_settlement_amount:
            ttl_amt = SubElement(grp_hdr, "TtlIntrBkSttlmAmt")
            ttl_amt.set("Ccy", message.group_header.total_interbank_settlement_amount.currency)
            ttl_amt.text = message.group_header.total_interbank_settlement_amount.to_xml_string()
        
        if message.group_header.interbank_settlement_date:
            SubElement(grp_hdr, "IntrBkSttlmDt").text = message.group_header.interbank_settlement_date.isoformat()
        
        # Settlement Information
        sttlm_inf = SubElement(grp_hdr, "SttlmInf")
        SubElement(sttlm_inf, "SttlmMtd").text = message.group_header.settlement_information.settlement_method
        
        # Add clearing system information (for both INDA and CLRG methods)
        if message.group_header.settlement_information.clearing_system:
            clr_sys = SubElement(sttlm_inf, "ClrSys")
            if message.group_header.settlement_information.clearing_system.code:
                SubElement(clr_sys, "Cd").text = message.group_header.settlement_information.clearing_system.code
            elif message.group_header.settlement_information.clearing_system.proprietary:
                SubElement(clr_sys, "Prtry").text = message.group_header.settlement_information.clearing_system.proprietary
            else:
                # Default clearing system for EUR with CLRG
                clr_sys = SubElement(sttlm_inf, "ClrSys")
                SubElement(clr_sys, "Cd").text = "TARGET2"
        
        # Add settlement account if provided
        if message.group_header.settlement_information.settlement_account:
            self._add_account(sttlm_inf, "SttlmAcct", message.group_header.settlement_information.settlement_account)
        
        # Add agents if present
        if message.group_header.instructing_agent:
            self._add_agent(grp_hdr, "InstgAgt", message.group_header.instructing_agent)
        
        if message.group_header.instructed_agent:
            self._add_agent(grp_hdr, "InstdAgt", message.group_header.instructed_agent)
        
        # Credit Transfer Transaction Information
        for cdt_trf_tx_inf in message.credit_transfer_transaction_information:
            tx_inf = SubElement(fit_to_fi_cct, "CdtTrfTxInf")
            
            # Payment Identification
            pmt_id = SubElement(tx_inf, "PmtId")
            if cdt_trf_tx_inf.payment_identification.instruction_identification:
                SubElement(pmt_id, "InstrId").text = cdt_trf_tx_inf.payment_identification.instruction_identification
            SubElement(pmt_id, "EndToEndId").text = cdt_trf_tx_inf.payment_identification.end_to_end_identification
            SubElement(pmt_id, "TxId").text = cdt_trf_tx_inf.payment_identification.transaction_identification  # Mandatory for CBPR+
            
            # Payment Type Information (CBPR+ compliance)
            if cdt_trf_tx_inf.payment_type_information:
                pmt_tp_inf = SubElement(tx_inf, "PmtTpInf")
                
                if cdt_trf_tx_inf.payment_type_information.instruction_priority:
                    SubElement(pmt_tp_inf, "InstrPrty").text = cdt_trf_tx_inf.payment_type_information.instruction_priority
                
                if cdt_trf_tx_inf.payment_type_information.service_level:
                    svc_lvl = SubElement(pmt_tp_inf, "SvcLvl")
                    if cdt_trf_tx_inf.payment_type_information.service_level.code:
                        SubElement(svc_lvl, "Cd").text = cdt_trf_tx_inf.payment_type_information.service_level.code
                    elif cdt_trf_tx_inf.payment_type_information.service_level.proprietary:
                        SubElement(svc_lvl, "Prtry").text = cdt_trf_tx_inf.payment_type_information.service_level.proprietary
                
                if cdt_trf_tx_inf.payment_type_information.local_instrument:
                    lcl_instrm = SubElement(pmt_tp_inf, "LclInstrm")
                    if cdt_trf_tx_inf.payment_type_information.local_instrument.code:
                        SubElement(lcl_instrm, "Cd").text = cdt_trf_tx_inf.payment_type_information.local_instrument.code
                    elif cdt_trf_tx_inf.payment_type_information.local_instrument.proprietary:
                        SubElement(lcl_instrm, "Prtry").text = cdt_trf_tx_inf.payment_type_information.local_instrument.proprietary
                
                if cdt_trf_tx_inf.payment_type_information.category_of_purpose:
                    ctgy_purp = SubElement(pmt_tp_inf, "CtgyPurp")
                    if cdt_trf_tx_inf.payment_type_information.category_of_purpose.code:
                        SubElement(ctgy_purp, "Cd").text = cdt_trf_tx_inf.payment_type_information.category_of_purpose.code
                    elif cdt_trf_tx_inf.payment_type_information.category_of_purpose.proprietary:
                        SubElement(ctgy_purp, "Prtry").text = cdt_trf_tx_inf.payment_type_information.category_of_purpose.proprietary
            
            # Interbank Settlement Amount
            intrbank_amt = SubElement(tx_inf, "IntrBkSttlmAmt")
            intrbank_amt.set("Ccy", cdt_trf_tx_inf.interbank_settlement_amount.currency)
            intrbank_amt.text = cdt_trf_tx_inf.interbank_settlement_amount.to_xml_string()
            
            # Settlement Date (mandatory)
            SubElement(tx_inf, "IntrBkSttlmDt").text = cdt_trf_tx_inf.interbank_settlement_date.isoformat()
            
            # Settlement Priority
            if cdt_trf_tx_inf.settlement_priority:
                SubElement(tx_inf, "SttlmPrty").text = cdt_trf_tx_inf.settlement_priority
            
            # Charge Bearer (CBPR+ compliance)
            SubElement(tx_inf, "ChrgBr").text = cdt_trf_tx_inf.charge_bearer
            
            # Transaction-level agents (Mandatory for CBPR+ routing)
            self._add_agent(tx_inf, "InstgAgt", cdt_trf_tx_inf.instructing_agent)
            self._add_agent(tx_inf, "InstdAgt", cdt_trf_tx_inf.instructed_agent)
            
            # Agents
            self._add_agent(tx_inf, "DbtrAgt", cdt_trf_tx_inf.debtor_agent)
            self._add_agent(tx_inf, "CdtrAgt", cdt_trf_tx_inf.creditor_agent)
            
            # Parties
            self._add_party(tx_inf, "Dbtr", cdt_trf_tx_inf.debtor)
            if cdt_trf_tx_inf.debtor_account:
                self._add_account(tx_inf, "DbtrAcct", cdt_trf_tx_inf.debtor_account)
            
            self._add_party(tx_inf, "Cdtr", cdt_trf_tx_inf.creditor)
            if cdt_trf_tx_inf.creditor_account:
                self._add_account(tx_inf, "CdtrAcct", cdt_trf_tx_inf.creditor_account)
            
            # Ultimate parties
            if cdt_trf_tx_inf.ultimate_debtor:
                self._add_party(tx_inf, "UltmtDbtr", cdt_trf_tx_inf.ultimate_debtor)
            if cdt_trf_tx_inf.ultimate_creditor:
                self._add_party(tx_inf, "UltmtCdtr", cdt_trf_tx_inf.ultimate_creditor)
            
            # Purpose
            if cdt_trf_tx_inf.purpose:
                purp = SubElement(tx_inf, "Purp")
                SubElement(purp, "Cd").text = cdt_trf_tx_inf.purpose
            
            # Remittance Information
            if cdt_trf_tx_inf.remittance_information:
                rmt_inf = SubElement(tx_inf, "RmtInf")
                if cdt_trf_tx_inf.remittance_information.unstructured:
                    for ustrd in cdt_trf_tx_inf.remittance_information.unstructured:
                        SubElement(rmt_inf, "Ustrd").text = ustrd
        
        return self._prettify_xml(root)
    
    def generate_pacs002(self, message: PACS002Message) -> str:
        """Generate PACS.002 XML message"""
        root = Element("Document")
        root.set("xmlns", self.pacs002_namespace)
        
        fit_to_fi_pmt_sts_rpt = SubElement(root, "FIToFIPmtStsRpt")
        
        # Group Header
        grp_hdr = SubElement(fit_to_fi_pmt_sts_rpt, "GrpHdr")
        SubElement(grp_hdr, "MsgId").text = message.group_header.message_identification
        # Use XSD-compliant timestamp with milliseconds for better compatibility
        msg_id_obj = MessageIdentification(creation_date_time=message.group_header.creation_date_time)
        SubElement(grp_hdr, "CreDtTm").text = msg_id_obj.get_xsd_compliant_timestamp()
        
        if message.group_header.instructing_agent:
            self._add_agent(grp_hdr, "InstgAgt", message.group_header.instructing_agent)
        if message.group_header.instructed_agent:
            self._add_agent(grp_hdr, "InstdAgt", message.group_header.instructed_agent)
        
        # Original Group Information and Status
        orgnl_grp_inf_and_sts = SubElement(fit_to_fi_pmt_sts_rpt, "OrgnlGrpInfAndSts")
        SubElement(orgnl_grp_inf_and_sts, "OrgnlMsgId").text = message.original_group_information_and_status.original_message_identification
        SubElement(orgnl_grp_inf_and_sts, "OrgnlMsgNmId").text = message.original_group_information_and_status.original_message_name_identification
        
        if message.original_group_information_and_status.original_creation_date_time:
            # Use XSD-compliant timestamp formatting for original creation time
            orig_msg_id_obj = MessageIdentification(creation_date_time=message.original_group_information_and_status.original_creation_date_time)
            SubElement(orgnl_grp_inf_and_sts, "OrgnlCreDtTm").text = orig_msg_id_obj.get_xsd_compliant_timestamp()
        
        if message.original_group_information_and_status.original_number_of_transactions:
            SubElement(orgnl_grp_inf_and_sts, "OrgnlNbOfTxs").text = message.original_group_information_and_status.original_number_of_transactions
        
        if message.original_group_information_and_status.group_status:
            SubElement(orgnl_grp_inf_and_sts, "GrpSts").text = message.original_group_information_and_status.group_status
        
        # Transaction Information and Status
        if message.transaction_information_and_status:
            for tx_inf_and_sts in message.transaction_information_and_status:
                tx_inf_element = SubElement(fit_to_fi_pmt_sts_rpt, "TxInfAndSts")
                
                if tx_inf_and_sts.status_identification:
                    SubElement(tx_inf_element, "StsId").text = tx_inf_and_sts.status_identification
                
                if tx_inf_and_sts.original_instruction_identification:
                    SubElement(tx_inf_element, "OrgnlInstrId").text = tx_inf_and_sts.original_instruction_identification
                
                SubElement(tx_inf_element, "OrgnlEndToEndId").text = tx_inf_and_sts.original_end_to_end_identification
                
                if tx_inf_and_sts.original_transaction_identification:
                    SubElement(tx_inf_element, "OrgnlTxId").text = tx_inf_and_sts.original_transaction_identification
                
                SubElement(tx_inf_element, "TxSts").text = tx_inf_and_sts.transaction_status
                
                if tx_inf_and_sts.status_reason_information:
                    for reason in tx_inf_and_sts.status_reason_information:
                        sts_rsn_inf = SubElement(tx_inf_element, "StsRsnInf")
                        SubElement(sts_rsn_inf, "Rsn").text = reason
                
                if tx_inf_and_sts.acceptance_date_time:
                    SubElement(tx_inf_element, "AccptncDtTm").text = tx_inf_and_sts.acceptance_date_time.isoformat()
                
                if tx_inf_and_sts.instructing_agent:
                    self._add_agent(tx_inf_element, "InstgAgt", tx_inf_and_sts.instructing_agent)
                
                if tx_inf_and_sts.instructed_agent:
                    self._add_agent(tx_inf_element, "InstdAgt", tx_inf_and_sts.instructed_agent)
        
        return self._prettify_xml(root)
    
    def _add_agent(self, parent: Element, tag: str, agent: BranchAndFinancialInstitutionIdentification):
        """Add agent information to XML element"""
        agent_elem = SubElement(parent, tag)
        fin_instn_id = SubElement(agent_elem, "FinInstnId")
        
        if agent.financial_institution_identification.bicfi:
            SubElement(fin_instn_id, "BICFI").text = agent.financial_institution_identification.bicfi
        
        if agent.financial_institution_identification.clearing_system_member_id:
            clr_sys_mmb_id = SubElement(fin_instn_id, "ClrSysMmbId")
            SubElement(clr_sys_mmb_id, "MmbId").text = agent.financial_institution_identification.clearing_system_member_id
        
        if agent.financial_institution_identification.name:
            SubElement(fin_instn_id, "Nm").text = agent.financial_institution_identification.name
        
        if agent.financial_institution_identification.postal_address:
            self._add_postal_address(fin_instn_id, "PstlAdr", agent.financial_institution_identification.postal_address)
    
    def _add_party(self, parent: Element, tag: str, party: PartyIdentification):
        """Add party information to XML element"""
        party_elem = SubElement(parent, tag)
        
        if party.name:
            SubElement(party_elem, "Nm").text = party.name
        
        # Add postal address (recommended for compliance)
        if party.postal_address:
            self._add_postal_address(party_elem, "PstlAdr", party.postal_address)
        
        if party.identification:
            id_elem = SubElement(party_elem, "Id")
            org_id = SubElement(id_elem, "OrgId")
            if party.identification.bic_or_bei:
                SubElement(org_id, "BICOrBEI").text = party.identification.bic_or_bei
    
    def _add_account(self, parent: Element, tag: str, account: CashAccount):
        """Add account information to XML element"""
        account_elem = SubElement(parent, tag)
        id_elem = SubElement(account_elem, "Id")
        SubElement(id_elem, "IBAN").text = account.identification
        
        if account.account_type:
            SubElement(account_elem, "Tp")
        
        if account.currency:
            SubElement(account_elem, "Ccy").text = account.currency
        
        if account.name:
            SubElement(account_elem, "Nm").text = account.name
    
    def _add_postal_address(self, parent: Element, tag: str, address: PostalAddress):
        """Add postal address to XML element - enhanced for CBPR+ compliance"""
        addr_elem = SubElement(parent, tag)
        
        if address.address_type:
            SubElement(addr_elem, "AdrTp").text = address.address_type
        if address.department:
            SubElement(addr_elem, "Dept").text = address.department
        if address.sub_department:
            SubElement(addr_elem, "SubDept").text = address.sub_department
        if address.street_name:
            SubElement(addr_elem, "StrtNm").text = address.street_name
        if address.building_number:
            SubElement(addr_elem, "BldgNb").text = address.building_number
        if address.building_name:
            SubElement(addr_elem, "BldgNm").text = address.building_name
        if address.floor:
            SubElement(addr_elem, "Flr").text = address.floor
        if address.post_box:
            SubElement(addr_elem, "PstBx").text = address.post_box
        if address.room:
            SubElement(addr_elem, "Room").text = address.room
        if address.post_code:
            SubElement(addr_elem, "PstCd").text = address.post_code
        if address.town_name:
            SubElement(addr_elem, "TwnNm").text = address.town_name
        if address.town_location_name:
            SubElement(addr_elem, "TwnLctnNm").text = address.town_location_name
        if address.district_name:
            SubElement(addr_elem, "DstrctNm").text = address.district_name
        if address.country_sub_division:
            SubElement(addr_elem, "CtrySubDvsn").text = address.country_sub_division
        
        # Add free format address lines if provided
        if address.address_line:
            for line in address.address_line[:7]:  # Max 7 lines allowed in ISO 20022
                SubElement(addr_elem, "AdrLine").text = line
        
        SubElement(addr_elem, "Ctry").text = address.country
        
        # Operational compliance check: Many banks require at least one of StrtNm, PstCd, TwnNm
        has_required_field = (address.street_name or address.post_code or 
                            address.town_name or address.address_line)
        if not has_required_field:
            # Add a warning comment in development/debugging scenarios
            pass  # This will be handled by validation tools
    
    def generate_pacs008_with_bah(self, message: PACS008Message, bah: BusinessApplicationHeader) -> str:
        """Generate PACS.008 XML message with Business Application Header for production"""
        # Create root RequestPayload element
        root = Element("RequestPayload")
        
        # Add Business Application Header
        app_hdr = SubElement(root, "AppHdr")
        app_hdr.set("xmlns", self.bah_namespace)
        
        SubElement(app_hdr, "CharSet").text = bah.character_set
        
        fr_elem = SubElement(app_hdr, "Fr")
        SubElement(fr_elem, "FIId").text = bah.from_party
        
        to_elem = SubElement(app_hdr, "To")
        SubElement(to_elem, "FIId").text = bah.to_party
        
        SubElement(app_hdr, "BizMsgIdr").text = bah.business_message_identifier
        SubElement(app_hdr, "MsgDefIdr").text = bah.message_definition_identifier
        SubElement(app_hdr, "BizSvc").text = bah.business_service
        SubElement(app_hdr, "CreDt").text = bah.creation_date.isoformat()
        SubElement(app_hdr, "CpyDplct").text = bah.copy_duplicate
        
        if bah.possible_duplicate:
            SubElement(app_hdr, "PssblDplct").text = "true"
        
        # Add the actual PACS.008 document
        doc_elem = SubElement(root, "Document")
        doc_elem.set("xmlns", self.namespace)
        
        # Generate the PACS.008 content (reuse existing logic)
        fit_to_fi_cct = SubElement(doc_elem, "FIToFICstmrCdtTrf")
        
        # Group Header
        grp_hdr = SubElement(fit_to_fi_cct, "GrpHdr")
        SubElement(grp_hdr, "MsgId").text = message.group_header.message_identification
        # Use XSD-compliant timestamp with milliseconds for better compatibility
        msg_id_obj = MessageIdentification(creation_date_time=message.group_header.creation_date_time)
        SubElement(grp_hdr, "CreDtTm").text = msg_id_obj.get_xsd_compliant_timestamp()
        SubElement(grp_hdr, "NbOfTxs").text = message.group_header.number_of_transactions
        
        if message.group_header.total_interbank_settlement_amount:
            ttl_amt = SubElement(grp_hdr, "TtlIntrBkSttlmAmt")
            ttl_amt.set("Ccy", message.group_header.total_interbank_settlement_amount.currency)
            ttl_amt.text = message.group_header.total_interbank_settlement_amount.to_xml_string()
        
        # Settlement Information
        sttlm_inf = SubElement(grp_hdr, "SttlmInf")
        SubElement(sttlm_inf, "SttlmMtd").text = message.group_header.settlement_information.settlement_method
        
        if message.group_header.settlement_information.settlement_method == "CLRG":
            clr_sys = SubElement(sttlm_inf, "ClrSys")
            SubElement(clr_sys, "Cd").text = "TARGET2"
        
        # Add transaction information (simplified for BAH wrapper)
        for cdt_trf_tx_inf in message.credit_transfer_transaction_information:
            tx_inf = SubElement(fit_to_fi_cct, "CdtTrfTxInf")
            
            # Payment Identification
            pmt_id = SubElement(tx_inf, "PmtId")
            SubElement(pmt_id, "EndToEndId").text = cdt_trf_tx_inf.payment_identification.end_to_end_identification
            SubElement(pmt_id, "TxId").text = cdt_trf_tx_inf.payment_identification.transaction_identification
            
            # Payment Type Information
            if cdt_trf_tx_inf.payment_type_information:
                pmt_tp_inf = SubElement(tx_inf, "PmtTpInf")
                if cdt_trf_tx_inf.payment_type_information.service_level:
                    svc_lvl = SubElement(pmt_tp_inf, "SvcLvl")
                    SubElement(svc_lvl, "Cd").text = cdt_trf_tx_inf.payment_type_information.service_level.code
            
            # Amount and settlement
            intrbank_amt = SubElement(tx_inf, "IntrBkSttlmAmt")
            intrbank_amt.set("Ccy", cdt_trf_tx_inf.interbank_settlement_amount.currency)
            intrbank_amt.text = cdt_trf_tx_inf.interbank_settlement_amount.to_xml_string()
            
            SubElement(tx_inf, "IntrBkSttlmDt").text = cdt_trf_tx_inf.interbank_settlement_date.isoformat()
            SubElement(tx_inf, "ChrgBr").text = cdt_trf_tx_inf.charge_bearer
            
            # Agents
            self._add_agent(tx_inf, "InstgAgt", cdt_trf_tx_inf.instructing_agent)
            self._add_agent(tx_inf, "InstdAgt", cdt_trf_tx_inf.instructed_agent)
            self._add_agent(tx_inf, "DbtrAgt", cdt_trf_tx_inf.debtor_agent)
            self._add_agent(tx_inf, "CdtrAgt", cdt_trf_tx_inf.creditor_agent)
            
            # Parties
            self._add_party(tx_inf, "Dbtr", cdt_trf_tx_inf.debtor)
            if cdt_trf_tx_inf.debtor_account:
                self._add_account(tx_inf, "DbtrAcct", cdt_trf_tx_inf.debtor_account)
            
            self._add_party(tx_inf, "Cdtr", cdt_trf_tx_inf.creditor)
            if cdt_trf_tx_inf.creditor_account:
                self._add_account(tx_inf, "CdtrAcct", cdt_trf_tx_inf.creditor_account)
            
            # Remittance information
            if cdt_trf_tx_inf.remittance_information:
                rmt_inf = SubElement(tx_inf, "RmtInf")
                if cdt_trf_tx_inf.remittance_information.unstructured:
                    for ustrd in cdt_trf_tx_inf.remittance_information.unstructured:
                        SubElement(rmt_inf, "Ustrd").text = ustrd
        
        return self._prettify_xml(root)
    
    def _prettify_xml(self, elem: Element) -> str:
        """Return a pretty-printed XML string"""
        rough_string = tostring(elem, 'unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")


# CLI Interface
@click.group()
def cli():
    """SWIFT Message Generator CLI - ISO 20022 XSD Compliant"""
    pass


@cli.command()
@click.argument('xml_file', type=click.Path(exists=True))
def validate_xsd(xml_file):
    """Validate XML file for XSD compliance and provide recommendations"""
    try:
        with open(xml_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        report = generate_xsd_validation_report(xml_content)
        operational_report = generate_operational_compliance_report(xml_content)
        compliance_notes = validate_xsd_compliance_notes()
        
        click.echo(f"\n🔍 Comprehensive Compliance Report for: {xml_file}")
        click.echo("=" * 70)
        
        click.echo("\n📊 XSD VALIDATION RESULTS")
        click.echo("-" * 30)
        
        # Namespace check
        if report["namespace_correct"]:
            click.echo("✅ Namespace: Correct")
        else:
            click.echo("❌ Namespace: Incorrect")
        
        # Timestamp format
        if report["timestamp_format"] == "optimal_milliseconds":
            click.echo("✅ Timestamp: Optimal (milliseconds)")
        elif report["timestamp_format"] == "microseconds_may_cause_issues":
            click.echo("⚠️  Timestamp: Uses microseconds (may cause compatibility issues)")
        else:
            click.echo("❌ Timestamp: Non-standard format")
        
        # Decimal format
        if report["decimal_format"] == "compliant":
            click.echo("✅ Amounts: Proper decimal formatting")
        else:
            click.echo("❌ Amounts: Decimal formatting issues")
        
        # BIC format
        if report["bic_format"] == "compliant":
            click.echo("✅ BIC Codes: Proper format")
        else:
            click.echo("❌ BIC Codes: Format issues")
        
        # XSD Recommendations
        if report["recommendations"]:
            click.echo("\n📋 XSD Recommendations:")
            for i, rec in enumerate(report["recommendations"], 1):
                click.echo(f"  {i}. {rec}")
        
        # Operational Compliance Results
        click.echo("\n🏦 OPERATIONAL COMPLIANCE RESULTS")
        click.echo("-" * 35)
        
        # Settlement Method
        if operational_report["settlement_method_compliance"] == "optimal":
            click.echo("✅ Settlement Method: Optimal for TARGET2 RTGS")
        elif operational_report["settlement_method_compliance"] == "problematic":
            click.echo("⚠️  Settlement Method: May cause rejections")
        else:
            click.echo("🔶 Settlement Method: Acceptable")
        
        # Address Completeness
        if operational_report["address_completeness"] == "compliant":
            click.echo("✅ Address Information: Complete and compliant")
        else:
            click.echo("⚠️  Address Information: May cause bank rejections")
        
        # Remittance Information
        if operational_report.get("remittance_info") == "present":
            click.echo("✅ Remittance Information: Present for payment identification")
        else:
            click.echo("⚠️  Remittance Information: Missing (recommended for TARGET2)")
        
        # Payment Type Information
        if operational_report["payment_type_info"] == "target2_optimized":
            click.echo("✅ Payment Type: TARGET2-optimized (HIGH/URGP/INST)")
        elif operational_report["payment_type_info"] == "sepa_compliant":
            click.echo("✅ Payment Type: SEPA compliant")
        elif operational_report["payment_type_info"] == "present":
            click.echo("🔶 Payment Type: Present but could be enhanced")
        else:
            click.echo("⚠️  Payment Type: Missing (affects routing)")
        
        # Agent Structure
        if operational_report["agent_structure"] == "minimal_compliant":
            click.echo("✅ Agent Structure: Clean and compliant")
        elif operational_report["agent_structure"] == "complete_but_redundant":
            click.echo("🔶 Agent Structure: Complete but potentially redundant")
        else:
            click.echo("❌ Agent Structure: Incomplete")
        
        # Operational Recommendations
        if operational_report["operational_recommendations"]:
            click.echo("\n⚠️  OPERATIONAL RECOMMENDATIONS:")
            for i, rec in enumerate(operational_report["operational_recommendations"], 1):
                click.echo(f"  {i}. {rec}")
        
        # Compliance notes
        click.echo("\n📖 COMPLIANCE GUIDELINES:")
        click.echo("  XSD Requirements:")
        for category, note in compliance_notes.items():
            click.echo(f"    • {category.replace('_', ' ').title()}: {note}")
        
        click.echo("\n💡 To fix issues, regenerate the XML using this tool with updated parameters.")
        
    except Exception as e:
        click.echo(f"❌ Error validating file: {e}", err=True)


@cli.command()
@click.option('--message-id', default=None, help='Message ID (auto-generated if not provided)')
@click.option('--amount', required=True, type=float, help='Transfer amount')
@click.option('--currency', required=True, help='Currency code (e.g., EUR, USD)')
@click.option('--debtor-name', required=True, help='Debtor name')
@click.option('--debtor-bic', required=True, help='Debtor agent BIC')
@click.option('--creditor-name', required=True, help='Creditor name')
@click.option('--creditor-bic', required=True, help='Creditor agent BIC')
@click.option('--debtor-iban', help='Debtor IBAN')
@click.option('--creditor-iban', help='Creditor IBAN')
@click.option('--debtor-country', help='Debtor country code (2 letters, e.g., DE, FR)')
@click.option('--creditor-country', help='Creditor country code (2 letters, e.g., DE, FR)')
@click.option('--service-level', help='Service level code (e.g., SEPA, NURG)')
@click.option('--category-purpose', help='Category of purpose code (e.g., TRAD, TREA)')
@click.option('--charge-bearer', default='SHAR', type=click.Choice(['SHAR', 'OUR', 'BEN']), help='Charge bearer (default: SHAR)')
@click.option('--instruction-priority', type=click.Choice(['HIGH', 'NORM']), help='Instruction priority')
@click.option('--debtor-street', help='Debtor street address')
@click.option('--debtor-city', help='Debtor city')
@click.option('--debtor-postcode', help='Debtor postal code')
@click.option('--creditor-street', help='Creditor street address')
@click.option('--creditor-city', help='Creditor city')
@click.option('--creditor-postcode', help='Creditor postal code')
@click.option('--remittance-info', help='Remittance information')
@click.option('--charge-bearer', default='SLEV', help='Charge bearer (SLEV=service level default, SHAR=shared, OUR=sender pays, BEN=receiver pays)')
@click.option('--output', '-o', help='Output file path')
def generate_pacs008(message_id, amount, currency, debtor_name, debtor_bic, 
                     creditor_name, creditor_bic, debtor_iban, creditor_iban, 
                     debtor_country, creditor_country, service_level, category_purpose,
                     charge_bearer, instruction_priority, debtor_street, debtor_city,
                     debtor_postcode, creditor_street, creditor_city, creditor_postcode,
                     remittance_info, output):
    """Generate PACS.008 interbank payment instruction"""
    
    generator = SWIFTMessageGenerator()
    
    # Create message components
    # Determine countries from BIC codes if not provided
    debtor_ctry = debtor_country or extract_country_from_bic(debtor_bic)
    creditor_ctry = creditor_country or extract_country_from_bic(creditor_bic)
    
    # Create enhanced postal addresses for CBPR+ compliance
    # Ensure minimum required fields for production banking systems
    debtor_address = PostalAddress(
        country=debtor_ctry,
        street_name=debtor_street or "Business Address",  # Default if missing
        town_name=debtor_city or ("Frankfurt am Main" if debtor_ctry == "DE" else "Unknown City"),
        post_code=debtor_postcode or "00000"  # Placeholder if missing
    )
    
    creditor_address = PostalAddress(
        country=creditor_ctry,
        street_name=creditor_street or "Business Address",  # Default if missing  
        town_name=creditor_city or ("Paris" if creditor_ctry == "FR" else "Unknown City"),
        post_code=creditor_postcode or "00000"  # Placeholder if missing
    )
    
    # Create parties with enhanced postal addresses
    debtor = PartyIdentification(
        name=debtor_name,
        postal_address=debtor_address
    )
    creditor = PartyIdentification(
        name=creditor_name,
        postal_address=creditor_address
    )
    
    debtor_agent = BranchAndFinancialInstitutionIdentification(
        financial_institution_identification=FinancialInstitutionIdentification(bicfi=debtor_bic)
    )
    
    creditor_agent = BranchAndFinancialInstitutionIdentification(
        financial_institution_identification=FinancialInstitutionIdentification(bicfi=creditor_bic)
    )
    
    payment_amount = Amount(currency=currency, amount=Decimal(str(amount)))
    
    payment_id = PaymentIdentification()
    if message_id:
        payment_id.instruction_identification = message_id
    else:
        # Use production-style message ID with sender BIC prefix
        payment_id.instruction_identification = generate_production_message_id(debtor_bic)
        payment_id.end_to_end_identification = generate_production_transaction_id(debtor_bic)
    
    # Create payment type information - use TARGET2-optimized by default for RTGS/TARGET2
    if service_level or category_purpose or instruction_priority:
        # Custom payment type info specified
        svc_lvl = ServiceLevel(code=service_level) if service_level else None
        ctgy_purp = CategoryOfPurpose(code=category_purpose) if category_purpose else None
        
        payment_type_info = PaymentTypeInformation(
            instruction_priority=instruction_priority,
            service_level=svc_lvl,
            category_of_purpose=ctgy_purp
        )
    else:
        # Default to TARGET2-optimized payment type for high-priority processing
        payment_type_info = create_target2_payment_type_info()
    
    # Create transaction with CBPR+ compliance
    transaction = CreditTransferTransaction(
        payment_identification=payment_id,
        payment_type_information=payment_type_info,
        interbank_settlement_amount=payment_amount,
        charge_bearer=charge_bearer or "SLEV",  # Default to SLEV for TARGET2
        debtor=debtor,
        debtor_agent=debtor_agent,
        creditor_agent=creditor_agent,
        creditor=creditor,
        instructing_agent=debtor_agent,  # Mandatory for CBPR+ routing
        instructed_agent=creditor_agent   # Mandatory for CBPR+ routing
    )
    
    # Add accounts if provided
    if debtor_iban:
        transaction.debtor_account = CashAccount(identification=debtor_iban)
    if creditor_iban:
        transaction.creditor_account = CashAccount(identification=creditor_iban)
    
    # Add remittance information (required for production-grade messages)
    if remittance_info:
        transaction.remittance_information = RemittanceInformation(unstructured=[remittance_info])
    else:
        # Provide default remittance info for TARGET2/production compliance
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m")
        payment_ref = payment_id.instruction_identification or generate_cbpr_compliant_id()
        default_remittance = f"Payment reference {timestamp}-{payment_ref[:8]}"
        transaction.remittance_information = RemittanceInformation(unstructured=[default_remittance])
    
    # Create group header with CBPR+ compliant settlement information
    settlement_info = SettlementInstruction(
        settlement_method="RTGS",  # RTGS for TARGET2 clearing system
        clearing_system=ClearingSystemIdentification(code="TARGET2")
    )
    
    group_header = GroupHeader(
        message_identification=generate_production_message_id(debtor_bic),
        total_interbank_settlement_amount=payment_amount,
        settlement_information=settlement_info
    )
    
    # Create message
    message = PACS008Message(
        group_header=group_header,
        credit_transfer_transaction_information=[transaction]
    )
    
    # Generate XML
    xml_content = generator.generate_pacs008(message)
    
    if output:
        # If output path contains directory, use as-is, otherwise use messages folder
        if '/' in output or '\\' in output:
            output_path = output
        else:
            output_path = get_messages_path(output)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        click.echo(f"PACS.008 message written to {output_path}")
    else:
        # Default filename with timestamp if no output specified
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"pacs008_{timestamp}.xml"
        output_path = get_messages_path(default_filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        click.echo(f"PACS.008 message written to {output_path}")


@cli.command()
@click.option('--original-message-id', help='Original message ID to report on (optional if using --from-pacs008)')
@click.option('--transaction-status', default='ACCP', 
              type=click.Choice(['ACCP', 'RJCT', 'PDNG', 'ACSC', 'ACWC', 'ACCC']),
              help='Transaction status')
@click.option('--original-end-to-end-id', help='Original end-to-end ID (optional if using --from-pacs008)')
@click.option('--status-reason', help='Status reason information')
@click.option('--instructing-agent-bic', help='Instructing agent BIC')
@click.option('--instructed-agent-bic', help='Instructed agent BIC')
@click.option('--from-pacs008', help='Path to PACS.008 XML file to extract IDs from')
@click.option('--delay-minutes', default='1-30', help='Delay range in minutes (e.g., "5-60" or "10")')
@click.option('--output', '-o', help='Output file path')
def generate_pacs002(original_message_id, transaction_status, original_end_to_end_id,
                     status_reason, instructing_agent_bic, instructed_agent_bic, 
                     from_pacs008, delay_minutes, output):
    """Generate PACS.002 payment status report"""
    
    generator = SWIFTMessageGenerator()
    
    # Handle ID extraction from PACS.008 file if provided
    extracted_msg_id = None
    extracted_end_to_end_id = None
    base_timestamp = None
    
    if from_pacs008:
        try:
            with open(from_pacs008, 'r', encoding='utf-8') as f:
                pacs008_content = f.read()
            
            extracted_msg_id = extract_message_id_from_pacs008(pacs008_content)
            extracted_end_to_end_id = extract_end_to_end_id_from_pacs008(pacs008_content)
            
            # Extract creation timestamp from PACS.008 for realistic delay calculation
            try:
                root = fromstring(pacs008_content)
                namespaces = {'ns': 'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08'}
                creation_element = root.find('.//ns:CreDtTm', namespaces)
                if creation_element is not None:
                    # Parse the timestamp (format: 2025-08-17T10:12:29.015)
                    timestamp_text = creation_element.text
                    if timestamp_text.endswith('Z'):
                        timestamp_text = timestamp_text.replace('Z', '+00:00')
                    from datetime import datetime as dt
                    base_timestamp = dt.fromisoformat(timestamp_text)
                    click.echo(f"✅ Extracted Original Creation Time: {timestamp_text}")
            except Exception as e:
                click.echo(f"Warning: Could not extract creation timestamp from PACS.008: {e}")
            
            if extracted_msg_id:
                click.echo(f"✅ Extracted Message ID: {extracted_msg_id}")
            if extracted_end_to_end_id:
                click.echo(f"✅ Extracted End-to-End ID: {extracted_end_to_end_id}")
                
        except FileNotFoundError:
            click.echo(f"❌ Error: PACS.008 file not found: {from_pacs008}", err=True)
            return
        except Exception as e:
            click.echo(f"❌ Error reading PACS.008 file: {e}", err=True)
            return
    
    # Use extracted IDs if available, otherwise use provided parameters
    final_message_id = extracted_msg_id or original_message_id
    final_end_to_end_id = extracted_end_to_end_id or original_end_to_end_id
    
    # Validate required parameters
    if not final_message_id:
        click.echo("❌ Error: original-message-id is required when not using --from-pacs008", err=True)
        return
    if not final_end_to_end_id:
        click.echo("❌ Error: original-end-to-end-id is required when not using --from-pacs008", err=True)
        return
    
    # Parse delay configuration
    try:
        if '-' in delay_minutes:
            min_delay, max_delay = map(int, delay_minutes.split('-'))
        else:
            min_delay = max_delay = int(delay_minutes)
    except ValueError:
        click.echo(f"❌ Error: Invalid delay format '{delay_minutes}'. Use format like '5-60' or '10'", err=True)
        return
    
    # Generate delayed timestamp
    if base_timestamp:
        delayed_time = generate_delayed_timestamp(base_timestamp, min_delay, max_delay)
        click.echo(f"✅ Simulated processing delay: {(delayed_time - base_timestamp).total_seconds():.1f} seconds")
    else:
        delayed_time = generate_delayed_timestamp(min_delay_minutes=min_delay, max_delay_minutes=max_delay)
    
    # Create group header with delayed timestamp
    group_header = PACS002GroupHeader(creation_date_time=delayed_time)
    
    if instructing_agent_bic:
        group_header.instructing_agent = BranchAndFinancialInstitutionIdentification(
            financial_institution_identification=FinancialInstitutionIdentification(bicfi=instructing_agent_bic)
        )
    
    if instructed_agent_bic:
        group_header.instructed_agent = BranchAndFinancialInstitutionIdentification(
            financial_institution_identification=FinancialInstitutionIdentification(bicfi=instructed_agent_bic)
        )
    
    # Create original group information with original creation timestamp
    original_group_info = OriginalGroupInformation(
        original_message_identification=final_message_id,
        original_creation_date_time=base_timestamp,  # Include original PACS.008 timestamp
        group_status=transaction_status
    )
    
    # Create transaction status information
    transaction_info = TransactionInformationAndStatus(
        original_end_to_end_identification=final_end_to_end_id,
        transaction_status=transaction_status
    )
    
    if status_reason:
        transaction_info.status_reason_information = [status_reason]
    
    # Create message
    message = PACS002Message(
        group_header=group_header,
        original_group_information_and_status=original_group_info,
        transaction_information_and_status=[transaction_info]
    )
    
    # Generate XML
    xml_content = generator.generate_pacs002(message)
    
    if output:
        # If output path contains directory, use as-is, otherwise use messages folder
        if '/' in output or '\\' in output:
            output_path = output
        else:
            output_path = get_messages_path(output)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        click.echo(f"PACS.002 message written to {output_path}")
    else:
        # Default filename with timestamp if no output specified
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"pacs002_{timestamp}.xml"
        output_path = get_messages_path(default_filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        click.echo(f"PACS.002 message written to {output_path}")


@cli.command()
@click.option('--from-bic', required=True, help='Sending institution BIC')
@click.option('--to-bic', required=True, help='Receiving institution BIC')
@click.option('--amount', required=True, type=float, help='Transfer amount')
@click.option('--currency', required=True, help='Currency code (e.g., EUR, USD)')
@click.option('--debtor-name', required=True, help='Debtor name')
@click.option('--debtor-bic', required=True, help='Debtor agent BIC')
@click.option('--creditor-name', required=True, help='Creditor name')
@click.option('--creditor-bic', required=True, help='Creditor agent BIC')
@click.option('--service-level', default='SEPA', help='Service level code (default: SEPA)')
@click.option('--output', '-o', help='Output file path')
def generate_production_pacs008(from_bic, to_bic, amount, currency, debtor_name, debtor_bic,
                               creditor_name, creditor_bic, service_level, output):
    """Generate production-ready PACS.008 with Business Application Header"""
    
    generator = SWIFTMessageGenerator()
    
    # Create BAH
    bah = BusinessApplicationHeader(
        from_party=from_bic,
        to_party=to_bic,
        message_definition_identifier="pacs.008.001.08",
        business_service="swift.cbprplus.01"
    )
    
    # Create basic message components
    debtor_ctry = extract_country_from_bic(debtor_bic)
    creditor_ctry = extract_country_from_bic(creditor_bic)
    
    debtor = PartyIdentification(
        name=debtor_name,
        postal_address=PostalAddress(country=debtor_ctry)
    )
    creditor = PartyIdentification(
        name=creditor_name,
        postal_address=PostalAddress(country=creditor_ctry)
    )
    
    debtor_agent = BranchAndFinancialInstitutionIdentification(
        financial_institution_identification=FinancialInstitutionIdentification(bicfi=debtor_bic)
    )
    creditor_agent = BranchAndFinancialInstitutionIdentification(
        financial_institution_identification=FinancialInstitutionIdentification(bicfi=creditor_bic)
    )
    
    payment_amount = Amount(currency=currency, amount=Decimal(str(amount)))
    
    # Create payment type info with CBPR+ requirements
    payment_type_info = PaymentTypeInformation(
        service_level=ServiceLevel(code=service_level),
        category_of_purpose=CategoryOfPurpose(code="TRAD")
    )
    
    # Create transaction
    transaction = CreditTransferTransaction(
        payment_identification=PaymentIdentification(),
        payment_type_information=payment_type_info,
        interbank_settlement_amount=payment_amount,
        charge_bearer="SLEV",  # SLEV recommended for TARGET2 cross-border
        debtor=debtor,
        debtor_agent=debtor_agent,
        creditor_agent=creditor_agent,
        creditor=creditor,
        instructing_agent=debtor_agent,
        instructed_agent=creditor_agent
    )
    
    # Create group header
    settlement_info = SettlementInstruction(
        settlement_method="RTGS",  # RTGS for TARGET2 clearing system
        clearing_system=ClearingSystemIdentification(code="TARGET2")
    )
    
    group_header = GroupHeader(
        total_interbank_settlement_amount=payment_amount,
        settlement_information=settlement_info
    )
    
    # Create message
    message = PACS008Message(
        group_header=group_header,
        credit_transfer_transaction_information=[transaction]
    )
    
    # Generate BAH-wrapped XML
    xml_content = generator.generate_pacs008_with_bah(message, bah)
    
    if output:
        # If output path contains directory, use as-is, otherwise use messages folder
        if '/' in output or '\\' in output:
            output_path = output
        else:
            output_path = get_messages_path(output)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        click.echo(f"Production PACS.008 with BAH written to {output_path}")
    else:
        # Default filename with timestamp if no output specified
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"production_pacs008_bah_{timestamp}.xml"
        output_path = get_messages_path(default_filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        click.echo(f"Production PACS.008 with BAH written to {output_path}")


@cli.command()
@click.argument('pacs008_file', type=click.Path(exists=True))
def extract_ids(pacs008_file):
    """Extract Message ID and End-to-End ID from a PACS.008 XML file"""
    try:
        with open(pacs008_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        msg_id = extract_message_id_from_pacs008(xml_content)
        end_to_end_id = extract_end_to_end_id_from_pacs008(xml_content)
        
        click.echo(f"\n📋 Extracted IDs from: {pacs008_file}")
        click.echo("=" * 50)
        
        if msg_id:
            click.echo(f"💼 Message ID: {msg_id}")
        else:
            click.echo("❌ Message ID: Not found")
        
        if end_to_end_id:
            click.echo(f"🔗 End-to-End ID: {end_to_end_id}")
        else:
            click.echo("❌ End-to-End ID: Not found")
        
        # Example command to generate corresponding PACS.002
        if msg_id and end_to_end_id:
            click.echo(f"\n💡 To generate a corresponding PACS.002:")
            click.echo(f"python swift_message_generator.py generate-pacs002 \\")
            click.echo(f"  --from-pacs008 \"{pacs008_file}\" \\")
            click.echo(f"  --transaction-status ACCP \\")
            click.echo(f"  --status-reason \"Payment processed successfully\" \\")
            click.echo(f"  --instructing-agent-bic BNPAFRPP \\")
            click.echo(f"  --instructed-agent-bic DEUTDEFF")
            
    except Exception as e:
        click.echo(f"❌ Error extracting IDs: {e}", err=True)


@cli.command()
def examples():
    """Show example usage"""
    click.echo("""
SWIFT Message Generator Examples:

1. Generate PACS.008 (CBPR+ Compliant Payment Instruction):
   python swift_message_generator.py generate-pacs008 \\
     --amount 25000.00 \\
     --currency EUR \\
     --debtor-name "ACME Corporation Ltd" \\
     --debtor-bic DEUTDEFF \\
     --creditor-name "Global Trading SA" \\
     --creditor-bic BNPAFRPP \\
     --debtor-iban DE89370400440532013000 \\
     --creditor-iban FR1420041010050500013M02606 \\
     --service-level SEPA \\
     --category-purpose TRAD \\
     --charge-bearer SHAR \\
     --instruction-priority NORM \\
     --debtor-street "Bahnhofstrasse 123" \\
     --debtor-city "Frankfurt am Main" \\
     --debtor-postcode "60311" \\
     --creditor-street "Avenue des Champs-Élysées 456" \\
     --creditor-city "Paris" \\
     --creditor-postcode "75008" \\
     --remittance-info "Payment for commercial contract CT-2024-789" \\
     --output cbpr_pacs008.xml

2. Generate PACS.002 (Status Report) - Traditional Method:
   python swift_message_generator.py generate-pacs002 \\
     --original-message-id "MSG123456" \\
     --transaction-status ACCP \\
     --original-end-to-end-id "E2E123456" \\
     --status-reason "Payment accepted" \\
     --instructing-agent-bic ABCDFRPP \\
     --instructed-agent-bic XYZGFRPP \\
     --output pacs002.xml

3. Generate PACS.002 from existing PACS.008 (Recommended - Auto ID extraction):
   python swift_message_generator.py generate-pacs002 \\
     --from-pacs008 messages/my_pacs008.xml \\
     --transaction-status ACCP \\
     --status-reason "Payment processed successfully" \\
     --instructing-agent-bic BNPAFRPP \\
     --instructed-agent-bic DEUTDEFF \\
     --delay-minutes "5-30" \\
     --output pacs002_response.xml

4. Extract IDs from existing PACS.008:
   python swift_message_generator.py extract-ids messages/my_pacs008.xml

5. Interactive mode examples:
   python swift_message_generator.py generate-pacs008 --help
   python swift_message_generator.py generate-pacs002 --help
""")


if __name__ == '__main__':
    cli()
