from commonregex import CommonRegex
from datahub.classification.privacy.privacy.types import EntityType
import re


class PatternEngine:
    def __init__(self):
        pass

    @classmethod
    def check_email(cls, data):
        if data == "":
            return False
        parsed_text = CommonRegex(data)
        emails = parsed_text.emails
        if len(emails) != 0:
            return True
        return False

    @classmethod
    def check_phone(cls, data):
        if data == "":
            return False
        parsed_text = CommonRegex(data)
        phones = parsed_text.phones
        if len(phones) != 0:
            return True
        return False

    @classmethod
    def check_ssn(cls, data_item):
        ssn_re = "^(?!666|000|9\\d{2})\\d{3}-(?!00)\\d{2}-(?!0{4})\\d{4}$"
        if re.match(ssn_re, data_item):
            return True
        return False

    @classmethod
    def check_street_addresses(cls, data):
        if data == "":
            return False
        parsed_text = CommonRegex(data)
        addresses = parsed_text.street_addresses
        if len(addresses) != 0:
            return True
        return False

    @classmethod
    def check_mac_addresses(cls, data):
        mac_re = '^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$'
        if re.match(mac_re, data):
            return True
        return False

    @classmethod
    def check_ip_addresses(cls, data):
        if data == "":
            return False
        parsed_text = CommonRegex(data)
        ips = parsed_text.ip
        if len(ips) != 0:
            return True
        return False

    @classmethod
    def check_ipv6_addresses(cls, data):
        if data == "":
            return False
        parsed_text = CommonRegex(data)
        ipv6s = parsed_text.ipv6
        if len(ipv6s) != 0:
            return True
        return False
