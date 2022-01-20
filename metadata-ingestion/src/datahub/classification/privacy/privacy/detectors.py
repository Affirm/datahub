from typing import List, Optional, Any
from datahub.classification.privacy.privacy.defs import (
    NAME_COLS,
    SUPPORTED_DBS,
    DOB_COLS,
    EMAIL_COLS,
    PHONE_COLS,
    MAC_ADDRESS_COLS,
    SSN_COLS,
    ADDRESS_COLS,
    IP_ADDRESS_COLS,
    ORGANIZATION_COLS,
    SENTINEL,
)
from datahub.classification.privacy.privacy.types import EntityType, DB_COLUMN
import json
from datahub.classification.privacy.privacy.utils import nested_dict_iter
from datahub.classification.privacy.privacy.pattern_engine import PatternEngine


class BaseDetector:
    entity_type = None
    column_name_hit_list = []
    undecided = EntityType.UNDECIDED

    @property
    def rules(self):
        return NotImplementedError

    def run_detection(self, db_col: DB_COLUMN) -> bool:
        for rule in self.rules:
            if rule(db_col):
                return self.entity_type
        return self.undecided

    def check_column_name(self, db_col):
        if db_col.column.upper() in self.column_name_hit_list:
            return True
        return False

    def check_json_samples_for_column_name(self, db_col):
        for s in db_col.samples:
            if isinstance(s, str):
                try:
                    json_dict = json.loads(s)
                    if isinstance(json_dict, dict):
                        for k, v in nested_dict_iter(json_dict):
                            if k.upper() in self.column_name_hit_list:
                                return True
                except ValueError as err:
                    pass
        return False


class NameDetector(BaseDetector):
    entity_type = EntityType.NAME
    column_name_hit_list = NAME_COLS

    @property
    def rules(self):
        return [self.check_column_name, self.check_json_samples_for_column_name, self.check_user_schema_name]

    def check_user_schema_name(self, db_col: DB_COLUMN) -> bool:
        if db_col not in SUPPORTED_DBS:
            return False

        if db_col.schema == "USERS" and db_col.column == "NAME":
            return True
        return False


class DOBDetector(BaseDetector):
    entity_type = EntityType.NAME
    column_name_hit_list = DOB_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name]


class EmailDetector(BaseDetector):
    entity_type = EntityType.EMAIL
    column_name_hit_list = EMAIL_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name,
                self.check_samples_with_regex]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        samples = [str(s) for s in db_col.samples if s]
        joined_samples = "\t".join(samples)
        return PatternEngine.check_email(joined_samples)


class PhoneDetector(BaseDetector):
    entity_type = EntityType.PHONE
    column_name_hit_list = PHONE_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name]
                # self.check_samples_with_regex]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        samples = [str(s) for s in db_col.samples if s]
        lengths = [len(s) for s in samples]
        if len(set(lengths)) != 2:
            return False
        for s in samples:
            if PatternEngine.check_phone(s):
                return True
        return False


class AddressDetector(BaseDetector):
    entity_type = EntityType.ADDRESS
    column_name_hit_list = ADDRESS_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name,
                self.check_samples_with_regex]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        samples = [str(s) for s in db_col.samples if s]
        joined_samples = "\t".join(samples)
        return PatternEngine.check_street_addresses(joined_samples)


class MACAddressDetector(BaseDetector):
    entity_type = EntityType.MAC_ADDRESS
    column_name_hit_list = MAC_ADDRESS_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        for s in db_col.samples:
            if PatternEngine.check_mac_addresses(str(s)):
                return True
        return False


class SSNDetector(BaseDetector):
    entity_type = EntityType.SSN
    column_name_hit_list = SSN_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name,
                self.check_samples_with_regex]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        for s in db_col.samples:
            if PatternEngine.check_ssn(str(s)):
                return True
        return False


class IPAddressDetector(BaseDetector):
    entity_type = EntityType.IP_ADDRESS
    column_name_hit_list = IP_ADDRESS_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        for s in db_col.samples:
            if PatternEngine.check_ip_addresses(str(s)) or PatternEngine.check_ipv6_addresses(str(s)):
                return True
        return False


class IPAddressDetector(BaseDetector):
    entity_type = EntityType.IP_ADDRESS
    column_name_hit_list = IP_ADDRESS_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name]

    def check_samples_with_regex(self, db_col: DB_COLUMN) -> bool:
        for s in db_col.samples:
            if PatternEngine.check_ip_addresses(str(s)) or PatternEngine.check_ipv6_addresses(str(s)):
                return True
        return False


class OrganizationDetector(BaseDetector):
    entity_type = EntityType.ORGANIZATION
    column_name_hit_list = ORGANIZATION_COLS

    @property
    def rules(self):
        return [self.check_column_name,
                self.check_json_samples_for_column_name]
