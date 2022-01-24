from enum import Enum
from collections import namedtuple


class EntityType(Enum):
    NAME = "NAME"
    DOB = "DOB"
    EMAIL = "EMAIL"
    PHONE = "PHONE"
    ADDRESS = "ADDRESS"
    SSN = "SSN"
    MAC_ADDRESS = "MAC_ADDRESS"
    IP_ADDRESS = "IP_ADDRESS"
    ORGANIZATION = "ORGANIZATION"
    UNDECIDED = "UNDECIDED"


DB_COLUMN = namedtuple("DB_COLUMN", "database schema table column samples")