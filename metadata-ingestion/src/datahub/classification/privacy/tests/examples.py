from privacy.api import PIIEngine

pii_engine = PIIEngine()
print(pii_engine.detect_entity_types(["jiarui.xu@affirm.com"]))
print(pii_engine.detect_entity_types(["J Xu"], "PROD__US", "ML", "Dataset", "First_Name"))
print(pii_engine.detect_entity_types(["445-48-4556"], "PROD__US", "ML", "Dataset", "SSN"))
print(pii_engine.detect_entity_types(['{"SSN": "334-56-2223", "decision": 1}', ], "PROD__US", "ML", "Dataset", "output"))
print(pii_engine.detect_entity_types(['{"COMPANY_NAME": "Affirm"}', ], "PROD__US", "ML", "Dataset", "output"))
print(pii_engine.detect_entity_types([1, 3], "PROD__US", "ML", "Dataset", "COMPANY"))
