from typing import Dict, List, Generator
from concurrent.futures import ThreadPoolExecutor
import requests
import threading

FETCH_QUERY = """
query GetDatasetGlossaryTerms($search_query: String!, $page_size: Int, $offset:Int) {
  search(input: {
    type: DATASET,
    # search query cannot be empty, workaround by searching a specific dataPlatoform name
    query: $search_query,
    count: $page_size,
    start: $offset,
  }) {
    total
    searchResults {
      entity {
        ... on Dataset {
          urn
          editableSchemaMetadata {
            editableSchemaFieldInfo {
              fieldPath
              glossaryTerms {
                terms {
                  term {
                    urn
                    name
                    # This returns null for some reason even though it can't be null
                    # hierarchicalName
                  }
                }
              }
            }
          }
          # How do we get the latest version?
          schemaMetadata(version: 0) {
            name
            fields {
              fieldPath
              glossaryTerms {
                terms {
                  term {
                    urn
                    name
                    # This returns null for some reason even though it can't be null
                    # hierarchicalName
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""
BASE_URL = 'https://datahub.stage-analytics-main.affirm-stage.com'
GRAPHQL_API = '/api/graphql'
LOGIN_API = '/logIn'
PAGE_SIZE = 500
MAX_DATASET_SEARCH_RESULTS = 1000000

class TermDetacher:

    def __init__(self):
        self.cookies = self._login_to_datahub("datahub", "datahub")
        self.executor = ThreadPoolExecutor(5)
        self.lock = threading.Lock()
        self.detached = 0

    def run_detach(self, terms: List[str]) -> None:
        with ThreadPoolExecutor(3) as executor:
          for term in terms:
            matching_urns = self._get_matching_urns(term)
            for objs in matching_urns:
                for field in objs["fields"]:
                    for term in field["terms"]:
                        executor.submit(self._detach_term, objs["dataset"], field["field"], term)
        print(f"Detached {self.detached} terms")

    '''
    Ex:
    [
        {
            "dataset": "datasetUrn1",
            "fields": [
                {
                    "field": "field1",
                    "terms": ["term1Urn", "term2Urn"]
                }
            ]
        }
    ]
    '''
    def _get_matching_urns(self, term: str) -> List[object]:
        res = []
        for entity in self._yield_graphql_search_results(term):
            if entity and entity['entity'] and entity['entity']['editableSchemaMetadata']:
                dataset_res = {}
                dataset_res['dataset'] = entity['entity']['urn']
                dataset_res['fields'] = []
                has_data = False
                for field in entity['entity']['editableSchemaMetadata']['editableSchemaFieldInfo']:
                    if field['glossaryTerms'] and field['glossaryTerms']['terms']:
                        terms_res = []
                        for term in field['glossaryTerms']['terms']:
                            terms_res.append(term['term']['urn'])
                            has_data |= True
                        dataset_res['fields'].append({
                            "field": field['fieldPath'],
                            "terms": terms_res
                        })
                if has_data:
                    res.append(dataset_res)
        return res

    def _detach_term(self, urn: str, field: str, term: str) -> None:
        payload = {
            "operationName": "removeTerm",
            "variables": {
                "input": {
                "termUrn": term,
                "resourceUrn": urn,
                "subResource": field,
                "subResourceType": "DATASET_FIELD"
                }   
            },  
            "query": "mutation removeTerm($input: TermAssociationInput!) {\n  removeTerm(input: $input)\n}\n"
        } 

        response = requests.post(f"{BASE_URL}{GRAPHQL_API}", json=payload, cookies=self.cookies)
        print(f"Detached {urn}:{field} term {term}, response text {response.text}")
        self.lock.acquire()
        self.detached += 1
        self.lock.release()

    def _login_to_datahub(
        self, username: str, datahub_password: str
    ) -> Dict[str, str]:
        url = f"{BASE_URL}{LOGIN_API}"
        creds = {"username": username, "password": datahub_password}

        return requests.post(url, json=creds).cookies.get_dict()

    def _yield_graphql_search_results(self, search_query: str) -> Generator[Dict, None, None]:
        total = MAX_DATASET_SEARCH_RESULTS
        offset = 0
        while offset < total:
            graphql_variables = {
                "search_query": search_query,
                "page_size": PAGE_SIZE,
                "offset": offset
            }

            payload = {'query': FETCH_QUERY, 'variables': graphql_variables}
            response = requests.post(f"{BASE_URL}{GRAPHQL_API}", json=payload, cookies=self.cookies)
            response_json = response.json()
            for entity in response_json['data']['search']['searchResults']:
                yield entity

            total = response_json['data']['search']['total']
            offset += PAGE_SIZE

cl = TermDetacher()
cl.run_detach(["PrivacyLaw.CCPA", "PrivacyLaw.PIPEDA", "PrivacyLaw.APP", "PiiData.CONTACT", "PiiData.DATE",
  "PiiData.GPE", "PiiData.MONEY", "PiiData.NORP", "PiiData.PERCENT", "PiiData.PERSON", "PiiData.TIME"])