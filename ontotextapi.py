import requests
import json
import utils

# Testing API key
api_key = 's4624iv14eij'
key_secret = 'd0ifs7vd3m80pa1'

endpoint = "https://kconnect-kb.s4.ontotext.com/v1/sparql"

# SPARQL query template for get narrower concepts
query_tmp = """
SELECT ?inst_full WHERE {{
 VALUES ?mesh {{ <{}> }}
 {{
   ?cui <http://www.w3.org/2008/05/skos-xl#prefLabel> ?mesh .
   ?cui <http://www.w3.org/2004/02/skos/core#narrower> ?inst_full .
 }}
 UNION
 {{
   ?inst_full <http://www.w3.org/2008/05/skos-xl#prefLabel> ?mesh .
 }}
}}
"""


top_freq_concepts = {"immunodeficiency ": "C0021051",
    "retinopathy": "C0035309",
    "Alopecia areata": "C0002170",
    "Peripheral neuropathy": "C0031117",
    "Haemolytic anaemia ": "C0002878",
    "ulcer": "C0041582",
    "Cardiomyopathy ": "C0878544",
    "TBM": "C0040341",
    "myocarditis": "C0027059",
    "Type 1 diabetes": "C0011854",
    "Juvenile diabetes": "C0011854",
    "diabetes Type 1": "C0011854",
    "Amyloidosis": "C0002726",
    "Psoriasis": "C0033860",
    "Agammaglobulinaemia": "C0001768",
    "Rheumatoid arthritis ": "C0003873"}


# do sparql query answering using linked life data service
def query_result(q):
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    ret = json.loads(utils.http_post_result(endpoint, "query=" + q,
                                            headers=headers, auth=(api_key, key_secret)))
    return ret['results']['bindings']


# generate prospector/mimir queries
def generate_prospector_query(concept_id):
    query2 = """
    select * where {{
        <http://linkedlifedata.com/resource/umls/id/{}> <http://www.w3.org/2008/05/skos-xl#prefLabel> ?mesh .
    }}
    """.format(concept_id)
    ret = query_result(query2)
    labels = [r['mesh']['value'] for r in ret]
    if len(labels) > 0:
        return query_tmp.format(labels[0])


# query to get all instances of a concept
def query_instances(concept_id):
    q = generate_prospector_query(concept_id)
    # print q
    ret = query_result(q)
    return [r['inst_full']['value'] for r in ret]


def get_all_instances():
    concepts = utils.load_json_data('./resources/autoimmune-concepts.json')
    for c in concepts:
        insts = query_instances(concepts[c])
        print '{}\t{}\t{}\t{}'.format(c, concepts[c], len(insts), json.dumps(insts))


def generate_top_queries():
    for c in top_freq_concepts:
        print '{}\n--{}\n\n'.format(c, generate_prospector_query(top_freq_concepts[c]))


def main():
    generate_top_queries()

if __name__ == "__main__":
    main()

