import requests
import json
import utils

# Testing API key
api_key = 's4624iv14eij'
key_secret = 'd0ifs7vd3m80pa1'

endpoint = "https://kconnect-kb.s4.ontotext.com/v1/sparql"
lld_sparql_json_api = 'http://linkedlifedata.com/sparql.json'
lld_autocomplete_api = 'http://linkedlifedata.com/autocomplete.json?q={term}&type=disorders'

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

subconcepts_only_query_tmp = """
SELECT ?inst_full WHERE {{
   <http://linkedlifedata.com/resource/umls/id/{}> <http://www.w3.org/2004/02/skos/core#narrower> ?inst_full .
}}
"""

subconcepts_transitive_query_tmp = """
SELECT ?inst_full WHERE {{
   <http://linkedlifedata.com/resource/umls/id/{}> <http://www.w3.org/2004/02/skos/core#narrowerTransitive> ?inst_full .
}}
"""

# mimir query template
mimir_query_temp = """
{{Mention sparql = "{}"}}
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
def query_result(q, endpoint_url=None):
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    if endpoint_url is None:
        endpoint_url = endpoint
    ret = json.loads(utils.http_post_result(endpoint_url, "query=" + q,
                                            headers=headers, auth=(api_key, key_secret)))
    return ret['results']['bindings']


# generate prospector/mimir queries
def generate_prospector_query(concept_id, sparql_only=None):
    query2 = """
    select * where {{
        <http://linkedlifedata.com/resource/umls/id/{}> <http://www.w3.org/2008/05/skos-xl#prefLabel> ?mesh .
    }}
    """.format(concept_id)
    ret = query_result(query2)
    labels = [r['mesh']['value'] for r in ret]
    if len(labels) > 0:
        if sparql_only is not None:
            return query_tmp.format(labels[0])
        else:
            return mimir_query_temp.format(query_tmp.format(labels[0]))


# query to get all instances of a concept
def query_instances(concept_id):
    # q = generate_prospector_query(concept_id, sparql_only=True)
    q = subconcepts_only_query_tmp.format(concept_id)
    # print q
    ret = query_result(q)
    return [r['inst_full']['value'] for r in ret]


def get_transitive_subconcepts(concept_id):
    q = subconcepts_transitive_query_tmp.format(concept_id)
    ret = query_result(q, endpoint_url=lld_sparql_json_api)
    return [r['inst_full']['value'][r['inst_full']['value'].rfind('/') + 1:] for r in ret]


def get_all_instances(save_file):
    concepts = utils.load_json_data('./resources/exact_concpts_mappings.json')
    concpet2subconcepts_csv = ''
    for c in concepts:
        if concepts[c] == '':
            continue
        insts = query_instances(concepts[c])
        insts = [concepts[c]] + insts
        print u'{}\t{}\t{}\t{}'.format(c, concepts[c], len(insts), json.dumps(insts))
        for cid in insts:
            concpet2subconcepts_csv += u'{}, {}\n'.format(c, cid[cid.rfind('/')+1:])
    if save_file is not None:
        utils.save_string(concpet2subconcepts_csv, save_file)


def generate_top_queries():
    for c in top_freq_concepts:
        print '{}\n--{}\n\n'.format(c, generate_prospector_query(top_freq_concepts[c], sparql_only=True))


def generate_all_queries():
    concepts = utils.load_json_data('./resources/autoimmune-concepts.json')
    concept2queries = {}
    for c in concepts:
        concept2queries[c] = generate_prospector_query(concepts[c])
        print '%s done' % c
    utils.save_json_array(concept2queries, './resources/mimir_queries.json')


def get_concept_label(concept_id):
    query2 = """
    select ?label where {{
        <http://linkedlifedata.com/resource/umls/id/{}> <http://www.w3.org/2008/05/skos-xl#prefLabel> ?labelObj .
        ?labelObj <http://www.w3.org/2008/05/skos-xl#literalForm> ?label .
        FILTER ( lang(?label) = "en" )
    }}
    """.format(concept_id)
    ret = query_result(query2)
    labels = [r['label']['value'] for r in ret]

    if len(labels) > 0:
        return labels[0]
    else:
        return None


def match_term_to_concept(term):
    t = requests.get(lld_autocomplete_api.format(**{'term': term})).content
    rets = json.loads(t)
    if 'results' in rets and len(rets['results']) > 0:
        return rets['results'][0]['uri']
    return None


def main():
    # generate_all_queries()
    # get_all_instances('./resources/all_insts.csv')
    print get_concept_label('C0018799')

if __name__ == "__main__":
    # main()
    print get_transitive_subconcepts('C0011854')
    # print match_term_to_concept('Type 1 diabetes')
