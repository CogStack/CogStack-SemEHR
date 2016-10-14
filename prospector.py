import utils

# gate prospector rpc url
prospector_url = ''


# the strange prospector query template
prospector_query_temp="""
"""


def query_prospector(query):
    q = prospector_query_temp.format(query)
    r = utils.http_post_result(prospector_url, q)
    print r


def query_all_concepts():
    concept2query = utils.load_json_data('./resources/mimir_queries.json')
    for c in concept2query:
        print 'querying %s' % c
        query_prospector(concept2query[c])
        break


def main():
    query_all_concepts()

if __name__ == "__main__":
    main()
