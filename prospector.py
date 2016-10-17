import utils
import xml.etree.ElementTree

# gate prospector rpc url
prospector_url = 'http://192.168.100.101:8080/gwt/gate.prospector.rpc.ProspectorRpc/rpc'

# Mimir Web Service
mimir_service_url = 'http://192.168.100.101:8080/dcf68f8f-14b1-49e9-ab44-3380eec0a22f/search'


# the strange prospector query template
prospector_query_temp="""
7|0|7|http://192.168.100.101:8080/gwt/gate.prospector.Prospector/|86F4D95DC3510B0A551734E474D9BBC7|gate.prospector.rpc.client.ProspectorRpcService|search|gate.prospector.rpc.client.SearchSpec/996986553|dcf68f8f-14b1-49e9-ab44-3380eec0a22f|{}|1|2|3|4|1|5|5|6|7|
"""


def query_prospector(query):
    q = prospector_query_temp.format(query)
    print q
    r = utils.http_post_result(prospector_url, q)
    print r


def query_all_concepts():
    concept2query = utils.load_json_data('./resources/mimir_queries.json')
    for c in concept2query:
        print 'querying %s' % c
        r = query_mimir('postQuery', {'queryString': concept2query[c]})
        print r
        e = xml.etree.ElementTree.fromstring(r)
        print e.find('queryId')
        break


def query_mimir(action, data):
    return utils.http_post_result('{}/{}'.format(mimir_service_url, action), data)


def main():
    #query_all_concepts()
    query_mimir('postQuery', {'queryString': 'mental'})

if __name__ == "__main__":
    main()
