import utils
import xml.etree.ElementTree
import random
import math

# gate prospector rpc url
prospector_url = 'http://192.168.100.101:8080/gwt/gate.prospector.rpc.ProspectorRpc/rpc'

# Mimir Web Service
mimir_service_url = 'http://192.168.100.101:8080/dcf68f8f-14b1-49e9-ab44-3380eec0a22f/search'
mimir_ns = {'m': 'http://gate.ac.uk/ns/mimir'}

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
        qid = get_xml_data(r, 'm:data/m:queryId', mimir_ns)
        print 'query id: %s' % qid

        r = query_mimir('documentsCount', {'queryId': qid})
        document_count = get_xml_data(r, 'm:data/m:value', mimir_ns)
        print 'documentCount: %s' % document_count
        if document_count != '':
            document_count = int(document_count)
            if document_count > 0:
                random_pick_results(c, qid, document_count, min(5, document_count))
            break


def random_pick_results(concept, qid, document_count, num):
    docs = []
    dids = []
    for i in range(num):
        doc = int(math.ceil(random.random() * document_count - 1))
        while doc in dids:
            doc = int(math.ceil(random.random() * document_count - 1))
        dids.append(doc)
        docs.append( query_mimir('renderDocument', {'queryId': qid, 'rank': doc}) )
    utils.save_json_array({'c': concept, 'docs': docs}, './samples/' + concept + '.json')


def get_xml_data(x, path, namespace=None):
    # print x
    e = xml.etree.ElementTree.ElementTree(xml.etree.ElementTree.fromstring(x))
    elem = e.getroot().find(path, namespace)
    # print elem
    if elem is not None:
        return elem.text
    else:
        return ''


def query_mimir(action, data):
    return utils.http_post_result('{}/{}'.format(mimir_service_url, action), data)


def main():
    query_all_concepts()

if __name__ == "__main__":
    main()
