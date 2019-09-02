import utils
import json
from umls_api.Authentication import Authentication
import requests
import sys
import urllib2
import urllib
import traceback
import math
import logging


class UMLSAPI(object):
    """
    Using UMLS APIs to get semantic associations between concepts
    """
    api_url = "https://uts-ws.nlm.nih.gov/rest"

    def __init__(self, api_key):
        self._tgt = None
        self._api_key = api_key
        self._auth = Authentication(api_key)

    @property
    def tgt(self):
        if self._tgt is None:
            self._tgt = self._auth.gettgt()
        return self._tgt

    def invalidate_tgt(self):
        self._tgt = None

    def get_st(self):
        return self._auth.getst(self.tgt)

    def match_term(self, term):
        query = {'ticket': self.get_st(), 'string': term}
        content_endpoint = self.api_url + '/search/current'
        r = requests.get(content_endpoint, params=query)
        r.encoding = 'utf-8'
        logging.debug(r.text)
        items = json.loads(r.text)
        jsonData = items["result"]
        return [o for o in jsonData['results']]

    def get_atoms(self, cui):
        content_endpoint = self.api_url + ('/content/current/CUI/%s/atoms' % cui)
        return [o['ui'] for o in self.get_all_objects(content_endpoint)]

    def get_aui_descendants(self, aui):
        content_endpoint = self.api_url + ('/content/current/AUI/%s/descendants' % 'A10134087')
        return [o['concept'][o['concept'].rfind('/')+1:] for o in self.get_all_objects(content_endpoint)]

    def get_cui_descendants(self, cui):
        auis = self.get_atoms(cui)
        descendants = []
        for aui in auis:
            descendants += self.get_aui_descendants(aui)

    def get_narrower_concepts(self, cui):
        content_endpoint = self.api_url + ('/content/current/CUI/%s/relations' % cui)
        return [(c['relatedId'][c['relatedId'].rfind('/')+1:], c['relationLabel'])
                for c in self.get_all_objects(content_endpoint) if c['relationLabel'] == 'RB']

    def get_all_objects(self, content_endpoint):
        objects = []
        obj = self.get_object(content_endpoint)
        if obj is not None:
            objects += obj['result']
            # print 'page count: %s ' % obj['pageCount']
            for i in range(2, obj['pageCount'] + 1):
                obj = self.get_object(content_endpoint, page_number=i)
                if obj is not None:
                    objects += obj['result']
        return objects

    def get_object(self, uri, page_number=1):
        logging.debug('retrieving [%s]' % uri)
        content = requests.get(uri, params={'ticket': self.get_st(), 'pageNumber': page_number}).content
        logging.debug('request content: [%s]' % content)
        try:
            obj = json.loads(content)
            return obj
        except Exception as e:
            logging.error(str(e))
        return None

    def transitive_narrower(self, concept, processed_set=None, result_set=None, skip_relations={}):
        """
        get transitive narrower concepts of a given concept
        :param concept:
        :param processed_set:
        :param result_set:
        :return:
        """
        if result_set is None:
            result_set = set([concept])
        if processed_set is None:
            processed_set = set()
        if concept in processed_set:
            return list(result_set)
        logging.debug('get narrower concepts of %s...' % concept)
        subconcepts = []
        try:
            subconcepts = self.get_narrower_concepts(concept)
            subconcepts2ignore = [] if concept not in skip_relations else skip_relations[concept]
            if len(subconcepts2ignore) > 0:
                logging.info('concepts to skip %s' % subconcepts2ignore)
            subconcepts = [c[0] for c in subconcepts if c[0] not in subconcepts2ignore]
            logging.info('%s has %s children' % (concept, len(subconcepts)))
        except:
            logging.error('error %s ' % sys.exc_info()[0])

        result_set |= set(subconcepts)
        processed_set.add(concept)
        for c in subconcepts:
            self.transitive_narrower(c, processed_set=processed_set, result_set=result_set,
                                     skip_relations=skip_relations)
        return list(result_set)


def align_mapped_concepts(map_file, disorder_file):
    concept_map = utils.load_json_data(map_file)
    disorders = [d.strip() for d in utils.read_text_file(disorder_file)]
    exact_mapped = {}
    for d in disorders:
        if d in concept_map:
            exact_mapped[d] = concept_map[d]
        else:
            exact_mapped[d] = ""
    print json.dumps(exact_mapped)


def get_umls_concept_detail(umls, cui):
    return umls.get_object(UMLSAPI.api_url + '/content/current/CUI/%s/' % cui)


def get_umls_definitions(umls, cui):
    return umls.get_object(UMLSAPI.api_url + '/content/current/CUI/%s/definitions' % cui)


def get_umls_atoms(umls, cui):
    return umls.get_object(UMLSAPI.api_url + '/content/current/CUI/%s/atoms' % cui)


def get_umls_source_descendants(umls, source, id):
    return umls.get_object(UMLSAPI.api_url + '/content/current/source/{source}/{id}/descendants'.format(
        **{'source': source, 'id': id}))


def complete_tsv_concept_label(umls, tsv_file):
    lines = []
    for l in utils.read_text_file(tsv_file):
        arr = l.split('\t')
        print arr
        arr.insert(1, get_umls_concept_detail(umls, arr[1])['result']['name'])
        lines.append(arr)
    print '\n'.join(['\t'.join(l) for l in lines])


def get_umls_client_inst(umls_key_file):
    """
    create a umls client instance using the key stored in give file
    :param umls_key_file: the text file containing UMLS API key
    :return:
    """
    key = utils.read_text_file_as_string(umls_key_file)
    logging.info('key [%s]' % key)
    return UMLSAPI(key)


def query_result(q, endpoint_url, key_file):
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = utils.http_post_result(endpoint_url,
                                      "apikey:" + utils.read_text_file_as_string(key_file) + "&query=" + q,
                                      headers=headers)
    print response
    ret = json.loads(response)
    return ret['results']['bindings']


def query(q,apikey,epr,f='application/json'):
    """Function that uses urllib/urllib2 to issue a SPARQL query.
       By default it requests json as data format for the SPARQL resultset"""

    try:
        params = {'query': q, 'apikey': apikey}
        params = urllib.urlencode(params)
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(epr+'?'+params)
        request.add_header('Accept', f)
        request.get_method = lambda: 'GET'
        url = opener.open(request)
        return url.read()
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
        raise e


def icd10_queries():
    endpoint = 'http://sparql.bioontology.org/sparql/'
    query_template = """
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT distinct ?umls, ?label
FROM <http://bioportal.bioontology.org/ontologies/ICD10>
WHERE {{
  <http://purl.bioontology.org/ontology/ICD10/{}> <http://bioportal.bioontology.org/ontologies/umls/cui> ?umls;
  <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
  ?s <http://bioportal.bioontology.org/ontologies/umls/isRoot> true.
}}
    """
    icd2umls = {}
    for c in range(ord('A'), ord('Z')+1):
        for i in xrange(0, 100):
            icd = '%s%s' % (chr(c), '0' + str(i) if i <= 9 else str(i))
            q = query_template.format(icd)
            ret = json.loads(query(q, utils.read_text_file_as_string('./resources/HW_NCBO_KEY.txt'), endpoint))
            ret = ret['results']['bindings']
            if len(ret) > 0:
                icd2umls[icd] = ret[0]['umls']['value']
                print '%s\t%s\t%s' % (icd, ret[0]['umls']['value'], ret[0]['label']['value'])
    logging.info(json.dumps(icd2umls))


def icd10_wildcard_queries(lines, icd_file=None):
    endpoint = 'http://sparql.bioontology.org/sparql/'
    query_template = """
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT distinct ?umls
FROM <http://bioportal.bioontology.org/ontologies/ICD10>
WHERE {{
 <http://purl.bioontology.org/ontology/ICD10/{icd10}> <http://bioportal.bioontology.org/ontologies/umls/cui> ?umls.
}}
"""
    if lines is None:
        lines = utils.read_text_file(icd_file)
    icd2umls = {}
    for l in lines:
        arr = l.split("\t")
        if len(arr) == 2:
            not_mapped = True
            icd = arr[0]
            q_icd = icd
            if '#' in icd or 'X' in icd:
                q_icd = icd.replace('#', '').replace('X', '')
            queries = []
            if len(q_icd) >= 4:
                q_icd = q_icd[:3] + '.' + q_icd[3:]
                queries.append(q_icd)
            elif len(q_icd) < 4:
                gen_q = set()
                for i in xrange(int(math.pow(10, 4-len(q_icd)))):
                    ff = '%0' + str(4-len(q_icd)) + 'd'
                    q = q_icd + ff % i
                    gen_q.add(q[:3])
                    gen_q.add(q[:3] + '.' + q[3:])
                queries += list(gen_q)
            for qi in queries:
                q = query_template.format(**{'icd10': qi})
                ret = json.loads(query(q, utils.read_text_file_as_string('./resources/HW_NCBO_KEY.txt'), endpoint))
                ret = ret['results']['bindings']
                if len(ret) > 0:
                    not_mapped = False
                for r in ret:
                    if icd not in icd2umls:
                        icd2umls[icd] = []
                    icd2umls[icd].append(r['umls']['value'])
            if not_mapped:
                logging.info('%s not mapped: \n %s' % (icd, qi))
    logging.info(json.dumps(icd2umls))
    return icd2umls


def do_get_concepts_names(concepts, umls, container):
    for c in concepts:
        try:
            details = get_umls_concept_detail(umls, c)
            print '%s\t%s' % (c, details['result']['name'])
            container.append([c, details['result']['name']])
        except:
            print '%s not retrievable %s' % (c, sys.exc_info()[0])


def get_concepts_names(umls, concepts):
    batch_size = 200

    batches = []
    for k in range(0, int(math.ceil(len(concepts)*1.0/batch_size))):
        batches.append(concepts[batch_size * k : batch_size * (k+1)])
        print 'batch %s, len %s' % (k, len(batches[-1]))
    container = []
    utils.multi_thread_tasking(batches, 20, do_get_concepts_names, args=[umls, container])
    print len(container)
    c2label = {}
    for r in container:
        c2label[r[0]] = r[1]
    return c2label


def print_readable_sc2closure(sc2closure, umls):
    s = ''
    for t in sc2closure:
        s += '%s\n' % t
        for c in sc2closure[t]:
            label = 'UNKNOWN'
            obj = get_umls_concept_detail(umls, c)
            if obj is not None:
                label = obj['result']['name']
            s += '\t%s[%s]\n' % (label, c)
            logging.debug('%s read as %s' % (c, label))
    print s


def get_umls():
    return get_umls_client_inst('./resources/HW_UMLS_KEY.txt')


def convert_manual_mapped_to_exact_mapped(manual_mapped_file):
    result = {}
    mm = utils.load_json_data(manual_mapped_file)
    for t in mm:
        for i in xrange(len(mm[t]['concepts'])):
            result['%s (%s)' % (t, i+1)] = {
                'closure': len(mm[t]['concepts']),
                'mapped': mm[t]['concepts'][i]
            }
    print json.dumps(result)


if __name__ == "__main__":
    logging.basicConfig(level='DEBUG')
    # align_mapped_concepts('./resources/autoimmune-concepts.json', './resources/auto_immune_gazetteer.txt')
    umls = get_umls_client_inst('./resources/HW_UMLS_KEY.txt')
    # rets = umls.match_term('type 2 diabetes')
    # cui = rets[0]['ui']
    # print cui
    subconcepts = umls.transitive_narrower('C0038525', None, None)
    print len(subconcepts), json.dumps(subconcepts)
    # next_scs = set([c[0] for c in subconcepts])
    # for sc in subconcepts:
    #     local_scs = umls.get_narrower_concepts(sc[0])
    #     next_scs |= set([c[0] for c in local_scs])
    #     print len(local_scs)
    # print 'total concepts: %s' % len(next_scs), json.dumps(list(next_scs))
    # print json.dumps(umls.get_object('https://uts-ws.nlm.nih.gov/rest/content/current/CUI/C0178298/relations'))
    # print json.dumps(get_umls_concept_detail(umls, 'C0011860'))
    # print json.dumps(get_umls_source_descendants(umls, 'NCI_NICHD', 'C26747'))
    # utils.save_json_array(json.dumps(
    #     get_concepts_names(umls, utils.read_text_file('./resources/all_A00-N99_concepts.txt'))),
    #                       './resources/all_A00-N99_concepts_labels.json')
    # print get_umls_concept_detail(umls, 'C0020538')['result']['name']
    # complete_tsv_concept_label(umls, './studies/IMPARTS/concepts_verified_chris.tsv')
    # icd10_queries()
    # print_readable_sc2closure(utils.load_json_data('./studies/autoimmune.v3.control/sc2closure.json'), umls)
    # icd10_wildcard_queries('./resources/icd10_kch_haematology.tsv')
    # convert_manual_mapped_to_exact_mapped('./studies/autoimmune.v3.control/manual_mapped_concepts.json')