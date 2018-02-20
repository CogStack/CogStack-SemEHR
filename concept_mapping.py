import utils
import json
from umls_api.Authentication import Authentication
import requests
import json


class UMLSAPI(object):
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
        print r.text
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
        objects += obj['result']
        # print 'page count: %s ' % obj['pageCount']
        for i in range(2, obj['pageCount'] + 1):
            objects += self.get_object(content_endpoint, page_number=i)['result']
        return objects

    def get_object(self, uri, page_number=1):
        # print uri
        content = requests.get(uri, params={'ticket': self.get_st(), 'pageNumber': page_number}).content
        # print content
        return json.loads(content)

    def transitive_narrower(self, concept, processed_set=None, result_set=None):
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

        subconcepts = self.get_narrower_concepts(concept)
        subconcepts = [c[0] for c in subconcepts]
        print '%s has %s children' % (concept, len(subconcepts))
        result_set |= set(subconcepts)
        processed_set.add(concept)
        for c in subconcepts:
            self.transitive_narrower(c, processed_set, result_set)
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


def complete_tsv_concept_label(umls, tsv_file):
    lines = []
    for l in utils.read_text_file(tsv_file):
        arr = l.split('\t')
        print arr
        arr.insert(1, get_umls_concept_detail(umls, arr[1])['result']['name'])
        lines.append(arr)
    print '\n'.join(['\t'.join(l) for l in lines])


if __name__ == "__main__":
    # align_mapped_concepts('./resources/autoimmune-concepts.json', './resources/auto_immune_gazetteer.txt')
    umls = UMLSAPI('')
    # rets = umls.match_term('Holter monitor test')
    # cui = rets[0]['ui']
    # print cui
    subconcepts = umls.transitive_narrower('C0178298', None, None)
    print len(subconcepts), json.dumps(subconcepts)
    # next_scs = set([c[0] for c in subconcepts])
    # for sc in subconcepts:
    #     local_scs = umls.get_narrower_concepts(sc[0])
    #     next_scs |= set([c[0] for c in local_scs])
    #     print len(local_scs)
    # print 'total concepts: %s' % len(next_scs), json.dumps(list(next_scs))
    # print json.dumps(umls.get_object('https://uts-ws.nlm.nih.gov/rest/content/current/CUI/C0178298/relations'))
    # print get_umls_concept_detail(umls, 'C0020538')
    # print get_umls_concept_detail(umls, 'C0020538')['result']['name']
    # complete_tsv_concept_label(umls, './studies/IMPARTS/concepts_verified_chris.tsv')