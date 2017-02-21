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
        return [(c['relatedId'][c['relatedId'].rfind('/')+1:], c['relationLabel']) for c in self.get_all_objects(content_endpoint)]

    def get_all_objects(self, content_endpoint):
        objects = []
        obj = self.get_object(content_endpoint)
        objects += obj['result']
        print 'page count: %s ' % obj['pageCount']
        for i in range(2, obj['pageCount'] + 1):
            objects += self.get_object(content_endpoint, page_number=i)['result']
        return objects

    def get_object(self, uri, page_number=1):
        print uri
        content = requests.get(uri, params={'ticket': self.get_st(), 'pageNumber': page_number}).content
        print content
        return json.loads(content)


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


if __name__ == "__main__":
    # align_mapped_concepts('./resources/autoimmune-concepts.json', './resources/auto_immune_gazetteer.txt')
    umls = UMLSAPI('148475b7-ad37-4e15-95a0-ff4d4060c132')
    rets = umls.match_term('Diabetes Mellitus')
    cui = rets[0]['ui']
    print cui
    # subconcepts = umls.get_narrower_concepts(cui)
    # print len(subconcepts), json.dumps(subconcepts)
    # print umls.get_object('https://uts-ws.nlm.nih.gov/rest/search/current?string=fracture')
