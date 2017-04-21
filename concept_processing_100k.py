from concept_mapping import UMLSAPI
import utils

_umls_api_key = '148475b7-ad37-4e15-95a0-ff4d4060c132'


def do_compute_subconcept(c, umls, container):
    try:
        subconcepts = umls.get_narrower_concepts(c)
        container.append((c, subconcepts))
    except ValueError:
        print 'no subconcepts found for %s' % c
        pass


def compute_all_subconcepts(concepts, file_path):
    c_to_subs = {}
    umls = UMLSAPI(_umls_api_key)
    container = []
    utils.multi_thread_tasking(concepts, 10, do_compute_subconcept, args=[umls, container])
    for p in container:
        c_to_subs[p[0]] = p[1]
    utils.save_json_array(c_to_subs, file_path)


def process_concepts():
    hpo_file = './resources/100k_hpo.json'
    sub_concept_file = './resources/100k_subconcepts.json'
    hpos = utils.load_json_data(hpo_file)
    concepts = set()
    for h in hpos:
        for c in hpos[h]:
            c = c.replace('UMLS:', '')
            concepts.add(c)
    print 'total concepts %s' % len(concepts)
    compute_all_subconcepts(list(concepts), sub_concept_file)


if __name__ == "__main__":
    process_concepts()
