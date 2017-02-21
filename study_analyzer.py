import ontotextapi as onto
import utils
import json
from os.path import isfile, join, split
import joblib as jl
import cohortanalysis as cohort


class StudyConcept(object):

    def __init__(self, name, terms):
        self.terms = terms
        self._name = name
        self._term_to_concept = None
        self._concept_closure = None

    def gen_concept_closure(self, term_concepts=None):
        """
        generate concept closures for all terms
        :param term_concepts: optional - expert verified mappings can be used
        :return:
        """
        self._term_to_concept = {}
        self._concept_closure = set()
        if term_concepts is None:
            term_concepts = {}
            for term in self.terms:
                concept_objs = onto.match_term_to_concept(term if not term.startswith("~~") else term[2:])
                if concept_objs is not None:
                    term_concepts[term] = [o['localName'] for o in concept_objs]

        for term in term_concepts:
            candidate_terms = []
            for concept in term_concepts[term]:
                candidate_terms.append((concept, onto.get_transitive_subconcepts(concept)))

            # pick the rich sub-concept mappings
            if len(candidate_terms) > 1:
                candidate_terms = sorted(candidate_terms, key=lambda x: -len(x[1]))
            if term.startswith('~~'):
                to_remove = set(candidate_terms[0][1])
                to_remove.add(candidate_terms[0][0])
                self._concept_closure -= to_remove
                print 'removed %s items' % len(to_remove)
            else:
                self._concept_closure.add(candidate_terms[0][0])
                self._concept_closure |= set(candidate_terms[0][1])
            self._term_to_concept[term] = {'mapped': candidate_terms[0][0], 'closure': len(candidate_terms[0][1])}

    @property
    def name(self):
        return self._name

    @property
    def concept_closure(self):
        if self._concept_closure is None:
            self.gen_concept_closure()
        return self._concept_closure

    @property
    def term_to_concept(self):
        if self._concept_closure is None:
            self.gen_concept_closure()
        return self._term_to_concept


class StudyAnalyzer(object):

    def __init__(self, name):
        self._study_name = name
        self._study_concepts = []

    @property
    def study_name(self):
        return self._study_name

    @study_name.setter
    def study_name(self, value):
        self._study_name = value

    @property
    def study_concepts(self):
        return self._study_concepts

    @study_concepts.setter
    def study_concepts(self, value):
        self._study_concepts = value

    def add_concept(self, concept):
        self.study_concepts.append(concept)

    def export_mapping_in_json(self):
        mapping = {}
        for c in self._study_concepts:
            mapping[c.name] = c.term_to_concept

    def serialise(self, out_file):
        print 'iterating concepts to populate the mappings'
        for c in self._study_concepts:
            tc = c.term_to_concept
        print 'saving...'
        jl.dump(self, out_file)
        print 'serialised to %s' % out_file

    @staticmethod
    def deserialise(ser_file):
        return jl.load(ser_file)

    def gen_study_table(self, cohort_name, out_file):
        cohort.populate_patient_study_table(cohort_name, self, out_file)


def study(folder, cohort_name):
    p, fn = split(folder)
    if isfile(join(folder, 'study_analyzer.pickle')):
        sa = StudyAnalyzer.deserialise(join(folder, 'study_analyzer.pickle'))
    else:
        concepts = utils.load_json_data(join(folder, 'study_concepts.json'))
        if len(concepts) > 0:
            scs = []
            for name in concepts:
                scs.append(StudyConcept(name, concepts[name]))
                print name, concepts[name]
        sa = StudyAnalyzer(fn)
        sa.study_concepts = scs
        sa.serialise(join(folder, 'study_analyzer.pickle'))

    merged_mappings = {}
    for c in sa.study_concepts:
        for t in c.term_to_concept:
            merged_mappings['(%s) %s' % (c.name, t)] = c.term_to_concept[t]
        print c.name, c.concept_closure
    print json.dumps(merged_mappings)
    print 'generating result table...'
    sa.gen_study_table(cohort_name, join(folder, 'result.csv'))
    print 'done'

if __name__ == "__main__":
    study('./studies/slam_physical_health/', 'CC_physical_health')
