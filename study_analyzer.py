import ontotextapi as onto
import utils
import cohortanalysis as cohort


class StudyAnalyzer(object):

    def __init__(self, terms, name):
        self.terms = terms
        self.study_name = name
        self._concept_closure = None

    def gen_concept_closure(self, term_concepts=None):
        if term_concepts is None:
            term_concepts = {}
            for term in self.terms:
                concept_obj = onto.match_term_to_concept(term)
                if concept_obj is not None:
                    term_concepts[term] = concept_obj['localName']
        self._concept_closure = set()
        for term in term_concepts:
            self._concept_closure.add(term_concepts[term])
            sub_concepts = onto.get_transitive_subconcepts(term_concepts[term])
            self._concept_closure |= set(sub_concepts)

    @property
    def concept_closure(self):
        if self._concept_closure is None:
            self.gen_concept_closure()
        return self._concept_closure

    def gen_study_table(self, cohort_name, out_file):
        cohort.populate_patient_concept_table(cohort_name, self.concept_closure, out_file)

if __name__ == "__main__":
    terms = utils.read_text_file('./studies/autoimmune.v2/Cancer.terms')
    sa = StudyAnalyzer(terms, 'Cancer')
    print sa.concept_closure
    # sa.gen_study_table('valproic acid patients', './studies/autoimmune.v2/Cancer.table')
