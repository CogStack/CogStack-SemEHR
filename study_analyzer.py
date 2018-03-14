import ontotextapi as onto
import utils
import json
from os.path import isfile, join, split
import joblib as jl
import cohortanalysis as cohort
from ann_post_rules import AnnRuleExecutor
import sys
import xml.etree.ElementTree as ET
import concept_mapping


class StudyConcept(object):

    def __init__(self, name, terms, umls_instance=None):
        self.terms = terms
        self._name = name
        self._term_to_concept = None
        self._concept_closure = None
        self._umls_instance = umls_instance

    def gen_concept_closure(self, term_concepts=None, concept_to_closure=None):
        """
        generate concept closures for all terms
        :param term_concepts: optional - expert verified mappings can be used
        :param concept_to_closure: precomputed concept to closure dictionary
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
                if concept_to_closure is not None:
                    candidate_terms.append((concept, concept_to_closure[concept]))
                else:
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

    @staticmethod
    def compute_all_concept_closure(all_concepts, umls_instance):
        concept_to_closure = {}
        print 'all concepts number %s' % len(all_concepts)
        computed = []
        results =[]
        utils.multi_thread_tasking(all_concepts, 40, StudyConcept.do_compute_concept_closure,
                                   args=[umls_instance, computed, results])
        for r in results:
            concept_to_closure[r['concept']] = r['closure']
        return concept_to_closure

    @staticmethod
    def do_compute_concept_closure(concept, umls_instance, computed, results):
        if concept not in computed:
            closure = umls_instance.transitive_narrower(concept)
            computed.append(concept)
            results.append({'concept': concept, 'closure': closure})
            print 'concept: %s transitive children %s' % (concept, closure)

    @property
    def name(self):
        return self._name

    @property
    def concept_closure(self):
        if self._concept_closure is None:
            self.gen_concept_closure()
        return self._concept_closure

    @concept_closure.setter
    def concept_closure(self, value):
        self._concept_closure = value

    @property
    def term_to_concept(self):
        if self._concept_closure is None:
            self.gen_concept_closure()
        return self._term_to_concept

    @term_to_concept.setter
    def term_to_concept(self, value):
        self._term_to_concept = value


class StudyAnalyzer(object):

    def __init__(self, name):
        self._study_name = name
        self._study_concepts = []
        self._skip_terms = []
        self._options = None

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

    @property
    def skip_terms(self):
        return self._skip_terms

    @skip_terms.setter
    def skip_terms(self, value):
        self._skip_terms = value

    def add_concept(self, concept):
        self.study_concepts.append(concept)

    def generate_exclusive_concepts(self):
        """
        it is important to have a set of disjoint concepts otherwise concept-document frequencies would
        contain double-counted results
        :return:
        """
        # call the concept closure property to make sure
        # that the closure has been generated before
        # compute the disjoint
        for sc in self.study_concepts:
            cc = sc.concept_closure
        intersections = {}
        explain_inter = {}
        for i in range(1, len(self.study_concepts)):
            for j in xrange(i):
                common = self.study_concepts[i].concept_closure & self.study_concepts[j].concept_closure
                if len(common) > 0:
                    intersections[self.study_concepts[i].name + ' - ' + self.study_concepts[j].name] = common
                    self.study_concepts[j].concept_closure -= common
                    explain_inter[self.study_concepts[j].name] = \
                        ['removed %s common (%s) concepts' % (len(common), self.study_concepts[i].name)] \
                            if self.study_concepts[j].name not in explain_inter \
                            else explain_inter[self.study_concepts[j].name] + \
                                 ['removed %s common (%s) concepts' % (len(common), self.study_concepts[i].name)]
        if len(intersections) > 0:
            print 'intersections [[\n%s\n]]' % json.dumps(explain_inter)
        for sc in self.study_concepts:
            print '%s %s' % (sc.name, len(sc.concept_closure))

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

    @property
    def study_options(self):
        return self._options

    @study_options.setter
    def study_options(self, value):
        self._options = value

    @staticmethod
    def deserialise(ser_file):
        return jl.load(ser_file)

    def gen_study_table(self, cohort_name, out_file):
        cohort.populate_patient_study_table(cohort_name, self, out_file)

    def gen_sample_docs(self, cohort_name, out_file):
        cohort.random_extract_annotated_docs(cohort_name, self, out_file, 10)

    def gen_study_table_with_rules(self, cohort_name, out_file, sample_out_file, ruler, ruled_out_file,
                                   sql_config, db_conn_file, text_preprocessing=False):
        sql_setting = get_sql_template(sql_config)
        cohort.populate_patient_study_table_post_ruled(cohort_name, self, out_file, ruler, 20,
                                                       sample_out_file, ruled_out_file,
                                                       sql_setting['patients_sql'], sql_setting['term_doc_anns_sql'],
                                                       sql_setting['skip_term_sql'],
                                                       db_conn_file, text_preprocessing=text_preprocessing)

    def gen_study_table_in_one_iteration(self, cohort_name, out_file, sample_out_file,
                                         sql_config, db_conn_file):
        sql_setting = get_one_iteration_sql_template(sql_config)
        cohort.generate_result_in_one_iteration(cohort_name, self, out_file, 20, sample_out_file,
                                                sql_setting['doc_to_brc_sql'],
                                                sql_setting['brc_sql'],
                                                sql_setting['anns_iter_sql'],
                                                sql_setting['skip_term_sql'],
                                                sql_setting['doc_content_sql'],
                                                db_conn_file)


def get_sql_template(config_file):
    root = ET.parse(config_file).getroot()
    return {'term_doc_anns_sql': root.find('term_doc_anns_sql').text,
            'patients_sql': root.find('patients_sql').text,
            'skip_term_sql': root.find('skip_term_sql').text}


def get_one_iteration_sql_template(config_file):
    root = ET.parse(config_file).getroot()
    return {'doc_to_brc_sql': root.find('doc_to_brc_sql').text,
            'brc_sql': root.find('brc_sql').text,
            'anns_iter_sql': root.find('anns_iter_sql').text,
            'doc_content_sql': root.find('doc_content_sql').text,
            'skip_term_sql': root.find('skip_term_sql').text}


def study(folder, cohort_name, sql_config_file, db_conn_file, umls_instance, do_one_iter=False, do_preprocessing=False):
    p, fn = split(folder)
    if isfile(join(folder, 'study_analyzer.pickle')):
        sa = StudyAnalyzer.deserialise(join(folder, 'study_analyzer.pickle'))
    else:
        sa = StudyAnalyzer(fn)
        if isfile(join(folder, 'exact_concepts_mappings.json')):
            concept_mappings = utils.load_json_data(join(folder, 'exact_concepts_mappings.json'))
            concept_to_closure = \
                StudyConcept.compute_all_concept_closure([concept_mappings[t] for t in concept_mappings],
                                                         umls_instance)

            scs = []
            for t in concept_mappings:
                sc = StudyConcept(t, [t])
                t_c = {}
                t_c[t] = [concept_mappings[t]]
                sc.gen_concept_closure(term_concepts=t_c, concept_to_closure=concept_to_closure)
                scs.append(sc)
                print sc.concept_closure
            sa.study_concepts = scs
            sa.serialise(join(folder, 'study_analyzer.pickle'))
        elif isfile(join(folder, 'manual_mapped_concepts.json')):
            mapped_scs = utils.load_json_data(join(folder, 'manual_mapped_concepts.json'))
            scs = []
            for t in mapped_scs:
                sc = StudyConcept(t, [t])
                sc.concept_closure = set(mapped_scs[t]['concepts'])
                tc = {}
                tc[t] = mapped_scs[t]['tc']
                sc.term_to_concept = tc
                scs.append(sc)
                print sc.term_to_concept, sc.concept_closure
            sa.study_concepts = scs
            print scs
            exit(0)
        else:
            concepts = utils.load_json_data(join(folder, 'study_concepts.json'))
            if len(concepts) > 0:
                scs = []
                for name in concepts:
                    scs.append(StudyConcept(name, concepts[name], umls_instance=umls_instance))
                    print name, concepts[name]
            sa.study_concepts = scs
            sa.serialise(join(folder, 'study_analyzer.pickle'))

    # compute disjoint concepts
    sa.generate_exclusive_concepts()

    if isfile(join(folder, 'skip_terms.json')):
        sa.skip_terms = utils.load_json_data(join(folder, 'skip_terms.json'))
    if isfile(join(folder, 'study_options.json')):
        sa.study_options = utils.load_json_data(join(folder, 'study_options.json'))
    merged_mappings = {}
    study_concept_list = []
    for c in sa.study_concepts:
        for t in c.term_to_concept:
            all_concepts = list(c.concept_closure)
            study_concept_list += all_concepts
            if len(all_concepts) > 1:
                idx = 0
                for cid in all_concepts:
                    merged_mappings['(%s) %s (%s)' % (c.name, t, idx)] = {'closure': len(all_concepts), 'mapped': cid}
                    idx += 1
            else:
                merged_mappings['(%s) %s' % (c.name, t)] = c.term_to_concept[t]
        # print c.name, c.term_to_concept, c.concept_closure
        # print json.dumps(list(c.concept_closure))
    print 'print merged mappings...'
    print json.dumps(merged_mappings)
    print len(study_concept_list)
    utils.save_string('\n'.join(study_concept_list), join(folder, 'all_concepts.txt'))
    print 'generating result table...'
    # sa.gen_study_table(cohort_name, join(folder, 'result.csv'))
    # sa.gen_sample_docs(cohort_name, join(folder, 'sample_docs.json'))
    ruler = AnnRuleExecutor()
    rules = utils.load_json_data(join(folder, 'post_filter_rules.json'))
    for r in rules:
        ruler.add_filter_rule(r['offset'], r['regs'])
    if do_one_iter:
        sa.gen_study_table_in_one_iteration(cohort_name, join(folder, 'result.csv'), join(folder, 'sample_docs.json'),
                                            sql_config_file, db_conn_file)
    else:
        sa.gen_study_table_with_rules(cohort_name, join(folder, 'result.csv'), join(folder, 'sample_docs.json'), ruler,
                                      join(folder, 'ruled_anns.json'), sql_config_file, db_conn_file,
                                      text_preprocessing=do_preprocessing)
    print 'done'


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('cp1252')
    # study('./studies/slam_physical_health/', 'CC_physical_health')
    # study('./studies/autoimmune.v2/', 'auto_immune',
    #       './index_settings/query_config_cam.xml',
    #       './index_settings/cam_dbconn.json'
    #       )
    # study('./studies/autoimmune', 'auto_immune',
    #       './index_settings/query_config_cam.xml',
    #       './index_settings/cam_dbconn.json')
    # study('./studies/HCVpos', 'HCVpos_cohort')
    # study('./studies/liver', 'auto_immune')
    # study('./studies/hepc_unknown_200', 'hepc_unknown')
    # study('./studies/karen', 'karen_2017')
    # study('./studies/skin_conditions/', 'addiction',
    #       './studies/skin_conditions/cluster_sql_config.xml',
    #       './studies/skin_conditions/dbcnn_input.json',
    #       concept_mapping.get_umls_client_inst('./resources/HW_UMLS_KEY.txt')
    #       )
    #study('./studies/COMOB_SD/', 'dyson',
    #      './studies/COMOB_SD/cluster_sql_config.xml',
    #      './studies/COMOB_SD/dbcnn_input.json',
    #      concept_mapping.get_umls_client_inst('./resources/HW_UMLS_KEY.txt')
    #      )
    # study('./studies/prathiv/', 'pirathiv',
    #       './studies/prathiv/one_iter_sql_config.xml',
    #       './studies/prathiv/dbcnn_input.json',
    #       concept_mapping.get_umls_client_inst('./resources/HW_UMLS_KEY.txt'),
    #       do_one_iter=True
    #       )
    study('./studies/emma_violence/', 'emma_violence',
          './studies/emma_violence/one_iter_sql_config.xml',
          './studies/emma_violence/dbcnn_input.json',
          concept_mapping.get_umls_client_inst('./resources/HW_UMLS_KEY.txt')
          )
    # study('./studies/raquel_cardic/', 'raquel_cardic',
    #       './studies/raquel_cardic/one_iter_sql_config.xml',
    #       './studies/raquel_cardic/dbcnn_input.json',
    #       concept_mapping.get_umls_client_inst('./resources/HW_UMLS_KEY.txt')
    #       )

