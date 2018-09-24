import utils
from os.path import isfile, join
from os import listdir
import re
import sqldbutils as dutil
from study_analyzer import StudyAnalyzer, StudyConcept

def populate_concept_level_performance(complete_validation_file, c_map_file):
    if isfile(c_map_file):
        return utils.load_json_data(c_map_file)
    lines = utils.read_text_file(complete_validation_file)
    concept2label = {}
    for l in lines[1:]:
        arr = l.split('\t')
        label = arr[2]
        concept = arr[8]
        c_map = None
        if concept not in concept2label:
            c_map = {}
            concept2label[concept] = c_map
        else:
            c_map = concept2label[concept]
        if label not in c_map:
            c_map[label] = 1
        else:
            c_map[label] += 1
    utils.save_json_array(concept2label, c_map_file)
    return concept2label


def populate_phenotype_validation_results(phenotype_def_file,
                                          complete_validation_file, c_map_file,
                                          output_file):
    c_map = populate_concept_level_performance(complete_validation_file, c_map_file)
    phenotypes = utils.load_json_data(phenotype_def_file)
    for p_name in phenotypes:
        p = phenotypes[p_name]
        p['validation'] = {}
        for c in p['concepts']:
            if c not in c_map:
                continue
            for label in c_map[c]:
                if label in p['validation']:
                    p['validation'][label] += c_map[c][label]
                else:
                    p['validation'][label] = c_map[c][label]
    utils.save_json_array(phenotypes, output_file)
    print 'done'


def do_phenotype_result():
    populate_phenotype_validation_results('./data/phenotype_def_file',
                                          './data/compelete_validat_file',
                                          './data/c_map_file.json', './data/phenotype_def_with_validation.json')


def do_phenotype_analysis(phenotype_result_file, c_map_file, output_folder):
    c_map = utils.load_json_data(c_map_file)
    p_map = utils.load_json_data(phenotype_result_file)
    # extract performances of phenotypes
    headers = ["posM", "hisM", "negM", "otherM", "wrongM"]
    rows = ['\t'.join(["phenotype"] + headers)]
    for p in p_map:
        v = p_map[p]['validation']
        if v is None or len(v) == 0:
            continue
        rows.append('\t'.join([p] + [str(v[h]) if h in v else '0' for h in headers]))
    utils.save_string('\n'.join(rows), join(output_folder, 'phenotype_performance.tsv'))


def add_concept_level_freqs(data_folder, c_map_file):
    reg_p = re.compile(".*annotations\\.csv")
    c_map = utils.load_json_data(c_map_file)
    for f in listdir(data_folder):
        if reg_p is not None:
            m = reg_p.match(f)
            if m is not None:
                print '%s matched, reading...' % f
                lines = utils.read_text_file(join(data_folder, f))
                for l in lines:
                    arr = l.split('\t')
                    if arr[0] not in c_map:
                        continue
                    if 'freq' not in c_map[arr[0]]:
                        c_map[arr[0]]['freq'] = 0
                    c_map[arr[0]]['freq'] += int(arr[1])
    utils.save_json_array(c_map, c_map_file)


def output_phenotypes(phenotype_file, phenotype_performance, c_map_file, output_file):
    p = utils.load_json_data(phenotype_file)
    c_map = utils.load_json_data(c_map_file)
    new_p = {}
    p_lines = utils.read_text_file(phenotype_performance)
    for l in p_lines[1:]:
        arr = l.split('\t')
        new_p[arr[0]] = p[arr[0]]
        pt = new_p[arr[0]]
        concepts = pt['concepts']
        pt['concepts'] = {}
        pt['prevalence'] = 0
        for c in concepts:
            pt['concepts'][c] = 0 if c not in c_map else c_map[c]['freq']
            pt['prevalence'] += pt['concepts'][c]
    utils.save_json_array(new_p, output_file)
    print 'new data saved to %s' % output_file


def phenotype_prevalence(phenotype_with_prev, output_file):
    pd = utils.load_json_data(phenotype_with_prev)
    utils.save_string('\n'.join(['\t'.join([p, str(pd[p]['prevalence']), str(len(pd[p]['concepts']))]) for p in pd]),
                      output_file)


def output_single_phenotype_detail(pprevalence_file, phenotype, output_file):
    pp = utils.load_json_data(pprevalence_file)
    p = pp[phenotype]
    rows = []
    rows.append('\t'.join(['total', str(p['prevalence'])]))
    for sp in p['subtypes']:
        rows.append('\t'.join([sp['phenotype'], str(p['concepts'][sp['concept']])]))
    for c in p['concepts']:
        rows.append('\t'.join([c, str(p['concepts'][c])]))
    utils.save_string('\n'.join(rows), output_file)
    print '% result saved to %s' % (phenotype, output_file)


def patient_level_analysis(complete_anns_file, output_file):
    lines = utils.read_text_file(complete_anns_file)
    pos_condition2patients = {}
    patient2conditions = {}
    positive_labels = ['posM', 'hisM']
    indexable_labels = ['posM', 'hisM', 'negM']
    for l in lines:
        arr = l.split('\t')
        label = arr[2]
        condition = arr[3]
        pid = arr[8]
        if label in positive_labels:
            pos_condition2patients[condition] = [pid] if condition not in pos_condition2patients else \
                pos_condition2patients[condition] + [pid]
        if label in indexable_labels:
            pd = patient2conditions[pid] if pid in patient2conditions else {}
            patient2conditions[pid] = pd
            if label in pd:
                pd[label].append(condition)
                pd[label] = list(set(pd[label]))
            else:
                pd[label] = [condition]
    utils.save_json_array({'p2c': patient2conditions, 'c2p': pos_condition2patients}, output_file)


def increase_freq_on_dict(c_group, c, t, id):
    c_obj = c_group[c] if c in c_group else {}
    c_group[c] = c_obj
    if 'unique_ids' not in c_obj:
        c_obj['unique_ids'] = set()
    unique_ids = c_obj['unique_ids']
    if id in unique_ids:
        return
    unique_ids.add(id)
    c_obj[t] = 1 if t not in c_obj else (c_obj[t] + 1)


def dump_mention_detail(studies_folder, include_study_pattern, dump_tsv_file, dump_concept_file):
    reg_p = re.compile(include_study_pattern)
    rows = ['\t'.join(['concept', 'pt', 'doc', 's', 'e', 'label', 'ruled'])]
    c_group = {}
    for f in listdir(studies_folder):
        m = reg_p.match(f)
        if m is not None:
            ruled_file = join(studies_folder, f, 'ruled_anns.json')
            if isfile(ruled_file):
                # {"p": "pid", "s": 3356, "e": 3365, "d": "did", "case-instance": [xxx"],
                # "c": "C0000833", "string_orig": "abscesses",
                # "ruled": "semehr hypothetical_filters.json"}
                ruleds = utils.load_json_data(ruled_file)
                for r in ruleds:
                    rows.append('\t'.join([r['c'], r['p'], r['d'], str(r['s']), str(r['e']), r['string_orig'], r['ruled']]))
                    increase_freq_on_dict(c_group, r['c'], r['ruled'], '-'.join([r['d'], str(r['s']), str(r['e'])]))
            pos_file = join(studies_folder, f, 'result.csv_json')
            if isfile(pos_file):
                # {"c": "C0000833", "e": 467, "d": "52773120", "string_orig": "abscess", "p": "10110421", "s": 460}
                poses = utils.load_json_data(pos_file)
                for r in poses:
                    rows.append('\t'.join([r['c'], r['p'], r['d'], str(r['s']), str(r['e']), r['string_orig'], '']))
                    increase_freq_on_dict(c_group, r['c'], 'pos', '-'.join([r['d'], str(r['s']), str(r['e'])]))

    rule_headers = ['semehr negation_filters.json',
                    'semehr hypothetical_filters.json',
                    'semehr not_mention_filters.json',
                    'semehr other_experiencer_filters.json',
                    'semehr cris_document_filters.json',
                    'skip-term',
                    'semehr s_skin.json',
                    'semehr s_karen.json',
                    'yodie',
                    'pos']
    c_rows = ['\t'.join(['concept'] + rule_headers)]
    for c in c_group:
        co = c_group[c]
        c_rows.append('\t'.join([c] + [str(co[h]) if h in co else '0' for h in rule_headers]))
    utils.save_string('\n'.join(rows), dump_tsv_file)
    utils.save_string('\n'.join(c_rows), dump_concept_file)
    print 'dumped to  %s' % dump_tsv_file


def phenotype_counting(phenotype_def, concept_level_results, output_file):
    pd = utils.load_json_data(phenotype_def)
    npd = {}
    cd = utils.read_text_file(concept_level_results)
    c_headers = cd[0].split('\t')
    headers = [h for h in c_headers[2:len(c_headers) - 1]]
    for r in cd[1:]:
        arr = r.split('\t')
        c = arr[0]
        num_mention = arr[12]
        for p in pd:
            if c in pd[p]['concepts']:
                po = npd[p] if p in npd else {'freq':0, 'p': p,
                                              'num_concepts': len(pd[p]['concepts'])}
                npd[p] = po
                po['freq'] += int(num_mention)
                for idx in xrange(2, len(arr) - 1):
                    h = headers[idx-2]
                    po[h] = int(arr[idx]) if h not in po else (int(arr[idx]) + int(po[h]))

    rows = ['\t'.join(['phenotype', 'num concepts'] + headers + ['prevalence'])]
    for p in npd:
        po = npd[p]
        rows.append('\t'.join([p, str(po['num_concepts'])] + [str(po[h]) for h in headers] + [str(po['freq'])]))
    utils.save_string('\n'.join(rows), output_file)


def load_phenotype_def_into_db():
    db_cnf = '../../studies/COMOB_SD/dbcnn_input.json'
    p_def_file = './data/phenotype_defs.json'
    pd = utils.load_json_data(p_def_file)
    w_sql = """
    insert into tp_phenotype_concepts (phenotype_id, concept_id) values 
    ('{pid}', '{cid}');
    """
    r_sql = """
    select * from tp_phenotypes
    """
    p_data = []
    dutil.query_data(r_sql, p_data, dutil.get_db_connection_by_setting(db_cnf))
    p2pid = {}
    for r in p_data:
        p2pid[r['phenotype_name']] = r['id']
    for p in pd:
        if p not in p2pid:
            print '%s not found in definition table' % p
            continue
        for c in pd[p]['concepts']:
            sql = w_sql.format(**{'pid': p2pid[p], 'cid': c})
            print 'executing [%s]' % sql
            dutil.query_data(sql, None, dbconn=dutil.get_db_connection_by_setting(db_cnf))
    print 'done'


def merge_study_concepts(studies_folder, include_study_pattern, merged_sa_name, merged_output_folder):
    reg_p = re.compile(include_study_pattern)
    merged_sa = StudyAnalyzer(merged_sa_name)
    for f in listdir(studies_folder):
        m = reg_p.match(f)
        if m is not None:
            sa_file = join(studies_folder, f, 'study_analyzer.pickle')
            print 'picking %s...' % f
            if isfile(sa_file):
                sa = StudyAnalyzer.deserialise(sa_file)
                for sc in sa.study_concepts:
                    merged_sa.add_concept(sc)
    study_concept_list = []
    for c in merged_sa.study_concepts:
        print '%s - %s' % (c.name, len(c.concept_closure))
        study_concept_list += list(c.concept_closure)
    print 'all concept len is %s' % (len(study_concept_list))
    utils.save_string('\n'.join(study_concept_list), join(merged_output_folder, 'all_concepts.txt'))
    merged_sa.serialise(join(merged_output_folder, 'merged_study_analyzer.pickle'))


def label_analyse(sql_template_file, db_cnf, output_file=None):
    sql_temps = utils.load_json_data(sql_template_file)
    concepts = []
    dutil.query_data(sql_temps['get_validated_concepts'], concepts,
                     dbconn=dutil.get_db_connection_by_setting(db_cnf))
    s = ''
    for c in concepts:
        data, output = concept_analyse(c['concept_id'], sql_temps['condition_label_sql'], sql_temps['wrong_label_sql'], db_cnf)
        s += output
    if output_file is not None:
        print 'saving output to %s...' % output_file
        utils.save_string(output, output_file)


def concept_analyse(concept_id, condition_label_sql, wrong_label_sql, db_cnf):
    # get condition mention labels
    concept_result = {'concept': concept_id, 'labels': {}}
    mc = MConcept(concept_id)
    results_condition_labels = []
    dutil.query_data(condition_label_sql.format(**{'concept': concept_id}), results_condition_labels,
                     dbconn=dutil.get_db_connection_by_setting(db_cnf))
    for r in results_condition_labels:
        if r['label'] not in mc.name2labels:
            mc.add_label(ConceptLabel(r['label']))
        mc.name2labels[r['label']].condition_mention = r['num']

    results_wrong_labels = []
    dutil.query_data(wrong_label_sql.format(**{'concept': concept_id}), results_wrong_labels,
                     dbconn=dutil.get_db_connection_by_setting(db_cnf))
    for r in results_wrong_labels:
        if r['label'] not in mc.name2labels:
            mc.add_label(ConceptLabel(r['label']))
        mc.name2labels[r['label']].wrong_mention = r['num']

    output = mc.output()
    print output
    return concept_result, output


class ConceptLabel(object):
    """
    concept label with frequencies
    implements ambiguity scoring
    """
    def __init__(self, label):
        self._label = label
        self._condition_freq = 0
        self._wrong_freq = 0

    @property
    def label(self):
        return self._label

    @property
    def condition_mention(self):
        return self._condition_freq

    @condition_mention.setter
    def condition_mention(self, value):
        self._condition_freq = value

    @property
    def wrong_mention(self):
        return self._wrong_freq

    @wrong_mention.setter
    def wrong_mention(self, value):
        self._wrong_freq = value

    @property
    def total_mentions(self):
        return self.condition_mention + self.wrong_mention

    @property
    def ambiguity_score(self):
        return self.wrong_mention * 1.0 / (self.wrong_mention + self.condition_mention)


class MConcept(object):
    def __init__(self, concept_id):
        self._concept_id = concept_id
        self._l2labels = {}
        self._total_freq = -1

    @property
    def total_freq(self):
        if self._total_freq == -1:
            self._total_freq = 0
            for l in self.labels:
                self._total_freq += l.total_mentions
        return self._total_freq

    @property
    def labels(self):
        return [self._l2labels[l] for l in self._l2labels]

    @property
    def name2labels(self):
        return self._l2labels

    def add_label(self, cl):
        self._l2labels[cl.label] = cl

    @property
    def ambiguity_score(self):
        s = 0
        total_freq = 0
        for l in self.name2labels:
            lb = self.name2labels[l]
            s += lb.ambiguity_score * (lb.condition_mention + lb.wrong_mention)
            total_freq += lb.condition_mention + lb.wrong_mention
        return s * 1.0 / total_freq

    def label_variation(self, k=2):
        c_sorted = sorted([l for l in self.labels], key=lambda x: -x.condition_mention)
        k_freq = 0
        t_freq = 0
        for i in xrange(len(c_sorted)):
            if i + 1 <= k:
                k_freq += c_sorted[i-1].condition_mention
            t_freq += c_sorted[i-1].condition_mention
        if t_freq == 0:
            return -1
        return 1 - k_freq * 1.0 / t_freq

    def ambiguity_contributions(self):
        l2c = {}
        for l in self.labels:
            if self.ambiguity_score == 0:
                l2c[l.label] = -1
            else:
                l2c[l.label] = l.ambiguity_score * l.total_mentions / self.total_freq / self.ambiguity_score
        return l2c

    def condition_contributions(self):
        l2c = {}
        for l in self.labels:
            if self.ambiguity_score == 1:
                l2c[l.label] = -1
            else:
                l2c[l.label] = (1 - l.ambiguity_score) * l.total_mentions / self.total_freq / (1 - self.ambiguity_score)
        return l2c

    def output(self):
        mc = self
        labels = sorted([l for l in mc.labels], key=lambda x: - x.total_mentions)
        s = '%s (ambiguity: %s; name variation@2: %s)\n' \
            % (self._concept_id, mc.ambiguity_score, mc.label_variation())
        s += 'label\tambiguity score\tcondition mention/wrong mention\tamb contri\tcond contri\n'
        amb_contris = mc.ambiguity_contributions()
        cond_contis = mc.condition_contributions()
        for l in labels:
            s += '%s\t%s\t%s/%s\t%s\t%s\n' % (l.label, l.ambiguity_score,
                                             l.condition_mention, l.wrong_mention,
                                             amb_contris[l.label], cond_contis[l.label])
        s += '\n' + ('-' * 30) + '\n'
        return s


if __name__ == "__main__":
    # do_phenotype_analysis('./data/phenotype_def_with_validation.json', './data/c_map_file.json', './data/pstats/')
    # add_concept_level_freqs('./data/', './data/c_map_file.json')
    # output_phenotypes('./data/phenotype_def_with_validation.json',
    #                   './data/pstats/phenotype_performance.tsv',
    #                   './data/c_map_file.json',
    #                   './data/phenotype_with_prevlence.json')
    # phenotype_prevalence('./data/phenotype_with_prevlence.json', './data/pprevalence.tsv')
    # output_single_phenotype_detail('./data/phenotype_with_prevlence.json', 'Cerebrovascular Disease', './data/Cerebrovascular_Disease.tsv')
    # dump_mention_detail('../../studies', r'skin.*|COMOB.*|karen.*',
    #                     './data/mention_dumps.tsv',
    #                     './data/concept_typed_dumps.tsv')
    # phenotype_counting('./data/phenotype_defs.json', './data/concept_level_results.tsv',
    #                    './data/phenotype_prev.tsv')
    # load_phenotype_def_into_db()
    # patient_level_analysis('/Users/honghan.wu/Documents/UoE/working_papers/text2phenome/completed_anns.tsv',
    #                        '/Users/honghan.wu/Documents/UoE/working_papers/text2phenome/condition_patient_dicts.json')
    dump_mention_detail('../../studies/physical_conditions/', r'skin.*|COMOB_SD.*|karen.*',
                        './data/mention_dumps_082018.tsv',
                        './data/concept_typed_dumps_082018.tsv')
    # phenotype_counting('./data/phenotype_defs.json', './data/concept_level_results.tsv',
    #                    './data/phenotype_prev.tsv')
    # merge_study_concepts('../../studies', r'skin.*|COMOB\_SD.*|karen.*|raquel.*|autoimmune|HCVpos', 'physical phenotypes', './data/')

