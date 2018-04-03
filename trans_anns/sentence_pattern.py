import joblib as jl
import text_generaliser as tg
import json
import spacy
import utils


_STOP_POS_TAGS = ['SPACE', 'VERB', 'PUNCT', 'CCONJ', 'ADP']
_en_model = None
_acc_high_threshold = .75
_acc_low_threshold = .25

class SentencePatternInst(object):
    def __init__(self):
        pass

    @property
    def sentence(self):
        return self._sentence

    @sentence.setter
    def sentence(self, value):
        self._sentence = value

    @property
    def pattern(self):
        return self._pattern


class CorpusAnalyzer(object):
    """
    A corpus analyzer - a container of annotated pattern instances that provides corpus level analytic functions
    """
    def __init__(self):
        self._pattern_to_inst = {}
        self._mini_comp_dict = None

    def add_pattern(self, pattern_inst):
        p = pattern_inst.pattern
        if p in self._pattern_to_inst:
            p_insts = self._pattern_to_inst[p]
        else:
            p_insts = []
            self._pattern_to_inst[p] = p_insts
        p_insts.append(pattern_inst)
        # p_insts.append({'sentence': pattern_inst.sentence, 'annotations': pattern_inst.annotations})

    @property
    def pattern_to_insts(self):
        return self._pattern_to_inst

    def show(self):
        arr = [(p, len(self._pattern_to_inst[p])) for p in self._pattern_to_inst]
        arr = sorted(arr, cmp=lambda x, y: y[1] - x[1])
        print 'total num patterns %s' % len(arr)
        for pt in arr[:10]:
            print '(Freq: %s) %s\n[%s]\n\n' % (pt[1], pt[0], self._pattern_to_inst[pt[0]][0].sentence)

    def get_simple_patterns(self, lenghth=5):
        return [p for p in self._pattern_to_inst if len(p.split('->')) <= lenghth]

    def test_pattern(self, pattern):
        for ptn in self.pattern_to_insts:
            if ptn.find(pattern) >= 0:
                self._pattern_to_inst[ptn][0].get_minimal_component()

    def merge_by_mini_comp(self):
        """
        generate a dictionary from minimal components to pattern instances.
        :return:
        """
        mini_ptns = {}
        for ptn in self.pattern_to_insts:
            for inst in self.pattern_to_insts[ptn]:
                m_ptn, text_comps = inst.get_minimal_component()
                if m_ptn is not None and m_ptn.find('KCL') < 0:
                    print m_ptn, inst.sentence
                arr = mini_ptns[m_ptn] if m_ptn in mini_ptns else []
                arr.append(inst)
                mini_ptns[m_ptn] = arr
        CorpusAnalyzer.print_dictionary_anns(mini_ptns)
        return mini_ptns

    def show_mini_comp_patterns(self):
        if self._mini_comp_dict is None:
            print 'minimal component dictionary is not available yet'
        CorpusAnalyzer.print_dictionary_anns(self._mini_comp_dict)

    def get_mini_comp_pattern_by_freq(self, freq=5):
        mini_comp = self.get_mini_comp_dict()
        freq_ptns = []
        for c in mini_comp:
            if len(mini_comp[c]) > freq:
                freq_ptns.append((c, len(mini_comp[c])))
        return freq_ptns

    def analyse_mini_comp_pattern(self, mini_ptn, variant_idxs):
        """
        analyse minimal component pattern instances to figure out
        which ones are easy and which are hard
        :param mini_ptn:
        :param variant_idxs:
        :return:
        """
        if self._mini_comp_dict is None:
            print 'minimal component dictionary is not available yet'
        var_dict = self.get_mini_pattern_var_dict(mini_ptn, variant_idxs)
        variant_insts = var_dict['dict']
        all_accuracy = var_dict['accuracy']
        print 'studying pattern [%s] that has %s variants covering %s instances' % (mini_ptn, len(variant_insts),
                                                                                    len(self._mini_comp_dict[mini_ptn]))
        # CorpusAnalyzer.print_dictionary_anns(variant_insts)

        # 1. clustering instances
        # - using frequent instances as basic clusters;
        freq_threshold = 5
        freq_patterns = []
        infreq_patterns = []
        for ptn in variant_insts:
            freq_patterns.append(ptn) if len(variant_insts[ptn]) >= freq_threshold else infreq_patterns.append(ptn)
        #
        # # word2vec similarities seem not very good at linking clinical related concepts
        # # UMLS semantic type might play a better role here?
        # nlp = CorpusAnalyzer.load_en_model()
        # freq_docs = []
        # for fp in freq_patterns:
        #     freq_docs.append(nlp(fp))
        # print freq_patterns
        # for ptn in infreq_patterns:
        #     d1 = nlp(ptn)
        #     sims = []
        #     i = 0
        #     for f_doc in freq_docs:
        #         sims.append((i, d1.similarity(f_doc)))
        #         i += 1
        #     sims = sorted(sims, lambda x1, x2: 1 if x2[1] - x1[1] >= 0 else -1)
        #     if sims[0][1] > .8:
        #         print '%s -> %s, %s' % (ptn, freq_patterns[sims[0][0]], sims[0][1])

        # 2. organise significant clusters by performances
        disc_ret = CorpusAnalyzer.discriminate_patterns(freq_patterns, variant_insts)
        easy_correct = disc_ret['easy_accurate']
        easy_incorrect = disc_ret['easy_inaccurate']
        difficulties = disc_ret['hard']

        print 'easy correct ones \n%s ' % easy_correct
        print 'easy incorrect ones \n%s ' % easy_incorrect
        print 'hard ones ones \n%s ' % difficulties
        print 'infrequent variants [%s] - %s' % (len(infreq_patterns), infreq_patterns)
        print '---------------\n'

        # 3. hard ones need further sentence level information
        return {'mini_comp_pattern': mini_ptn,
                'all_accuracy': all_accuracy,
                'instance_number': len(self._mini_comp_dict[mini_ptn]),
                'variation_pos': variant_idxs,
                'infrequent_variants': infreq_patterns,
                'easy_accurate': easy_correct,
                'easy_inaccurate': easy_incorrect,
                'hard': difficulties}

    @staticmethod
    def get_mini_pattern_inst_text(comp_texts, variant_idxs):
        key_arr = []
        if len(variant_idxs) > 0:
            prev_idx = variant_idxs[0]
            for idx in variant_idxs:
                if idx >= len(comp_texts):
                    break
                if idx > prev_idx + 1:
                    key_arr += ['=#=']
                key_arr += comp_texts[idx]
                prev_idx = idx
        key = ' '.join(key_arr)
        return key

    def get_mini_pattern_var_dict(self, mini_ptn, variant_idxs):
        if self._mini_comp_dict is None:
            print 'minimal component dictionary is not available yet'
        variant_insts = {}
        num_correct = 0
        num_incorrect = 0

        for inst in self._mini_comp_dict[mini_ptn]:
            ptn, comp_texts = inst.get_minimal_component()
            key = CorpusAnalyzer.get_mini_pattern_inst_text(comp_texts, variant_idxs)
            arr = variant_insts[key] if key in variant_insts else []
            arr.append(inst)
            variant_insts[key] = arr
            if inst.annotations[0]['gt_label'] != '-':
                if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label']:
                    num_correct += 1
                else:
                    num_incorrect += 1
        all_accuracy = num_correct * 1.0 / (num_correct + num_incorrect) if num_correct + num_incorrect > 0 else '-'
        return {'dict': variant_insts, 'accuracy': all_accuracy}

    @staticmethod
    def get_sent_pattern_key(ptns):
        # ptn_str = '<-'.join([p[0] for p in ptns])
        arr = []
        for p in ptns:
            if len(arr) > 0 and arr[-1] == u'VERB':
                break
            arr.append(p[0])
        return '<-'.join(arr)

    @staticmethod
    def get_sent_pattern_inst_text(sent_pattern):
        arr = []
        for p in sent_pattern:
            if p[0] in [u'VERB', u'NOUN', u'PROPN']:
                arr.append(p[2])
            if p[0] == u'VERB':
                break
        return ' '.join(arr).strip()

    def analyse_sentence_scope(self, mini_ptn, variant_idxs, variant_text):
        var_result = self.get_mini_pattern_var_dict(mini_ptn, variant_idxs)
        var_dict = var_result['dict']
        ptn2insts = {}
        for inst in var_dict[variant_text]:
            # format: [('KCL_COMP', 7, ''), (u'ADP', 5, u'for'), (u'VERB', 4, u'Tested', [])]
            ptns = inst.sentence_scope_pattern()
            ptn_str = CorpusAnalyzer.get_sent_pattern_key(ptns)
            arr = []
            if ptn_str in ptn2insts:
                arr = ptn2insts[ptn_str]
            else:
                ptn2insts[ptn_str] = arr
            arr.append(inst)

        sent_pattern_result = CorpusAnalyzer.sort_pattern_insts(ptn2insts)
        print json.dumps(sent_pattern_result)
        sent_pattern_result['hard_ones'] = {}
        print 'working on hard ones...'
        for ptn in sent_pattern_result['hard']:
            print 'working on %s (#inst %s)...' % (ptn[0], ptn[2])
            spec_ptn_insts = {}
            for inst in ptn2insts[ptn[0]]:
                s_sent_specific = CorpusAnalyzer.get_sent_pattern_inst_text(inst.sent_pattern)
                arr = []
                if s_sent_specific in spec_ptn_insts:
                    arr = spec_ptn_insts[s_sent_specific]
                else:
                    spec_ptn_insts[s_sent_specific] = arr
                arr.append(inst)
            cls2ptns = tg.word2vect_clustering(CorpusAnalyzer.load_en_model(), [sp for sp in spec_ptn_insts],
                                               cluster_prefix='spec_verbs', eps=3.6)
            cls2insts = {}
            for cls in cls2ptns:
                for p in cls2ptns[cls]:
                    arr = []
                    if cls in cls2insts:
                        arr = cls2insts[cls]
                    else:
                        cls2insts[cls] = arr
                    arr += spec_ptn_insts[p]
            ret = CorpusAnalyzer.sort_pattern_insts(cls2insts, threshold=2)
            ret['cls2ptns'] = cls2ptns
            print json.dumps(ret)
            print '----\n'
            sent_pattern_result['hard_ones'][ptn[0]] = ret
        return sent_pattern_result

    @staticmethod
    def sort_pattern_insts(ptn2insts, threshold=5):
        arr = [(p, len(ptn2insts[p])) for p in ptn2insts]
        arr = sorted(arr, cmp=lambda x1, x2: x2[1] - x1[1])
        freq_ptns = []
        infreq_ptns = []
        for pe in arr:
            if pe[1] > threshold:
                freq_ptns.append(pe)
            else:
                infreq_ptns.append(pe)
        print 'infrequent ones [%s]' % infreq_ptns
        return CorpusAnalyzer.discriminate_patterns([p[0] for p in freq_ptns], ptn2insts)

    @staticmethod
    def discriminate_patterns(freq_patterns, variant_insts):
        # - easy ones: high/low accuracy [0, .25] and [.75, 1]
        # - hard ones: (.25, .75)
        easy_correct = []
        easy_incorrect = []
        difficulties = []
        for fp in freq_patterns:
            num_correct = 0
            num_incorrect = 0
            for inst in variant_insts[fp]:
                if inst.annotations[0]['gt_label'] != '-':
                    if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label']:
                        num_correct += 1
                    else:
                        num_incorrect += 1
            if num_correct + num_incorrect > 0:
                accuracy = num_correct * 1.0 / (num_correct + num_incorrect)
                if accuracy >= _acc_high_threshold:
                    easy_correct.append((fp, accuracy, len(variant_insts[fp])))
                elif accuracy <= _acc_low_threshold:
                    easy_incorrect.append((fp, accuracy, len(variant_insts[fp]))
                                          )
                else:
                    difficulties.append((fp, accuracy, len(variant_insts[fp])))
        return {'easy_accurate': easy_correct,
                'easy_inaccurate': easy_incorrect,
                'hard': difficulties}


    @staticmethod
    def print_dictionary_anns(mini_ptns):
        print '#patterns %s' % len(mini_ptns)
        for p in mini_ptns:
            correct_ptns = []
            incorrect_ptns = []
            for inst in mini_ptns[p]:
                if inst.annotations[0]['gt_label'] != '-':
                    correct_ptns.append(inst.sentence + ' ' + inst.annotations[0]['signed_label']) \
                        if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label'] \
                        else incorrect_ptns.append(inst.sentence + ' ' + inst.annotations[0]['signed_label'])
            print '%s, #insts %s, #veryfied: %s, accuracy:%s' % \
                  (p, len(mini_ptns[p]),
                   len(correct_ptns) + len(incorrect_ptns),
                   1.0 * len(correct_ptns) / (len(correct_ptns) + len(incorrect_ptns))
                   if len(correct_ptns) + len(incorrect_ptns) > 0 else '-')
            if len(correct_ptns) + len(incorrect_ptns) > 20:
                print '**correct: %s' % correct_ptns
                print '**incorrect: %s' % incorrect_ptns
            print '--\n'

    def get_mini_comp_dict(self):
        if self._mini_comp_dict is None:
            print 'producing minimal component dictionary...'
            self._mini_comp_dict = self.merge_by_mini_comp()
        return self._mini_comp_dict

    def produce_save_comp_dict(self, mini_save_file):
        self._mini_comp_dict = self.merge_by_mini_comp()
        CorpusAnalyzer.save_minimal_component_dictionary(self._mini_comp_dict, mini_save_file)

    def load_mini_comp_dict(self, mini_save_file):
        self._mini_comp_dict = jl.load(mini_save_file)

    def serialise(self, save_file_path):
        jl.dump(self, save_file_path)

    @staticmethod
    def load_seralisation(load_file_path):
        return jl.load(load_file_path)

    @staticmethod
    def save_minimal_component_dictionary(mini_dic, mini_save_file):
        jl.dump(mini_dic, mini_save_file)

    @staticmethod
    def load_en_model():
        global _en_model
        if _en_model is None:
            _en_model = spacy.load('en_core_web_lg')
        return _en_model


class CorpusPredictor(object):
    def __init__(self, corpus_model):
        self._model = corpus_model

    def predcit(self, sentInst):
        # print sentInst
        mini_comp_ptn, token_list = sentInst.get_minimal_component()
        print mini_comp_ptn
        for mp in self._model:
            if mp['mini_comp_pattern'] == mini_comp_ptn:
                if mp['all_accuracy'] >= _acc_high_threshold or mp['all_accuracy'] <= _acc_low_threshold:
                    return mp['all_accuracy']
                else:
                    key = CorpusAnalyzer.get_mini_pattern_inst_text(token_list, mp['variation_pos'])
                    match_ret = CorpusPredictor.match_pattern(mp, key)
                    if match_ret is None:
                        # not matched
                        return mp['all_accuracy']
                    elif match_ret[1]:
                        # matched easy patterns, either accurate ones or inaccurate ones
                        return match_ret[0]
                    else:
                        # matched hard patterns
                        # generate sentence pattern
                        sent_ptns = sentInst.sentence_scope_pattern()
                        # get pattern string
                        sent_ptn_str = CorpusAnalyzer.get_sent_pattern_key(sent_ptns)
                        return CorpusPredictor.match_sentence_pattern(key, mp['hard_ones'],
                                                                      sent_ptns, sent_ptn_str, mp['all_accuracy'])
                break
        # pattern never seen before
        return -1

    @staticmethod
    def match_sentence_pattern(key, hard_ones, sent_ptns, sent_ptn_str, default_accuracy):
        if key in hard_ones:
            pattern_component = hard_ones[key]
            match_ret = CorpusPredictor.match_pattern(pattern_component, sent_ptn_str)
            if match_ret is None:
                # not matched, roll back to mini component accuracy
                return default_accuracy
            elif match_ret[1]:
                # matched easy patterns, either accurate ones or inaccurate ones
                return match_ret[0]
            else:
                # matched hard patterns, continue with sentence variants
                if sent_ptn_str in pattern_component['hard_ones']:
                    sp = pattern_component['hard_ones'][sent_ptn_str]
                    # get sentence pattern instance text
                    s_sent_specific = CorpusAnalyzer.get_sent_pattern_inst_text(sent_ptns)
                    # get cluster label
                    cls_name = s_sent_specific
                    for cls in sp['cls2ptns']:
                        if s_sent_specific in sp['cls2ptns'][cls]:
                            cls_name = cls
                            break
                    sent_mret = CorpusPredictor.match_pattern(sp, cls_name)
                    if sent_mret is None:
                        # return hard overall accuracy
                        return match_ret[0][1]
                    elif sent_mret[1]:
                        # matched easy patterns, either accurate ones or inaccurate ones
                        return sent_mret[0]
                    else:
                        # return leave hard accuracy
                        return sent_mret[0][1]
                else:
                    return match_ret[0][1]
        else:
            # not in hard pattern, either in infrequent ones or not seen before
            return default_accuracy

    @staticmethod
    def match_pattern(mp, key):
        easy_ptn = CorpusPredictor.iterate_get_ptn(mp['easy_accurate'] + mp['easy_accurate'], key)
        if easy_ptn is not None:
            return easy_ptn[1], True
        else:
            hard_ptn = CorpusPredictor.iterate_get_ptn(mp['hard'], key)
            if hard_ptn is not None:
                # need to do sentence level pattern matching
                return hard_ptn, False
        return None

    @staticmethod
    def iterate_get_ptn(ptn_list, key):
        for p in ptn_list:
            # p is in the form: ['VARIANT KEY', ACCURACY, FREQ]
            if p[0] == key:
                return p
        return None

    @staticmethod
    def load_corpus_model(model_file):
        corpus_model = utils.load_json_data(model_file)
        return CorpusPredictor(corpus_model)


class POSSentencePatternInst(SentencePatternInst):
    """
    A pattern instance - a sentence containing at least one annotation
    """
    def __init__(self, sent_model, anns):
        self._sent_model = sent_model.as_doc()
        self._anns = anns
        self._sentence = sent_model.text
        self._raw_pattern = None
        self._pattern = None
        self._mini_text_comps = None
        self._sentence_scope_pattern = None
        self._doc_id = None

    def process(self):
        self.pos_tag_parsing()
        self.generalise()

    @property
    def doc_id(self):
        return self._doc_id

    @doc_id.setter
    def doc_id(self, value):
        self._doc_id = value

    @property
    def annotations(self):
        return self._anns

    @property
    def sentence(self):
        return self._sentence

    def sent_model(self):
        return self._sent_model

    def pos_tag_parsing(self):
        self._raw_pattern = []
        print 'sent Doc model [%s]' % self._sent_model
        for sent in self._sent_model.sents:
            for token in sent:
                e = token.idx + len(token.text)
                is_matched = False
                for ann in self._anns:
                    if token.idx >= ann['s'] and e <= ann['e']:
                        self._raw_pattern.append((token.text, token.pos_, True, ann['signed_label'], ann['gt_label']))
                        is_matched = True
                        break
                # print '%s-%s, %s: [%s]' % (token.idx, e, token.idx, token.text)
                if not is_matched:
                    self._raw_pattern.append((token.text, token.pos_))

    def generalise(self):
        """
        simple POS tag based generalisation implementation
        :return:
        """
        self._pattern = []
        prev_tag = None
        for r in self._raw_pattern:
            this_tag = 'KCL' if len(r) > 2 else r[1]
            if prev_tag != this_tag:
                self._pattern.append(this_tag)
                prev_tag = this_tag
        # remove punctuation and tailing space chars
        while len(self._pattern) > 0 and self._pattern[-1] in [u'PUNCT', u'SPACE']:
            self._pattern.pop(-1)
        self._pattern = '->'.join(self._pattern)

    def get_token_by_text_pos(self, text, pos):
        """
        search token using its text and POS tag
        :param text:
        :param pos:
        :return:
        """
        for sent in self._sent_model.sents:
            for t in sent:
                if t.text == text and t.pos_ == pos:
                    return t
        return None

    def iter_get_children(self, token, container):
        """
        iteratively add a token's children into a container
        :param token:
        :param container:
        :return:
        """
        # 'iter on %s ' % token
        try:
            for c in token.children:
                if not POSSentencePatternInst.addable_indirect(c):
                    break
                if c not in container:
                    container.append(c)
                    self.iter_get_children(c, container)
        except Exception:
            print '!!!getting children failed!!!!'

    @staticmethod
    def addable_indirect(token):
        return token.pos_ not in _STOP_POS_TAGS

    @property
    def mini_text_component(self):
        return self._mini_text_comps

    def get_minimal_component(self):
        """
        compute semantically minimal component around the annotated entity based on sentence parse tree
        :return:
        """
        # print '--'
        # print 'min comps [%s]' % self.sentence
        # for token in self._sent_model:
        #     print(token.text, token.dep_, token.head.text, token.head.pos_,
        #           [child for child in token.children])

        # locate first consecutive ann tokens
        ann_tokens = []
        first_ann_token = None
        for p in self._raw_pattern:
            if len(p) > 2:
                first_ann_token = self.get_token_by_text_pos(p[0], p[1])
                if first_ann_token is not None:
                    if len(ann_tokens) > 0:
                        if ann_tokens[-1].i == first_ann_token.i -1:
                            ann_tokens.append(first_ann_token)
                        else:
                            break
                    else:
                        ann_tokens.append(first_ann_token)
        # print '=====>%s' % ann_tokens
        if first_ann_token is None:
            print 'annotated token not found!! %s, %s' % (p[0], p[1])
            print self._raw_pattern
            return None, None
        else: 
            # token_comps = [first_ann_token]
            self.iter_get_children(first_ann_token, ann_tokens)
            parents = []
            prev_p = first_ann_token
            p = first_ann_token.head
            while p != prev_p:
                if p.pos_ == 'VERB' or p.pos_ == 'CCONJ':
                    break
                if p.i >= first_ann_token.i:
                    self.iter_get_children(p, ann_tokens)
                parents.append(p)
                prev_p = p
                p = p.head
            # print 'print parents: [%s]' % ['%s(%s, %s)' % (p.text, p.pos_, p.i) for p in parents]
            arr_ptns = [(p.pos_, p.i, p.text) for p in ann_tokens] + [(p.pos_, p.i, p.text) for p in parents if p.i >= first_ann_token.i]
            arr_ptns = sorted(arr_ptns, cmp=lambda x1, x2: x1[1] - x2[1])
            generalised_ptn = self.remove_line_separated_tokens([p for p in arr_ptns], first_ann_token)
            # print generalised_ptn

            # simplify
            s_ptns = []
            tokens_list = []
            sim_min_pattern = self.simplify_minimal_pattern(generalised_ptn)
            for p in sim_min_pattern:
                if len(s_ptns) > 0 and s_ptns[-1] == p[0]: # p[0] - pos tag, p[1] - index, p[2] - text
                    tokens_list[-1].append(p[2])
                    continue
                s_ptns.append(p[0])
                tokens_list.append([p[2]])
            self._mini_text_comps = sim_min_pattern
            return '->'.join(s_ptns), tokens_list

    def remove_line_separated_tokens(self, ptn_list, ann_token):
        last_head_space = -1
        first_tail_space = 10000
        for t in self._sent_model:
            if t.i > ann_token.i:
                if t.dep_ == u'':
                    first_tail_space = t.i
                    break
            elif t.i > ptn_list[0][1]:
                if t.dep_ == u'':
                    last_head_space = t.i
            if t.i > ptn_list[len(ptn_list) - 1][1]:
                break
            # if t.dep_ == u'' and ptn_list[0][1] <= t.i <= ptn_list[len(ptn_list) - 1][1]:
            #     return [p for p in ptn_list if p[1] <= t.i]
        ret = []
        for p in ptn_list:
            if p[1] > last_head_space:
                ret.append(p)
        new_ret = []
        for p in ret:
            if p[1] < first_tail_space:
                new_ret.append(p)
        return new_ret

    def simplify_minimal_pattern(self, ptns):
        """
        replace annotated token(s) with a special tag and merge consecutive duplicate tags
        :param ptns:
        :return:
        """
        ann_tokens = []
        for r in self._raw_pattern:
            if len(r) > 2:
                ann_tokens.append((r[0], r[1]))
        s_ptn = []
        for ptn in ptns:
            to_add = ptn
            for at in ann_tokens:
                if ptn[0] == at[1] and ptn[2] == at[0]:
                    to_add = ('KCL', ptn[1], ptn[2])
                    break
            s_ptn.append(to_add)
        return s_ptn

    def find_tokey_by_index(self, idx):
        for t in self._sent_model:
            if t.i == idx:
                return t
        return None

    def sentence_scope_pattern(self):
        last_idx = self.mini_text_component[-1][1]
        # it seems important to remove SPACE separated lines because of the sentence tokenizer does not work very well
        min_idx = -1
        max_idx = 10000
        for t in self._sent_model:
            # print '%s\t%s\t%s' % (t.text, t.dep_, t.head.text)
            if t.i < last_idx:
                if t.dep_ == u'':
                    min_idx = t.i
            elif t.dep_ == u'':
                max_idx = t.i
                break
        # print min_idx, max_idx
        ptn = [('KCL_COMP', last_idx, '')]
        last_token = self.find_tokey_by_index(last_idx)
        while last_token.head.pos_ != u'VERB' and last_token.head != last_token:
            last_token = last_token.head
            if last_token.i <= min_idx or last_token.i >= max_idx:
                break
            ptn.append((last_token.pos_, last_token.i, last_token.text))
        while last_token.head.pos_ == u'VERB' and last_token.head != last_token:
            last_token = last_token.head
            if last_token.i <= min_idx or last_token.i >= max_idx:
                break
            verb_ptn = (last_token.pos_, last_token.i, last_token.text)
            negs = []
            try:
                for c in last_token.children:
                    if c.dep_ == u'neg':
                        negs.append((c.pos_, c.i, c.text))
            except Exception:
                print '!!!reading children error!!!'
                pass
            verb_ptn = list(verb_ptn)
            verb_ptn.append(negs)
            verb_ptn = tuple(verb_ptn)
            ptn.append(verb_ptn)
        # if last_token.pos_ == u'VERB' and last_token.head == last_token:
        #     for c in last_token.children:
        #         if c.i <= min_idx or c.i >= max_idx:
        #             break
        #         if c.dep_ == u'nsubj':
        #             ptn.append((c.pos_, c.i, c.text))
        #             break
        self._sentence_scope_pattern = ptn
        return ptn

    @property
    def sent_pattern(self):
        return self._sentence_scope_pattern
