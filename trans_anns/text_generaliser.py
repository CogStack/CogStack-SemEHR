import spacy


def sentence_parsing(s):
    s = s.replace('\n', ' ')
    nlp = spacy.load('en')
    doc = nlp(s)
    for token in doc:
        print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
              token.shape_, token.is_alpha, token.is_stop)

    print '\ntree parsed as follows\n'

    for token in doc:
        print(token.text, token.dep_, token.head.text, token.head.pos_,
              [child for child in token.children])


def word2vec_testing():
    nlp = spacy.load('en_core_web_lg')
    tokens = nlp(u'think concern have')
    for token1 in tokens:
        for token2 in tokens:
            print('%s vs %s:' % (token1.text, token2.text),
                  token1.similarity(token2))


# word2vec_testing()
# sentence_parsing(u'Routine blood investigation and virology for Hep C done')

s1 = u"""
 60 yo female with hx of chronic hepatitis C infection (without
   cirrhosis) presenting to the ED with 4 day hx of dysuria, chills and
   found to have a grossly positive U/A with evidence of bilateral
   pyelonephritis on CT Abd/pelvis who became hypotensive to 80/30.
"""
sentence_parsing(s1)
