import tensorflow as tf
import os
import time
import utils
import numpy as np
from sklearn.model_selection import KFold
import shutil


BATCH_SIZE = 80
NUM_STEPS = 41
HIDDEN_SIZE = 300
SKIP_STEP = 4
TEMPRATURE = 0.7
LR = 0.00005
LEN_GENERATED = 16
TYPED_ANNS = ['posM', 'negM', 'hisM', 'otherM']
ANN_TOKEN = 'UMLS_CONCEPT_LABEL'
USE_TWO_CLASSES = True
DATASET_SPLIT = .9
number_of_layers = 1
EPOCH_SIZE = 5


class AnnAssessor(object):
    def __init__(self):
        pass

    def load_model(self):
        pass


class AssessorTrainer(object):
    def __init__(self):
        self._one_dir_size = (NUM_STEPS - 1)/2
        self._vocab = None
        self._idx2word = None
        self._vocab_file = None
        self._learning_rate = 0.003
        self._encoded_output = None
        self._typed_anns = None
        self._data = None
        self._labels = None
        self._offset = 0
        self._model_name = 'cris'

    @property
    def model_name(self):
        return self._model_name

    @model_name.setter
    def model_name(self, value):
        self._model_name = value

    @property
    def hidden_size(self):
        return HIDDEN_SIZE

    @property
    def vocabulary(self):
        return self._vocab

    @vocabulary.setter
    def vocabulary(self, value):
        self._vocab = value
        self._idx2word = {}
        for w in value:
            self._idx2word[value[w]] = w
        self._vocab[ANN_TOKEN] = len(self._vocab)
        self._idx2word[len(self._idx2word)] = ANN_TOKEN
        print(len(self._idx2word))
        self._typed_anns = []
        for t in TYPED_ANNS:
            self._typed_anns.append(self._vocab[t])

    def vocab_encode(self, tokens):
        return [self._vocab[t] + 1 for t in tokens]

    def vocab_decode(self, encoding):
        return ' '.join([self._idx2word[idx] for idx in encoding])

    @property
    def encoded_output(self):
        return self._encoded_output

    @encoded_output.setter
    def encoded_output(self, value):
        self._encoded_output = value

    @property
    def vocab_file(self):
        return self._vocab_file

    @vocab_file.setter
    def vocab_file(self, value):
        self._vocab_file = value
        self.vocabulary = utils.load_json_data(value)

    def process_data(self):
        encoded_ann_ctx_file = self.encoded_output
        one_dir_size = self._one_dir_size
        ann_ctxs = utils.load_json_data(encoded_ann_ctx_file)
        self._data = []
        self._labels = []
        for ctx in ann_ctxs:
            chunk = [c + 1 for c in ctx['prev']][-one_dir_size:] \
                    + self.vocab_encode([ANN_TOKEN]) \
                    + [c + 1 for c in ctx['next']][:one_dir_size]
            chunk += [0] * (NUM_STEPS - len(chunk))
            self._data.append(chunk)
            # produce labels
            if not USE_TWO_CLASSES:
                label = np.zeros(len(self._typed_anns))
                label[self._typed_anns.index(ctx['label_encoded'])] = 1
                self._labels.append(label)
            else:
                self._labels.append([1.0, 0.0] if ctx['label'] == 'posM' else [0.0, 1.0])

    def reset_read_offset(self):
        self._offset = 0

    def read_bidirectional_data(self, batch_size=BATCH_SIZE, test=False):
        cut_pos = int(len(self._data) * DATASET_SPLIT)
        s = cut_pos + 1 if test else 0
        e = len(self._data) if test else cut_pos
        cur_start = s + self._offset
        cur_end = min(s + self._offset + batch_size, e)

        if cur_start >= e:
            return None, None

        batch_data = self._data[cur_start: cur_end]
        batch_labels = self._labels[cur_start: cur_end]
        self._offset = cur_end - s
        return batch_data, batch_labels

    def k_fold_read_data(self, batch_size=BATCH_SIZE, folds=10, epoch=EPOCH_SIZE):
        X = np.array(self._data)
        Y = np.array(self._labels)
        kf = KFold(folds)
        for train_index, test_index in kf.split(self._data):
            train_len = len(train_index) // batch_size + (1 if len(train_index) % batch_size != 0 else 0)
            for epoch_idx in xrange(epoch):
                print('epoch %s ...' % epoch_idx)
                for idx in xrange(train_len):
                    yield X[train_index[idx * batch_size : (idx + 1) * batch_size]], \
                          Y[train_index[idx * batch_size : (idx + 1) * batch_size]], \
                          None, None
            yield None, None, X[test_index], Y[test_index]

    def init_bidir_graph(self):
        label_size = 2 if USE_TWO_CLASSES else len(self._typed_anns)
        # initialise placeholders
        x = tf.placeholder(tf.int32, [None, None])
        y = tf.placeholder(tf.float32, [None, label_size])

        oh_seq = tf.one_hot(x - 1, len(self.vocabulary))
        print('vocab size: %s' % len(self.vocabulary))

        # this line to calculate the real length of seq
        # all seq are padded to be of the same length which is NUM_STEPS
        length = tf.reduce_sum(tf.reduce_max(tf.sign(oh_seq), 2), 1)
        length = tf.cast(length, tf.int32)

        if number_of_layers > 1:
            stacked_fw = tf.contrib.rnn.MultiRNNCell([tf.contrib.rnn.GRUCell(self.hidden_size) for _ in range(number_of_layers)],
                                                     state_is_tuple=False)
            stacked_bw = tf.contrib.rnn.MultiRNNCell([tf.contrib.rnn.GRUCell(self.hidden_size) for _ in range(number_of_layers)],
                                                     state_is_tuple=False)
            in_state_fw = tf.placeholder_with_default(
                stacked_fw.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size * number_of_layers])
            in_state_bw = tf.placeholder_with_default(
                stacked_bw.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size * number_of_layers])
            outputs, out_states = tf.nn.bidirectional_dynamic_rnn(stacked_fw, stacked_bw, oh_seq,
                                                                  sequence_length=length,
                                                                  # dtype=tf.float32)
                                                                  initial_state_fw=in_state_fw,
                                                                  initial_state_bw=in_state_bw)
        else:
            cell_fw = tf.contrib.rnn.GRUCell(self.hidden_size)
            cell_bw = tf.contrib.rnn.GRUCell(self.hidden_size)
            in_state_fw = tf.placeholder_with_default(
                cell_fw.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size])
            in_state_bw = tf.placeholder_with_default(
                cell_bw.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size])
            outputs, out_states = tf.nn.bidirectional_dynamic_rnn(cell_fw, cell_bw , oh_seq,
                                                                  sequence_length=length,
                                                                  # dtype=tf.float32)
                                                                  initial_state_fw=in_state_fw,
                                                                  initial_state_bw=in_state_bw)
        output_fw, output_bw = outputs
        outputs = tf.concat([output_fw, output_bw], -1)

        # Hack to build the indexing and retrieve the right output.
        batch_size = tf.shape(outputs)[0]
        # Start indices for each sample
        index = tf.range(0, batch_size) * NUM_STEPS + (length - 1)
        # Indexing
        outputs = tf.gather(tf.reshape(outputs, [-1, HIDDEN_SIZE]), index)

        # fully_connected is syntactic sugar for tf.matmul(w, output) + b
        # it will create w and b for us
        # logits = tf.contrib.layers.fully_connected(outputs, len(self.vocabulary), None)

        # Define weights
        weights = {
            'out': tf.Variable(tf.random_normal([HIDDEN_SIZE, label_size]))
        }
        biases = {
            'out': tf.Variable(tf.random_normal([label_size]))
        }
        logits = tf.matmul(outputs, weights['out']) + biases['out']
        # loss = tf.reduce_sum(
        #     tf.nn.weighted_cross_entropy_with_logits(targets=y, logits=logits, pos_weight=tf.constant([0.32, 1.0])))
        loss = tf.reduce_sum(tf.nn.softmax_cross_entropy_with_logits(logits=logits, labels=y))
        return x, y, oh_seq, logits, loss, (in_state_fw, in_state_bw), out_states

    def train_bidir(self):
        x, y, oh_seq, logits, loss, _, out_state = self.init_bidir_graph()

        global_step = tf.Variable(0, dtype=tf.int32, trainable=False, name='global_step')
        optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(loss, global_step=global_step)
        saver = tf.train.Saver()
        start = time.time()
        with tf.Session() as sess:
            writer = tf.summary.FileWriter('graphs/gist', sess.graph)
            sess.run(tf.global_variables_initializer())

            chk_path = 'checkpoints/' + self.model_name
            if os.path.exists(chk_path):
                shutil.rmtree(chk_path)
                os.makedirs(chk_path)
            else:
                os.makedirs(chk_path)
            # ckpt = tf.train.get_checkpoint_state(os.path.dirname(chk_path + '/checkpoint'))
            # if ckpt and ckpt.model_checkpoint_path:
            #     saver.restore(sess, ckpt.model_checkpoint_path)

            iteration = global_step.eval()
            fold_idx = 0
            for train_x, train_y, test_x, test_y in at.k_fold_read_data():
                if train_x is not None:
                    batch_loss, _ = sess.run([loss, optimizer], {x: train_x, y: train_y})
                    if (iteration + 1) % SKIP_STEP == 0:
                        print('\tLoss {}. Time {}'.format(batch_loss, time.time() - start))
                        start = time.time()
                        saver.save(sess, chk_path + '/rnn', iteration)
                    iteration += 1
                elif test_x is not None:
                    print '\n\ntesting on [%s] items' % len(test_x)
                    self.test_Data(sess, logits, x, y, test_x, test_y, fold_idx)
                    fold_idx += 1
                    if os.path.exists(chk_path):
                        shutil.rmtree(chk_path)
                        os.makedirs(chk_path)
                    else:
                        os.makedirs(chk_path)
                    sess.run(tf.global_variables_initializer())

    def test_Data(self, sess, logits, x, y, test_x, test_y, fold_idx):
        n_correct = 0
        n_total = 0
        n_pos_correct = 0
        n_pos_total = 0
        output = ''
        for idx in xrange(len(test_x)):
            p, c = sess.run([tf.nn.softmax(logits), y], {x: test_x[idx:idx+1], y: test_y[idx:idx+1]})
            predicted = np.argmax(p)
            correct_label = np.argmax(c)
            if predicted == correct_label:
                n_correct += 1
            line = '%s\t%s\t%s\t%s\t%s' % (n_total, predicted, correct_label,
                                          '%.2f' % p.tolist()[0][predicted],
                                          '\t'.join(['%.2f' % p for p in p.tolist()[0]]))
            print(line)
            output += line + '\n'
            n_total += 1
            if correct_label == 0:
                n_pos_total += 1
                if correct_label == predicted:
                    n_pos_correct += 1
        utils.save_string(output, './output/weighted_fold_%s.txt' % fold_idx)
        print 'accuracy: %s' % (1.0 * n_correct / n_total)
        print 'pos recall: %s' % (1.0 * n_pos_correct / n_pos_total)

    def test_bidir(self):
        x, y, oh_seq, logits, loss, _, out_state = self.init_bidir_graph()
        global_step = tf.Variable(0, dtype=tf.int32, trainable=False, name='global_step')
        optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(loss, global_step=global_step)
        saver = tf.train.Saver()
        start = time.time()
        with tf.Session() as sess:
            writer = tf.summary.FileWriter('graphs/gist', sess.graph)
            sess.run(tf.global_variables_initializer())

            ckpt = tf.train.get_checkpoint_state(os.path.dirname('checkpoints/' + self.model_name + '/checkpoint'))
            if ckpt and ckpt.model_checkpoint_path:
                saver.restore(sess, ckpt.model_checkpoint_path)

            iteration = global_step.eval()
            data_batch, label_batch = self.read_bidirectional_data(batch_size=1, test=True)
            n_correct = 0
            n_total = 0
            n_pos_correct = 0
            n_pos_total = 0
            while data_batch is not None:
                p, c = sess.run([tf.nn.softmax(logits), y], {x: data_batch, y: label_batch})
                predicted = np.argmax(p)
                correct_label = np.argmax(c)
                if predicted == correct_label:
                    n_correct += 1
                print '%s\t%s\t%s\t%s\t%s' % (n_total, predicted, correct_label,
                                              '%.2f' % p.tolist()[0][predicted],
                                              '\t'.join(['%.2f' % p for p in p.tolist()[0]]))
                data_batch, label_batch = self.read_bidirectional_data(batch_size=1, test=True)
                n_total += 1
                if correct_label == 0:
                    n_pos_total += 1
                    if correct_label == predicted:
                        n_pos_correct += 1
            print 'accuracy: %s' % (1.0 * n_correct / n_total)
            print 'pos accuracy: %s' % (1.0 * n_pos_correct / n_pos_total)


if __name__ == "__main__":
    at = AssessorTrainer()
    at.model_name = 'cris_2classes_3layers'
    at.vocab_file = './data/word_to_index_full.json'
    at.encoded_output = './data/encoded_ann_ctx_full.json'
    at.process_data()
    at.train_bidir()
    # at.test_bidir()