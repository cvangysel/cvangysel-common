from cvangysel import io_utils, multiprocessing_utils

import collections
import io
import itertools
import logging
import multiprocessing
import numpy as np
import os
import tempfile
import re
import scipy.stats
import shutil
import subprocess
import sys

measures = {
    'success_1': 'P@1',
    'P_5': 'P@5',
    'P_10': 'P@10',
    'P_100': 'P@100',
    'map': 'MAP',
    'map_cut_10': 'MAP@10',
    'map_cut_100': 'MAP@100',
    'map_cut_1000': 'MAP@1000',
    'ndcg': 'NDCG',
    'ndcg_cut_10': 'NDCG@10',
    'ndcg_cut_100': 'NDCG@100',
    'recip_rank': 'MRR',
    'bpref': 'Bpref',
    'Rprec': 'R-Precision',
    'recall_10': 'Recall@10',
}


def _parse_trectext(iter, ignore_content=False):
    start_doc_re = re.compile(r'^<DOC>$')
    end_doc_re = re.compile(r'^(.*)</DOC>$')

    start_doc_hdr = re.compile(r'^<DOCHDR>$')
    end_doc_hdr = re.compile(r'^</DOCHDR>$')

    doc_id_re = re.compile(r'^<DOCNO>(\s*(.*)\s*</DOCNO>)?$')
    doc_old_id_re = re.compile(r'^<DOCOLDNO>\s*(.*)\s*</DOCOLDNO>$')

    current_document = None
    current_content = None

    def process_content(content):
        if current_document is None:
            logging.error(
                'Encountered input outside of document context: %s', content)

            return False
        elif current_document['id'] is None:
            logging.error(
                'Encountered input before document identifier: %s', content)

            return False
        elif current_content is None:
            logging.error(
                'Encountered input within document without context: %s',
                content)

            return False

        if ignore_content:
            return True

        content = content.strip()

        # TODO(cvangysel): filter_non_ascii is a very harsh filtering technique.
        #                  Figure out if we really need this here.
        current_content.append(io_utils.filter_non_ascii(content))

        return True

    for line in iter:
        doc_id_match = doc_id_re.match(line)
        doc_old_id_match = doc_old_id_re.match(line)
        end_doc_match = end_doc_re.match(line)

        if line.isspace():
            continue
        elif start_doc_re.match(line):
            assert current_document is None, current_document

            current_document = {
                'id': None,
                'header': [],
                'content': [],
            }

            current_content = current_document['content']
        elif end_doc_match:
            assert current_document is not None

            process_content(end_doc_match.group(1))

            yield current_document['id'], current_document['content']

            current_document = None
            current_content = None
        elif start_doc_hdr.match(line):
            assert current_document is not None
            assert current_document['id'] is not None
            assert not current_document['content'], (current_document, [line])

            current_content = current_document['header']
        elif end_doc_hdr.match(line):
            assert current_document is not None
            assert current_document['id'] is not None
            assert not current_document['content']

            current_content = current_document['content']
        elif doc_id_match:
            assert current_document is not None
            assert current_document['id'] is None

            if doc_id_match.group(2) is not None:
                current_document['id'] = doc_id_match.group(2).strip()
            else:
                current_document['id'] = next(iter).strip()
                assert next(iter).strip() == '</DOCNO>'
        elif doc_old_id_match:
            pass  # Legacy document id.
        else:
            process_content(line)


def _iter_trectext_document_ids_worker(data):
    document_path, encoding = data

    logging.debug('Iterating over %s.', document_path)

    with io_utils.open(document_path, 'r', encoding=encoding) as f:
        return [doc_id for doc_id, _ in _parse_trectext(f)]


def _iter_trectext_documents_multiprocessing_worker_initializer(
        result_queue,
        replace_digits, strip_html, tokenize,
        ignore_words,
        document_ids,
        encoding):
    _iter_trectext_documents_multiprocessing_worker_.result_queue = \
        result_queue

    _iter_trectext_documents_multiprocessing_worker_.strip_html = strip_html
    _iter_trectext_documents_multiprocessing_worker_.replace_digits = \
        replace_digits
    _iter_trectext_documents_multiprocessing_worker_.tokenize = tokenize
    _iter_trectext_documents_multiprocessing_worker_.ignore_words = \
        ignore_words
    _iter_trectext_documents_multiprocessing_worker_.document_ids = \
        set(document_ids)

    _iter_trectext_documents_multiprocessing_worker_.encoding = encoding

    _iter_trectext_documents_multiprocessing_worker_.digit_regex = \
        re.compile('\d+')


def _iter_trectext_documents_multiprocessing_worker_(document_path):
    logging.debug('Iterating over %s.', document_path)

    num_documents = 0

    with io_utils.open(
            document_path, 'r',
            encoding=_iter_trectext_documents_multiprocessing_worker_.
            encoding) as f:
        for doc_id, text in _parse_trectext(f):
            if (_iter_trectext_documents_multiprocessing_worker_.
                document_ids and
                    doc_id not in
                    _iter_trectext_documents_multiprocessing_worker_.
                    document_ids):
                continue

            # Concatenate document lines.
            text = ' '.join(text)

            if _iter_trectext_documents_multiprocessing_worker_.strip_html:
                text = io_utils.strip_html(text)

            if _iter_trectext_documents_multiprocessing_worker_.replace_digits:
                text = (
                    _iter_trectext_documents_multiprocessing_worker_.
                    digit_regex.
                    sub('<num>', text))

            if _iter_trectext_documents_multiprocessing_worker_.tokenize:
                tokens = io_utils.tokenize_text(
                    text,
                    ignore_words=(
                        _iter_trectext_documents_multiprocessing_worker_.
                        ignore_words))

                _iter_trectext_documents_multiprocessing_worker_.\
                    result_queue.put((doc_id, tokens))
            else:
                _iter_trectext_documents_multiprocessing_worker_.\
                    result_queue.put((doc_id, text))

            num_documents += 1

    return num_documents

_iter_trectext_documents_multiprocessing_worker = \
    multiprocessing_utils.WorkerFunction(
        _iter_trectext_documents_multiprocessing_worker_)


remove_parentheses_re = re.compile(r'\((.*)\)')


def parse_query(unsplitted_terms):
    assert isinstance(unsplitted_terms, str)

    unsplitted_terms = remove_parentheses_re.sub(
        r'\1', unsplitted_terms.strip())
    unsplitted_terms = unsplitted_terms.replace('/', ' ')
    unsplitted_terms = unsplitted_terms.replace('-', ' ')

    return list(io_utils.token_stream(
        io_utils.lowercased_stream(
            io_utils.filter_non_latin_stream(
                io_utils.filter_non_alphanumeric_stream(
                    io_utils.unicode_normalize_stream(
                        iter(unsplitted_terms))))), eos_chars=[]))


def parse_qrel(f_qrel, lowercase_items=False):
    qrel = []

    def key_fn(line):
        return line.split()[0]

    for topic_id, group in itertools.groupby(
            sorted(f_qrel, key=key_fn),
            key=key_fn):
        relevant_items = []

        for line in group:
            _, _, item, relevance = line.strip().split()
            relevance = float(relevance)

            if lowercase_items:
                item = item.lower()

            relevant_items.append((item, relevance))

        qrel.append((topic_id, relevant_items))

    return qrel


def parse_trec_topics(f_trec_topics):
    tag_re = re.compile(r'^<([A-Za-z]+)>(.*)$')

    def _parse_line():
        line = next(f_trec_topics).strip()

        while not line:
            line = next(f_trec_topics).strip()

        return line

    topics = []

    current_topic = None
    line = _parse_line()

    try:
        while f_trec_topics:
            if current_topic is None:
                assert line == '<top>'

                current_topic = {}

                line = _parse_line()
            else:
                if line == '</top>':
                    topics.append(current_topic)
                    current_topic = None

                    line = _parse_line()
                elif line[0] == '<':
                    content = line

                    while True:
                        line = _parse_line()

                        if line[0] == '<':
                            break
                        else:
                            content += ' '
                            content += line

                    match = tag_re.match(content)

                    if match:
                        value = match.group(2).strip()

                        if value.find(':'):
                            value = value[value.find(':') + 1:].strip()

                        if value.isdigit():
                            value = int(value)

                        current_topic[match.group(1)] = value
                    else:
                        if content[:2] == '</':
                            pass  # Everything is fine.
                        else:
                            logging.error('Unmatched content: %s', content)

    except StopIteration:
        pass

    return topics


class TRECTextReader(object):

    def __init__(self, document_paths, encoding):
        self.document_paths = document_paths
        self.encoding = encoding

    def iter_document_ids(self, num_workers=1):
        document_ids = set()

        pool = multiprocessing.Pool(num_workers)

        for chunk_idx, chunk_document_ids in enumerate(pool.map(
                _iter_trectext_document_ids_worker,
                [(path, self.encoding) for path in self.document_paths])):
            if (chunk_idx + 1) % 5 == 0:
                logging.info('Processed %d out of %d paths (%.4f%%).',
                             chunk_idx + 1, len(self.document_paths),
                             100.0 * (chunk_idx + 1) / len(
                                 self.document_paths))

            document_ids.update(set(chunk_document_ids))

        return document_ids

    def iter_documents(self, replace_digits=True, strip_html=True):
        digit_regex = re.compile('\d+')

        for document_path in self.document_paths:
            logging.debug('Iterating over %s.', document_path)

            with io_utils.open(document_path, 'r', encoding=self.encoding) as f:
                for doc_id, text in _parse_trectext(f):
                    text = ' '.join(text)

                    if strip_html:
                        text = io_utils.strip_html(text)

                    if replace_digits:
                        text = digit_regex.sub('<num>', text)

                    yield doc_id, text

    # TODO(cvangysel): merge iter_document_multiprocessing and iter_documents.
    # However, there are users of iter_documents that implement multiprocessing
    # themselves. Therefore, we should be careful when doing this.
    def iter_document_multiprocessing(self, num_workers=1,
                                      replace_digits=True, strip_html=True,
                                      tokenize=False, ignore_words=set(),
                                      document_ids=set()):
        assert num_workers >= 1

        document_ids = set(document_ids) if document_ids else set()

        if not tokenize:
            assert not ignore_words, \
                'ignore_words should only be set if ' \
                'tokeniziation is requested.'

        result_q = multiprocessing.Queue()

        pool = multiprocessing.Pool(
            num_workers,
            initializer=(
                _iter_trectext_documents_multiprocessing_worker_initializer),
            initargs=[result_q,
                      replace_digits, strip_html,
                      tokenize, ignore_words,
                      document_ids,
                      self.encoding])

        worker_result = pool.map_async(
            _iter_trectext_documents_multiprocessing_worker,
            self.document_paths)

        # We will not submit any more tasks to the pool.
        pool.close()

        it = multiprocessing_utils.QueueIterator(
            pool, worker_result, result_q)

        result_idx = 0

        while True:
            try:
                result = next(it)

                result_idx += 1
            except StopIteration:
                break

            yield result


class ShardedTRECTextWriter(object):

    def __init__(self, base, shard_size, encoding='ascii'):
        self.base = base
        self.shard_size = shard_size
        self.encoding = encoding

        self.current = None

        self.flush()

    def flush(self):
        if self.current is not None:
            (id, f) = self.current

            f.close()
        else:
            id = 0

        id += 1

        path = '{0}_{1}.trectext'.format(self.base, id)
        if os.path.exists(path):
            raise RuntimeError('Output shard already exists.')

        logging.info('Writing shard %s', path)

        if self.encoding is not None:
            f = io_utils.open(path, 'w', encoding=self.encoding)
        else:
            f = io_utils.open(path, 'wb')

        self.current = (id, f)
        self.current_count = 0

    def close(self):
        assert self.current is not None

        self.current[1].close()

    def _write(self, data):
        if self.encoding is None and isinstance(data, str):
            self.current[1].write(data.encode(self.encoding))
        elif self.encoding is not None and isinstance(data, bytes):
            self.current[1].write(data.decode(self.encoding))
        else:
            self.current[1].write(data)

    def write_document(self, doc_id, doc_text):
        self._write('<DOC>\n')
        self._write('<DOCNO>{0}</DOCNO>\n'.format(doc_id))
        self._write('<TEXT>\n')
        self._write(doc_text)
        self._write('\n')
        self._write('</TEXT>\n')
        self._write('</DOC>\n')

        self.current_count += 1

        if self.current_count % self.shard_size == 0:
            self.flush()


class EntityDocumentAssociations(object):

    def __init__(self, f, document_ids=None, max_unique_entities=False):
        self.entities = set()

        self.entities_per_document = collections.defaultdict(set)
        self.documents_per_entity = collections.defaultdict(set)

        self.max_entities_per_document = 0
        self.num_associations = 0

        for entity_id, document_id, _ in (
                assoc.strip().split() for assoc in f):
            if document_ids is not None and document_id not in document_ids:
                continue

            # If we only want to keep track of a maximum number of entities.
            if max_unique_entities:
                # Check if we already know the entity, if so, carry on.
                if entity_id in self.entities:
                    pass
                # If not, verify if we are still below the maximum. If not,
                # jump to next line.
                elif len(self.entities) >= max_unique_entities:
                    continue

            self.entities.add(entity_id)

            self.entities_per_document[document_id].add(entity_id)
            self.max_entities_per_document = max(
                self.max_entities_per_document,
                len(self.entities_per_document[document_id]))

            self.documents_per_entity[entity_id].add(document_id)

            self.num_associations += 1


class IdentityDocumentAssociations(object):

    def __init__(self, document_ids):
        self.entities = set(document_ids)

        self.entities_per_document = {
            document_id: set([document_id])
            for document_id in self.entities
        }

        self.documents_per_entity = {
            document_id: set([document_id])
            for document_id in self.entities
        }

        self.max_entities_per_document = 1
        self.num_associations = len(self.entities)


def parse_topics(file_or_files,
                 max_topics=sys.maxsize, delimiter=';'):
    assert max_topics >= 0 or max_topics is None

    topics = collections.OrderedDict()

    if not isinstance(file_or_files, list) and \
            not isinstance(file_or_files, tuple):
        if not isinstance(file_or_files, io.IOBase):
            file_or_files = list(file_or_files)
        else:
            file_or_files = [file_or_files]

    for f in file_or_files:
        assert isinstance(f, io.IOBase), type(f)

        for line in f:
            assert(isinstance(line, str))

            line = line.strip()

            if not line:
                continue

            try:
                topic_id, terms = line.split(delimiter, 1)
            except ValueError:
                logging.warning('Unable to process "%s" in topics list.',
                                line)

                continue

            if topic_id in topics and (topics[topic_id] != terms):
                logging.error('Duplicate topic "%s" (%s vs. %s).',
                              topic_id,
                              topics[topic_id],
                              terms)

            topics[topic_id] = terms

            if max_topics > 0 and len(topics) >= max_topics:
                break

    return topics


def parse_trec_eval(f):
    measure_re = re.compile(r'^([A-Za-z0-9_\-]+)\s+(.*)\s+([0-9\.e\-]+)$')

    trec_eval = collections.defaultdict(dict)

    for line in f:
        result = measure_re.match(line)

        if result:
            measure, topic, value = \
                result.group(1), result.group(2), result.group(3)

            if measure in trec_eval[topic]:
                raise RuntimeError()

            trec_eval[topic][measure] = float(value)

    return trec_eval


def compute_significance(first_trec_eval, second_trec_eval, measures):
    topics = set(first_trec_eval.keys())
    topics = topics.intersection(set(second_trec_eval))

    if 'all' in topics:
        topics.remove('all')

    significance_results = {}

    for measure in measures:
        a = []
        b = []

        for topic in topics:
            a.append(first_trec_eval[topic][measure])
            b.append(second_trec_eval[topic][measure])

        statistic, p_value = scipy.stats.ttest_rel(a, b)

        logging.debug('P-value for %s=%.4f (sum of differences=%.4f).',
                      measure, p_value, (np.array(a) - np.array(b)).sum())

        significance_results[measure] = p_value

    return significance_results


def parse_run(f_run, max_depth=None):
    run = {}

    current_query_id = None

    for line in f_run:
        query_id, _, object_id, ranking, score, _ = line.strip().split()
        score = float(score)

        if current_query_id != query_id:
            assert query_id not in run
            run[query_id] = {}

            current_query_id = query_id
            prev_min_score = np.inf
        else:
            assert score <= prev_min_score
            prev_min_score = score

        assert object_id not in run[query_id]

        if max_depth and len(run[query_id]) >= max_depth:
            continue

        run[query_id][object_id] = score

    return run


def parse_trec_run(f, return_score=False,
                   ignore_duplicates=False,
                   ignore_parse_errors=False):
    run = collections.defaultdict(collections.OrderedDict)

    for idx, line in enumerate(f):
        if not line.strip():  # Skip empty lines.
            continue

        try:
            data = line.strip().split()

            # In some old runs, the 7th field contained
            # relevance feedback labels.
            if len(data) not in (6, 7):
                raise ValueError()

            topic_id, _, candidate, ranking, score, _ = data[:6]
        except ValueError as e:
            logging.error('Encountered parsing error at line %d (%s).',
                          idx + 1, line.strip())

            if not ignore_parse_errors:
                raise e

        if not ignore_duplicates:
            assert candidate not in run[topic_id], (
                topic_id, candidate)
        elif ignore_duplicates and candidate in run[topic_id]:
            logging.warning('Candidate %s occurs at least twice '
                            'in topic %s at rank %s.',
                            candidate, topic_id, ranking)

        if not return_score:
            run[topic_id][candidate] = float(ranking)
        else:
            run[topic_id][candidate] = float(score)

    return run


class TRECRun(object):

    def __init__(self):
        self.data = collections.defaultdict(dict)

    def add_score(self, subject_id, entity_id, score):
        assert isinstance(subject_id, str)
        assert isinstance(entity_id, str)

        assert entity_id not in self.data[subject_id]

        self.data[subject_id][entity_id] = float(score)

    def write_run(self, model_name, out_f,
                  max_objects_per_query=sys.maxsize):
        data = {}

        for subject_id in self.data:
            data[subject_id] = [
                (score, entity_id)
                for entity_id, score in self.data[subject_id].items()]

        write_run(model_name, data, out_f, max_objects_per_query)


def write_run(model_name, data, out_f,
              max_objects_per_query=sys.maxsize,
              skip_sorting=False):
    return write_ranking(
        model_name, data, out_f, max_objects_per_query, skip_sorting,
        '{subject} Q0 {object} {rank} {relevance:.40f} {model_name}\n')


def write_qrel(model_name, data, out_f,
               max_objects_per_query=sys.maxsize,
               skip_sorting=False):
    return write_ranking(
        None, data, out_f, max_objects_per_query, skip_sorting,
        '{subject} 0 {object} {relevance}\n')


class OnlineTRECRun(object):

    def __init__(self, name, rank_cutoff=sys.maxsize, write_fn=write_run):
        self.name = name
        self.rank_cutoff = rank_cutoff

        self.write_fn = write_fn

        self.tmp_file = tempfile.NamedTemporaryFile(
            mode='w', delete=False)

        logging.info('Writing temporary run to %s.',
                     self.tmp_file.name)

    def add_ranking(self, subject_id, object_assesments):
        assert self.tmp_file

        self.write_fn(self.name,
                      data={subject_id: object_assesments},
                      out_f=self.tmp_file,
                      max_objects_per_query=self.rank_cutoff)

    def close_and_return_temporary_path(self):
        assert self.tmp_file

        tmp_file_path = self.tmp_file.name

        self.tmp_file.close()

        return tmp_file_path

    def close_and_write(self, out_path, overwrite=True):
        if not overwrite:
            assert not os.path.exists(out_path)
        elif os.path.exists(out_path):
            logging.warning('Overwriting run %s.', out_path)

        tmp_file_path = self.close_and_return_temporary_path()
        shutil.copy(tmp_file_path, out_path)

        os.remove(tmp_file_path)

    def close_and_evaluate(self, qrel):
        assert self.tmp_file

        assert isinstance(qrel, OnlineTRECRun) and qrel.write_fn == write_qrel
        assert qrel.tmp_file

        qrel_tmp_file_path = qrel.tmp_file.name
        run_tmp_file_path = self.tmp_file.name

        qrel.tmp_file.close()
        self.tmp_file.close()

        trec_eval = evaluate(run_tmp_file_path, qrel_tmp_file_path)

        os.remove(qrel_tmp_file_path)
        os.remove(run_tmp_file_path)

        return trec_eval


def evaluate(run_path, qrel_path):
    assert os.path.exists(run_path)
    assert os.path.exists(qrel_path)

    command = ['trec_eval -q -m all_trec {} {}'.format(qrel_path, run_path)]
    out = subprocess.check_output(command, shell=True)
    return parse_trec_eval(out.decode('ascii').split('\n'))


def write_ranking(model_name, data, out_f,
                  max_objects_per_query,
                  skip_sorting,
                  format):
    """
    Write a run to an output file.

    Parameters:
        - model_name: identifier of run.
        - data: dictionary mapping topic_id to object_assesments;
            object_assesments is an iterable (list or tuple) of
            (relevance, object_id) pairs.

            The object_assesments iterable is sorted by decreasing order.
        - out_f: output file stream.
        - max_objects_per_query: cut-off for number of objects per query.
    """
    for subject_id, object_assesments in data.items():
        if not object_assesments:
            logging.warning('Received empty ranking for %s; ignoring.',
                            subject_id)

            continue

        # Probe types, to make sure everything goes alright.
        # assert isinstance(object_assesments[0][0], float) or \
        #     isinstance(object_assesments[0][0], np.float32)
        assert isinstance(object_assesments[0][1], str) or \
            isinstance(object_assesments[0][1], bytes)

        if not skip_sorting:
            object_assesments = sorted(object_assesments, reverse=True)

        if max_objects_per_query < sys.maxsize:
            object_assesments = object_assesments[:max_objects_per_query]

        if isinstance(subject_id, bytes):
            subject_id = subject_id.decode('utf8')

        for rank, (relevance, object_id) in enumerate(object_assesments):
            if isinstance(object_id, bytes):
                object_id = object_id.decode('utf8')

            out_f.write(format.format(
                subject=subject_id,
                object=object_id,
                rank=rank + 1,
                relevance=relevance,
                model_name=model_name))
