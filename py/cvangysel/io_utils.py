import bs4
import codecs
import collections
import itertools
import io
import logging
import multiprocessing
import numpy as np
import os
import re
import sys
import unicodedata
import html.parser as HTMLParser

parser = HTMLParser.HTMLParser()

Word = collections.namedtuple('Word', ['id', 'count'])


def tokenize_text(text, ignore_words=set()):
    assert(isinstance(text, str) or isinstance(text, bytes))

    return tuple(
        token_stream(
            lowercased_stream(
                filter_non_latin_stream(
                    filter_non_alphanumeric_stream(
                        iter(text)))),
            eos_chars=[], ignore_words=ignore_words))


def filter_non_ascii(data):
    return str(''.join(char for char in data if ord(char) < 128))


def character_stream(file_stream, limit=None, encoding='latin1'):
    file_size = os.fstat(file_stream.fileno()).st_size

    if limit is not None:
        file_size = min(file_size, limit)

    while file_stream.tell() < file_size:
        try:
            char = file_stream.read(1)
        except UnicodeDecodeError as e:
            logging.warning(e)

        if not char:
            logging.error('Encountered exhausted file stream before EOF.')

            break

        if isinstance(char, bytes):
            try:
                char = char.decode(encoding)
            except UnicodeDecodeError:
                logging.error('Encountered UnicodeDecodeError for '
                              'parsing character "%d".', ord(char))

        # Introduce white space before certain characters, such that
        # tokenisation is done properly later down the line.
        if char in ('<',):
            yield ' '

        yield char

        # Introduce white space after certain characters; see above.
        if char in ('>',):
            yield ' '


def filter_non_latin_stream(character_stream):
    latin_letters_cache = {}

    def _filter_non_latin(character):
        if character.isspace():
            return True

        if character not in latin_letters_cache:
            character_name = unicodedata.name(character)

            latin_letters_cache[character] = \
                'LATIN' in character_name or \
                'SIGN' in character_name

        return latin_letters_cache[character]

    return filter(_filter_non_latin, character_stream)


def filter_non_alphanumeric_stream(character_stream):
    allowed_chars = set(['<', '/', '>'])

    return filter(
        lambda char: (
            char.isalnum() or char.isspace() or char in allowed_chars),
        character_stream)


def token_stream(unicode_stream, delimiters=(' ', '\t', '\n'),
                 eos_chars=['\n'], eos_token='</s>',
                 ignore_words=[]):
    delimiters = set(delimiters)
    eos_chars = set(eos_chars)

    ignore_words = set(ignore_words)

    buff = io.StringIO()

    while True:
        try:
            char = next(unicode_stream)
        except StopIteration:
            break

        if char in eos_chars or char in delimiters:
            value = buff.getvalue()
            if value:
                if not ignore_words or (
                        ignore_words and value not in ignore_words):
                    yield value

            buff = io.StringIO()

            if char in eos_chars:
                yield eos_token
        else:
            buff.write(char)

    remainder = buff.getvalue()
    if remainder:
        if not ignore_words or (
                ignore_words and remainder not in ignore_words):
            yield remainder

        if eos_chars:
            yield eos_token


def lowercased_stream(iterable):
    return (s.lower() for s in iterable)


def translated_token_stream(iterable, words):
    for word in iterable:
        if word in words:
            yield words[word].id


def windowed_translated_token_stream(iterable, window_size, words,
                                     eos_chars=['\n'], eos_token='</s>',
                                     stride=1, padding_token=None,
                                     callback=None):
    assert eos_token in words
    assert padding_token in words or padding_token is None
    assert stride >= 1 and stride <= window_size

    buffer = collections.deque(maxlen=window_size)

    num_yielded_windows = 0

    def probe():
        while True:
            try:
                word = next(iterable)
            except StopIteration:
                return False

            # If </S> is encountered, clear buffer and return to caller.
            if word in eos_chars or word == eos_token:
                buffer.clear()

                return True

            if word in words:
                buffer.append(word)

                return True

    def prepare_buffer():
        return tuple(words[word].id for word in buffer)

    while True:
        # If buffer below capacity, fill-up until full.
        new_data = False

        while len(buffer) < window_size:
            if not probe():
                # Add padding to the current buffer, if requested.
                if padding_token is not None and len(buffer) > 0 and new_data:
                    while len(buffer) < window_size:
                        buffer.append(padding_token)

                    yield prepare_buffer()
                    num_yielded_windows += 1

                if hasattr(callback, '__call__'):
                    callback(num_yielded_windows, tuple(buffer))

                return
            else:
                new_data = True

        yield prepare_buffer()
        num_yielded_windows += 1

        # Remove number of elements equal to the stride.
        [buffer.popleft() for _ in range(stride)]

        assert len(buffer) == (window_size - stride)


def replace_numeric_tokens_stream(iterable, placeholder_token='<num>'):
    _digits = re.compile('\d')

    for token in iterable:
        if _digits.search(token):
            yield placeholder_token
        else:
            yield token


def downsample_tokens_stream(iterable,
                             num_word_impressions,
                             words,
                             sample_threshold,
                             callback=None):
    if callback is not None:
        assert hasattr(callback, '__call__')

    num_tokens = 0
    num_discarded_tokens = 0

    abs_threshold_freq = float(sample_threshold) * num_word_impressions

    for word in iterable:
        if word not in words:
            continue

        num_tokens += 1

        abs_word_frequency = words[word].count

        # Copy-pasta from word2vec source:
        #    https://word2vec.googlecode.com/svn/trunk/word2vec.c at line 396.
        #
        # This boils down to:
        #   sqrt(word_freq / threshold_freq) *
        #           sqrt(threshold_freq / word_freq)^2
        #   = sqrt(threshold_freq / word_freq)
        #
        # where the frequencies are in absolute counts.
        word_retain_prob = (
            np.sqrt(float(abs_word_frequency) / abs_threshold_freq) + 1.0) * (
            abs_threshold_freq / float(abs_word_frequency))

        if word_retain_prob < 1.0:
            if word_retain_prob < np.random.rand():
                num_discarded_tokens += 1

                continue

        yield word

    if callback is not None:
        callback(num_tokens, num_discarded_tokens)


def extract_vocabulary_worker(payload):
    filename, idx, num_chunks, params = payload

    if params['encoding'] != 'ascii' and num_chunks != 1:
        raise NotImplementedError('We do not yet support chunking files '
                                  'if multi-byte encoding are used.')

    numerical_placeholder_token = (
        params['numerical_placeholder_token']
        if 'numerical_placeholder_token' in params else False)

    min_word_size = (
        params['min_word_size'] if 'min_word_size' in params else 0)

    logging.debug('I am worker with id %d (total=%d) reading %s.',
                  idx, num_chunks, filename)

    # idx should be zero-indexed.
    assert idx < num_chunks

    f = open(filename, 'r', encoding=params.get('encoding', None))
    file_size = os.fstat(f.fileno()).st_size

    chunk_size = file_size // num_chunks

    start_position = idx * chunk_size

    if idx == (num_chunks - 1):
        end_position = file_size
    else:
        end_position = (idx + 1) * chunk_size

    logging.debug('[%s:%d] Reading from %d to %d (file size=%d).',
                  filename, idx, start_position, end_position, file_size)

    # Set file marker.
    f.seek(start_position)

    # Read current batch of characters.
    char_stream = character_stream(f, limit=end_position)

    word_stream = token_stream(lowercased_stream(
        filter_non_latin_stream(
            filter_non_alphanumeric_stream(
                itertools.chain(*char_stream)))))

    # Count words.
    word_counts = collections.defaultdict(int)
    num_words = 0

    for word in word_stream:
        if len(word) < min_word_size:
            continue

        if numerical_placeholder_token and word.isdigit():
            word = numerical_placeholder_token

        word_counts[word] += 1
        num_words += 1

    f.close()

    logging.info('[%s:%d] Done.', filename, idx)

    return num_words, word_counts


def extract_vocabulary(filenames, encoding,
                       min_count=-1, max_vocab_size=-1, min_word_size=1,
                       eos_token='</s>', numerical_placeholder_token='<num>',
                       ignore_tokens=(),
                       num_workers=1):
    ignore_tokens = set(ignore_tokens)

    logging.info('Extracting vocabulary from %d corpora using %d worker(s).',
                 len(filenames), num_workers)

    num_chunks = max(num_workers // len(filenames), 1)

    params = {
        'numerical_placeholder_token': numerical_placeholder_token,
        'min_word_size': min_word_size,
        'encoding': encoding,
    }

    payloads = [(filename, idx, num_chunks, params) for filename in filenames
                for idx in range(num_chunks)]

    logging.debug('Multiprocessing payloads: %s.', payloads)

    def _aggregate_results(results):
        # Aggregate words.
        word_counts = collections.defaultdict(int)
        num_words = 0

        for chunk_num_words, chunk_word_counts in results:
            logging.info('Worker observed %d words (%d unique).',
                         chunk_num_words, len(chunk_word_counts))

            for word, count in chunk_word_counts.items():
                if len(word) < min_word_size:
                    continue

                word_counts[word] += count
                num_words += count

        return num_words, word_counts

    if num_workers > 1:
        pool = multiprocessing.Pool(num_workers)

        results = pool.imap_unordered(extract_vocabulary_worker, payloads)
        num_words, word_counts = _aggregate_results(results)

        pool.close()
    else:
        results = (extract_vocabulary_worker(payload) for payload in payloads)
        num_words, word_counts = _aggregate_results(results)

    word_counts = [(word, count) for word, count in word_counts.items()
                   if word not in ignore_tokens]

    num_unique_words = len(word_counts)

    logging.info('Observed %d words (of which %d unique).',
                 num_words, num_unique_words)

    # Remove words with low counts.
    if min_count >= 1:
        logging.info('Filtering words that occur less than %d times.',
                     min_count)

        word_counts = filter(lambda word: word[1] >= min_count, word_counts)

    word_counts = sorted(word_counts, key=lambda x: x[1], reverse=True)

    if max_vocab_size >= 1:
        word_counts = word_counts[:max_vocab_size]

    words = dict((word, Word(idx, count))
                 for idx, (word, count) in enumerate(word_counts))
    tokens = [word for word, _ in word_counts]

    # Make </S> known to dictionary.
    if eos_token not in words:
        logging.info(
            'End-of-sentence token "%s" not found in vocabulary.',
            eos_token)

        eos_id = len(words)
        tokens.append(eos_token)
        words[eos_token] = Word(eos_id, 0)
    else:
        logging.info(
            'End-of-sentence token "%s" with statistics %s.',
            eos_token, words[eos_token])

    if numerical_placeholder_token not in words:
        logging.info(
            'Numerical placeholder token "%s" not found in vocabulary.',
            numerical_placeholder_token)
    else:
        logging.info(
            'Numerical placeholder token "%s" with statistics %s.',
            numerical_placeholder_token, words[numerical_placeholder_token])

    logging.info('Retained %d unique words.', len(words))

    assert len(words) == len(tokens)

    return words, tokens


def recursively_decode_html_entities(text):
    old_text = None

    while old_text != text:
        old_text = text
        text = parser.unescape(text)

    return text


def strip_html(html, include_metatags=True):
    try:
        html = recursively_decode_html_entities(html)
    except:
        e = sys.exc_info()[0]

        logging.warning(
            'Exception during recursively_decode_html_entities: %s', e)

    try:
        soup = bs4.BeautifulSoup(html, 'lxml')
    except:
        return None

    # Remove javascript.
    [s.extract() for s in soup('script')]

    # Remove css.
    [s.extract() for s in soup('style')]

    content = []

    # First, extract meta tags.
    if include_metatags:
        content.extend(
            meta['content'] for meta in soup('meta')
            if 'content' in meta)

    # Add text content from the page.
    content.append(soup.get_text(' ', strip=True))

    return ' '.join(content)
