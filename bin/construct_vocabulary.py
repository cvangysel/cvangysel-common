#!/usr/bin/env python

import sys

from cvangysel import argparse_utils, io_utils, logging_utils, nltk_utils

import argparse
import logging
import pickle


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel', type=str, default='INFO')

    parser.add_argument('document_paths',
                        type=argparse_utils.existing_file_path, nargs='+')

    parser.add_argument('--encoding', type=str, default='latin1')

    parser.add_argument('--vocabulary_min_count', type=int, default=2)
    parser.add_argument('--vocabulary_min_word_size', type=int, default=2)
    parser.add_argument('--vocabulary_max_size', type=int, default=65536)

    parser.add_argument('--include_stopwords',
                        action='store_true', default=False)

    parser.add_argument('--num_workers',
                        type=argparse_utils.positive_int, default=8)

    parser.add_argument('--dictionary_out', required=True)
    parser.add_argument('--humanreadable_dictionary_out', default=None)

    args = parser.parse_args()

    try:
        logging_utils.configure_logging(args)
    except IOError:
        return -1

    ignore_words = set()

    if not args.include_stopwords:
        ignore_words.update(nltk_utils.get_stopwords())

    logging.info('Constructing vocabulary.')

    vocabulary = io_utils.construct_vocabulary(
        args.document_paths,
        num_workers=args.num_workers,
        min_count=args.vocabulary_min_count,
        min_word_size=args.vocabulary_min_word_size,
        max_vocab_size=args.vocabulary_max_size,
        ignore_tokens=ignore_words,
        encoding=args.encoding)

    logging.info('Pickling vocabulary.')

    with open(args.dictionary_out, 'wb') as f_out:
        pickle.dump(vocabulary, f_out, pickle.HIGHEST_PROTOCOL)

    if args.humanreadable_dictionary_out is not None:
        with open(args.humanreadable_dictionary_out, 'w',
                  encoding=args.encoding) as f_out:
            for token, token_meta in vocabulary.iteritems():
                f_out.write('{} {}\n'.format(token, token_meta.id))

if __name__ == "__main__":
    sys.exit(main())
