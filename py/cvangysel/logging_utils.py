import logging
import os
import socket
import subprocess
import sys


def get_formatter():
    return logging.Formatter(
        '%(asctime)s [%(threadName)s.{}] '
        '[%(name)s] [%(levelname)s]  '
        '%(message)s'.format(get_hostname()))


def configure_logging(args, output_path=None):
    loglevel = args.loglevel if hasattr(args, 'loglevel') else 'INFO'

    # Set logging level.
    numeric_log_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_log_level, int):
        raise ValueError('Invalid log level: %s' % loglevel.upper())

    # Configure logging level.
    logging.basicConfig(level=numeric_log_level)
    logging.getLogger().setLevel(numeric_log_level)

    # Set-up log formatting.
    log_formatter = get_formatter()

    for handler in logging.getLogger().handlers:
        handler.setFormatter(log_formatter)

    if output_path is not None:
        # Sanity-check model output.
        if os.path.exists('{0}.log'.format(output_path)):
            logging.error('Model output already exists.')

            raise IOError()

        # Output to file.
        file_handler = logging.FileHandler('{0}.log'.format(output_path))
        file_handler.setFormatter(log_formatter)
        logging.getLogger().addHandler(file_handler)

    logging.info('Arguments: %s', args)
    logging.info('Git revision: %s', get_git_revision_hash())


def log_module_info(*modules):
    for module in modules:
        logging.info('%s version: %s (%s)',
                     module.__name__,
                     module.__version__,
                     module.__path__)


def get_git_revision_hash():
    try:
        proc = subprocess.Popen(
            ['git', 'rev-parse', 'HEAD'],
            stdout=subprocess.PIPE,
            cwd=os.path.dirname(os.path.realpath(sys.path[0] or __file__)))

        return proc.communicate()[0].strip()
    except:
        return None


def get_hostname():
    return socket.gethostbyaddr(socket.gethostname())[0]
