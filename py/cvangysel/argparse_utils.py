import argparse
import os


def positive_int(value):
    try:
        value = int(value)
        assert value >= 0

        return value
    except:
        raise argparse.ArgumentTypeError(
            '"{0}" is not a valid positive int'.format(value))


def positive_float(value):
    try:
        value = float(value)
        assert value > 0.0

        return value
    except:
        raise argparse.ArgumentTypeError(
            '"{0}" is not a valid positive float'.format(value))


def ratio(value):
    try:
        value = float(value)
        assert value >= 0.0 and value <= 1.0

        return value
    except:
        raise argparse.ArgumentTypeError(
            '"{0}" is not a valid ratio'.format(value))


def existing_directory_path(value):
    value = str(value)

    if os.path.isdir(value):
        return value
    else:
        raise argparse.ArgumentTypeError(
            '"{0}" is not a directory'.format(value))


def nonexisting_directory_path(value):
    value = str(value)

    if not os.path.exists(value) and not os.path.isdir(value):
        return value
    else:
        raise argparse.ArgumentTypeError(
            '"{0}" already exists.'.format(value))


def existing_file_path(value):
    value = str(value)

    if os.path.exists(value):
        return value
    else:
        raise argparse.ArgumentTypeError(
            'File "{0}" does not exists.'.format(value))


def nonexisting_file_path(value):
    value = str(value)

    if not os.path.exists(value):
        return value
    else:
        raise argparse.ArgumentTypeError(
            'File "{0}" already exists.'.format(value))


def bytes(encoding):
    def _formatter(value):
        return value.encode(encoding)

    return _formatter
