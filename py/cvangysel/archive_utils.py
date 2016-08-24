import logging
import os
import shutil
import subprocess
import tempfile


class Extract7zArchive(object):

    """
        Usage example:

        with archive_utils.Extract7zArchive(archive_path) as uncompressed_path:
            ....
    """

    def __init__(self, path):
        assert os.path.exists(path)

        self.path = path

    def __enter__(self):
        if not os.path.isdir(self.path):
            self.tmp_dir = tempfile.mkdtemp()

            command = ['7z', 'e', self.path, '-o{0}'.format(self.tmp_dir)]

            logging.info('Extracting %s (%s).', self.path, command)

            ret = subprocess.call(command)
            assert ret == 0

            return self.tmp_dir
        else:
            logging.info('Using pre-extracted directory %s.', self.path)

            return self.path

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, 'tmp_dir'):
            logging.info('Removing temporary directory %s.', self.tmp_dir)

            shutil.rmtree(self.tmp_dir)
