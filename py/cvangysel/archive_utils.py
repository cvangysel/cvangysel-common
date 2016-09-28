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


class PackedFile(object):

    def __init__(self, filename, encoding):
        assert os.path.exists(filename)

        self.filename = filename
        self.encoding = encoding

        _, self.tmp_file = tempfile.mkstemp()

        proc = subprocess.Popen(['pigz', '-dz', '-c', self.filename],
                                stdout=subprocess.PIPE)

        with open(self.tmp_file, 'wb') as f_tmp:
            while True:
                line = proc.stdout.readline()
                if line:
                    f_tmp.write(line)
                else:
                    break

        self.f = open(self.tmp_file, 'r', encoding=self.encoding)

        for attr in ('seek', 'tell', 'read', 'fileno'):
            setattr(self, attr, getattr(self.f, attr))

    def __enter__(self):
        return self.f.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.f.close()

        for attr in ('__enter__', '__exit__', 'seek', 'tell', 'read', 'fileno'):
            setattr(self, attr, None)

        os.remove(self.tmp_file)
