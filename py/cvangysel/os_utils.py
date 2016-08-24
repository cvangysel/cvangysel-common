import fnmatch
import os


def glob(base, pattern):
    assert os.path.isdir(base)

    base = os.path.abspath(base)

    candidates = []

    for root, dirnames, filenames in os.walk(base):
        for filename in filenames:
            qualified_path = os.path.join(root, filename)
            subpath = qualified_path[len(base):].strip('/')

            if fnmatch.fnmatchcase(subpath, pattern):
                candidates.append(qualified_path)

    return candidates


def pick_gpu_device():
    if 'CUDA_VISIBLE_DEVICES' not in os.environ:
        raise RuntimeError(
            'No CUDA devices visible to process. '
            'Make sure the CUDA_VISIBLE_DEVICES environment '
            'variable is set accordingly.')
    else:
        gpu_devices = map(
            int, os.environ['CUDA_VISIBLE_DEVICES'].split(','))

        return gpu_devices.pop()
