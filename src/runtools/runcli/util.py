import os


def expand_user(file):
    if not isinstance(file, str) or not file.startswith('~'):
        return file

    return os.path.expanduser(file)
