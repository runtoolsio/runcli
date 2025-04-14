import os


def expand_user(file):
    if not isinstance(file, str) or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


def print_file(path):
    path = expand_user(path)
    print('Showing file: ' + str(path))
    with open(path, 'r') as file:
        print(file.read())
