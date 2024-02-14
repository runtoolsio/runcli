from runtools import runner
from runtools.runcore import paths

from runtools.runner import api, events


def run(args):
    # TODO Disable socket log messages
    clean_socket(api.API_FILE_EXTENSION, "API")
    clean_socket(events.TRANSITION_LISTENER_FILE_EXTENSION, "transition listeners")
    clean_socket(events.OUTPUT_LISTENER_FILE_EXTENSION, "output listeners")


def clean_socket(file_extension, socket_group):
    cleaned = runner.clean_stale_sockets(paths.socket_files(file_extension))
    print(f"Cleaned sockets for {socket_group}: {len(cleaned)}")
    for c in cleaned:
        print('  cleaned: ' + str(c))
