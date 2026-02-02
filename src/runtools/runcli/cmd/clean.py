from runtools.runcore import paths
from runtools.runcore.listening import TRANSITION_LISTENER_FILE_EXTENSION, OUTPUT_LISTENER_FILE_EXTENSION
from runtools.runcore.util.socket import clean_stale_sockets
from runtools.runjob.server import RPC_FILE_EXTENSION


def run(args):
    # TODO Disable socket log messages
    clean_socket(RPC_FILE_EXTENSION, "RPC")
    clean_socket(TRANSITION_LISTENER_FILE_EXTENSION, "transition listeners")
    clean_socket(OUTPUT_LISTENER_FILE_EXTENSION, "output listeners")


def clean_socket(file_extension, socket_group):
    cleaned = clean_stale_sockets(paths.socket_files_provider(file_extension))
    print(f"Cleaned sockets for {socket_group}: {len(cleaned)}")
    for c in cleaned:
        print('  cleaned: ' + str(c))
