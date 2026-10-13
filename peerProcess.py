#This is basically our main.py, it parses the config, coordinates connections, shared state, and shutdown.

import argparse
import socket
import sys
import threading
from pathlib import Path

from peer import MESSAGE_TYPES, Peer, ProtocolError, exchange_handshake


class PeerProcess:
    def __init__(self, local_id, common, peer_rows, logger, working_directory):
        self.local_id = local_id #id passed in command prompt
        self.common = common #common config file
        self.logger = logger #logger object
        self.working_directory = Path(working_directory)
        self.ordered_ids = [row["peer_id"] for row in peer_rows]
        self.local_position = self.ordered_ids.index(local_id)
        self.local_info = peer_rows[self.local_position]
        self.earlier_ids = self.ordered_ids[:self.local_position]
        self.later_ids = set(self.ordered_ids[self.local_position + 1:]) #made this a set b/c it's quicker lookup when checking incoming handshake

        self.file_size = common["FileSize"]
        self.piece_size = common["PieceSize"]
        self.piece_count = (self.file_size + self.piece_size - 1) // self.piece_size #int division rounded up
        self.complete_file_acquired = self.local_info["has_file"]
        self.local_bitfield = [self.complete_file_acquired] * self.piece_count
        self.peers = {
            row["peer_id"]: Peer(
                row["peer_id"], row["hostname"], row["port"], row["has_file"],
                self.piece_count, self.file_size, self.piece_size)
            for row in peer_rows if row["peer_id"] != local_id
        }

        self.state_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.listener = None #listening socket
        self.accept_thread = None # ^ the thread calling accept() on that socket
        self._workers = []
        self._pending_sockets = set() #sockets still connecting/handshaking before being assigned to a Peer object
        self._lifecycle_lock = threading.Lock() #Protect connection workers during shutdown

        #TODO later: scheduler, piece selection/storage, and completion detection.

def main():
    arguments = argparse.ArgumentParser(description="P2P midpoint peer process")
    arguments.add_argument("peer_id", type=int)
    args = arguments.parse_args()
    working_directory = Path.cwd()
    log = None
    try:
        # TODO (config): implement these agreed interfaces in config.py
        from config import load_common, load_peers
        common = load_common(working_directory / "Common.cfg")
        peer_rows = load_peers(working_directory / "PeerInfo.cfg")

        # TODO (logger): implement this interface in logger.py
        from logger import PeerLogger
        log = PeerLogger(args.peer_id, working_directory)
        process = PeerProcess(args.peer_id, common, peer_rows, log, working_directory)
        process.run()
    except (OSError, ValueError, NotImplementedError) as error:
        print(f"Startup failed: {error}")
        return 1
    finally:
        if log is not None:
            log.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
