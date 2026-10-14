#Write the required timestamped events to log_peer_<our ID>.log.
import os
import threading

class PeerLogger:
    def __init__(self, local_id, working_directory):
        #TODO: Open the log in working_directory. Use a lock for concurrent writes.
        self.local_id = local_id
        log_path = os.path.join(working_directory, f"log_peer_{local_id}.log")

        self.file = open(log_path, "a")
        self.lock = threading.Lock()

    def log(self, event, **fields):
        #TODO: Write the specification's wording with date/hour/minute/second.
        #Current calls use these event names and fields:
        #connection: remote_id, outgoing (True = we connected to them)
        #have: remote_id, piece_index
        #interested / not_interested / choke / unchoke: remote_id
        #Later: preferred (peer_ids), optimistic (remote_id), complete (no fields),
        #downloaded_piece (remote_id, piece_index, piece_count).
        raise NotImplementedError("Log writing is not implemented yet")

    def close(self):
        #TODO: Flush and close the file after networking threads finish.
        raise NotImplementedError("Logger cleanup is not implemented yet")
