#Write the required timestamped events to log_peer_<our ID>.log.


class PeerLogger:
    def __init__(self, local_id, working_directory):
        #TODO: Open the log in working_directory. Use a lock for concurrent writes.
        raise NotImplementedError("Logger is not implemented yet")

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
