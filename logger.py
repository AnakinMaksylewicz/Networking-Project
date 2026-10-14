#Write the required timestamped events to log_peer_<our ID>.log.
import os
import threading
import datetime

class PeerLogger:
    def __init__(self, local_id, working_directory):
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
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if event == "connection":
            remote_id = fields["remote_id"]
            outgoing = fields["outgoing"]
            if outgoing:
                message = f"Peer {self.local_id} makes a connection to Peer {remote_id}."
            else:
                message = f"Peer {self.local_id} is connected from Peer {remote_id}."

        elif event == "have":
            remote_id = fields["remote_id"]
            piece_index = fields["piece_index"]
            message = f"Peer {self.local_id} received the 'have' message from {remote_id} for the piece {piece_index}."

        elif event == "interested":
            remote_id = fields["remote_id"]
            message = f"Peer {self.local_id} received the 'interested' message from {remote_id}."

        elif event == "not_interested":
            remote_id = fields["remote_id"]
            message = f"Peer {self.local_id} received the 'not interested' message from {remote_id}."

        elif event == "choke":
            remote_id = fields["remote_id"]
            message = f"Peer {self.local_id} is choked by {remote_id}."

        elif event == "unchoke":
            remote_id = fields["remote_id"]
            message = f"Peer {self.local_id} is unchoked by {remote_id}."

        elif event == "preferred":
            peer_ids = fields["peer_ids"]
            id_list = ",".join(str(p) for p in peer_ids)
            message = f"Peer {self.local_id} has the preferred neighbors {id_list}."

        elif event == "optimistic":
            remote_id = fields["remote_id"]
            message = f"Peer {self.local_id} has the optimistically unchoked neighbor {remote_id}."

        elif event == "complete":
            message = f"Peer {self.local_id} has downloaded the complete file."

        elif event == "downloaded_piece":
            remote_id = fields["remote_id"]
            piece_index = fields["piece_index"]
            piece_count = fields["piece_count"]
            message = (
                f"Peer {self.local_id} has downloaded the piece {piece_index} from {remote_id}. "
                f"Now the number of pieces it has is {piece_count}."
            )

        else:
            raise ValueError(f"Unknown event type: {event}")

        line = f"[{timestamp}]: {message}\n"

        with self.lock:
            self.file.write(line)
            self.file.flush()

    def close(self):
        with self.lock:
            self.file.flush()
            self.file.close()