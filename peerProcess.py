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

        #TODO later: scheduler, piece selection/storage, and completion detection.


    def start(self):
        #create directory for this peer's shared-file data
        folder = self.working_directory / f"peer_{self.local_id}"
        folder.mkdir(exist_ok=True)
        # ^ Place the seed file here before starting a peer with has_file = 1.

        #set up listener socket
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind((self.local_info["hostname"], self.local_info["port"]))
        self.listener.listen()
        self.listener.settimeout(0.5)
        #thread that loops and constantly monitors listener socket to accept connections & connect socket to correct peer object
        #this only handles peer ids AHEAD of ours, it never connects with peer ids before ours.
        self.accept_thread = threading.Thread(
            target=self._accept_loop, name=f"accept-{self.local_id}")
        self.accept_thread.start()
        #connect to peers before us
        for remote_id in self.earlier_ids:
            if self.stop_event.is_set():
                break
            self._connect_earlier(remote_id)

    @staticmethod
    def _close_socket(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def _accept_loop(self):
        # this thread handles the handshake and attaches the socket based off handshake result's peer id.
        while not self.stop_event.is_set():
            try:
                sock, _address = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                self.stop_event.set()
                break

            self._establish(sock, None)

    def _connect_earlier(self, remote_id):
        #connect w/ peers earlier in the peerinfo config than us
        peer = self.peers[remote_id]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        connected = False
        try:
            sock.connect((peer.hostname, peer.port))
            connected = True
        except OSError as error:
            print(f"Could not connect to {remote_id}: {error}")
            return
        finally:
            if not connected:
                sock.close()

        self._establish(sock, remote_id)

    def _establish(self, sock, expected_id): #connects an accepted socket to a peer object
                                             #also starts reader and writer thread for peer, allowing for communication
        attached = False
        try:
            sock.settimeout(5)  # Stop waiting if the handshake stalls
            remote_id = exchange_handshake(sock, self.local_id, expected_id)
            if expected_id is None and remote_id not in self.later_ids:
                raise ProtocolError("Incoming ID must be a later configured peer")
            sock.settimeout(None)
            with self.state_lock:
                if self.stop_event.is_set():
                    return
                peer = self.peers[remote_id]
                # Log the connection before its reader handles messages.
                self.logger.log("connection", remote_id=remote_id,
                                outgoing=expected_id is not None)
                peer.connected = True
                peer.start(sock, self.local_bitfield.copy(),
                           self.handle_message, self.handle_disconnect)
                attached = True
            print(f"Peer {self.local_id}: handshake complete with {remote_id}")
        except (OSError, EOFError, ValueError, RuntimeError) as error:
            if not self.stop_event.is_set():
                print(f"Connection rejected/failed: {error}", file=sys.stderr)
        finally:
            if not attached:
                self._close_socket(sock)

    def handle_message(self, peer, kind, value):
        """ This is the function we pass in to peer.start() that changes a peer's attributes based on info and decides whether
        to respond to a message. Called by the reader thread after decoding a message.

        peer = the remote Peer object that sent it.
        kind = an integer from MESSAGE_TYPES, literally just the message type
        value = decoded payload: bitfield list, piece index, or None.

        Use peer methods to queue outgoing messages. Do not write to sockets
        directly or create threads here."""

        #protect state and handle exits
        with self.state_lock:
            if self.stop_event.is_set() or not peer.connected:
                return

            # handle kind (int 0-7)(per spec) corresponding to a value from MESSAGE_TYPES dict in peer.py
            # update values and log relevant updates such as changes in interest, chokes, and availability
            if kind == MESSAGE_TYPES["BITFIELD"]:
                peer.bitfield = value.copy()
                self._update_interest(peer)
            elif kind == MESSAGE_TYPES["HAVE"]:
                peer.bitfield[value] = True
                self.logger.log("have", remote_id=peer.peer_id, piece_index=value)
                self._update_interest(peer)
            elif kind == MESSAGE_TYPES["INTERESTED"]:
                peer.peer_interested = True
                self.logger.log("interested", remote_id=peer.peer_id)
            elif kind == MESSAGE_TYPES["NOT_INTERESTED"]:
                peer.peer_interested = False
                self.logger.log("not_interested", remote_id=peer.peer_id)
            elif kind == MESSAGE_TYPES["CHOKE"]:
                peer.peer_choking = True
                self.logger.log("choke", remote_id=peer.peer_id)
            elif kind == MESSAGE_TYPES["UNCHOKE"]:
                peer.peer_choking = False
                self.logger.log("unchoke", remote_id=peer.peer_id)

            # REQUEST/PIECE handling after midpoint submission

    def _update_interest(self, peer):
        """Compares peer's pieces with ours. If they have something we need, call
            peer.interested(). Otherwise, call peer.not_interested(). Called while
            handle_message holds the state lock, don't acquire the lock again here."""

        # TODO: Compare the neighbor's availability with our local bitfield
        # to determine whether they have anything we still need.

        # Queue using Peer.interested()
        # Send the initial announcement, then only announce changes (peer class has a self.interest_announced attr for this).
        # Keep our saved interest and announcement status consistent with it.

        # This concerns OUR interest; preserve THEIR interest in our pieces.
        # Sending interest announcements does not require logging
        raise NotImplementedError("TODO: calculate and announce our interest")

    def handle_disconnect(self, peer, reason):
        #Called by the reader after it closes the connection.
        #Do not join threads here; process shutdown handles that.

        with self.state_lock:
            #TODO:
            #Set peer.connected to False.
            #Set peer.pending_request to None.
            #Set peer.is_preferred and peer.is_optimistic to False.
            #Keep peer.bitfield: disconnecting does not erase known ownership.
            raise NotImplementedError("TODO: update disconnected peer state")

        if not self.stop_event.is_set():
            print(
                f"Peer {peer.peer_id} disconnected: {reason}",
                file=sys.stderr
            )

    def run(self):
        try:
            self.start()
            print(f"Peer {self.local_id} running; Ctrl+C to stop")
            while not self.stop_event.wait(0.5):
                pass
        except KeyboardInterrupt:
            print(f"Stopping peer {self.local_id}")
        finally:
            self.shutdown()

    def shutdown(self):
        self.stop_event.set()

        if self.listener is not None:
            self._close_socket(self.listener)

        if self.accept_thread is not None:
            self.accept_thread.join()

        for peer in self.peers.values():
            peer.close()

        for peer in self.peers.values():
            peer.join()


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
