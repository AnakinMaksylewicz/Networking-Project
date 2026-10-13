#Class to make operations on peers easier, as well as info storage
#The peer class holds a peer's state and also encodes, reads, and writes its TCP messages.

import queue
import socket
import struct
import threading


#constants specified in instructions
HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"
HANDSHAKE_SIZE = 32
UINT32 = struct.Struct("!I")

class ProtocolError(ValueError):
    pass
    #error class for whenever a peer sends us a badly formatted msg for whatever reason


MESSAGE_TYPES = {
    "CHOKE": 0,
    "UNCHOKE": 1,
    "INTERESTED": 2,
    "NOT_INTERESTED": 3,
    "HAVE": 4,
    "BITFIELD": 5,
    "REQUEST": 6,
    "PIECE": 7
}

def receive_exact(sock, size):
    """Helper for reading exact number of bytes from the stream

    keep reading from recv until we get all of our desired bytes, since,
    for example, a 32-byte handshake could arrive like recv(32) -> 12 bytes,
    recv(20) -> 20 bytes (basically, in pieces).

    empty recv means EOF, including EOF in the middle of a message.
    The socket's owner closes it to interrupt this blocking operation.
    """
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise EOFError("Connection closed before the read finished")
        result.extend(chunk)
    return bytes(result)


def encode_handshake(peer_id):
    return HANDSHAKE_HEADER + bytes(10) + UINT32.pack(peer_id)


def exchange_handshake(sock, local_id, expected_id=None):
    """Send handshake, then receive handshake, decode, and return the ID of
       the peer we just handshook

    Incoming connections do not have an expected ID yet. Connection handler
    in peerProcess.py should check the returned ID against its configuration
    before attaching this socket.
    """
    sock.sendall(encode_handshake(local_id))
    data = receive_exact(sock, HANDSHAKE_SIZE)
    if data[:18] != HANDSHAKE_HEADER:
        raise ProtocolError("Incorrect handshake header")
    #last 4 bytes are the peer ID
    remote_id = UINT32.unpack(data[28:])[0]
    if remote_id <= 0 or remote_id == local_id: #instructions say peer ID must be pos. int.
        raise ProtocolError("Invalid remote peer ID")
    if expected_id is not None and remote_id != expected_id:
        raise ProtocolError("Handshake peer ID does not match the intended peer")
    return remote_id


def pack_bitfield(bits):
    #Helper for padding bitfield with trailing zeroes for sending to peers after handshake
    payload = bytearray((len(bits) + 7) // 8)
    for index, owned in enumerate(bits):
        if owned:
            payload[index // 8] |= 1 << (7 - index % 8)
    return bytes(payload)


def unpack_bitfield(payload, piece_count):
    return [bool(payload[i // 8] & (1 << (7 - i % 8)))
            for i in range(piece_count)]

class Peer:
    """Class that represents another running peer

    Very simple; a reader and a writer thread are constantly running. The reader reads messages from the incoming socket.
    When we call methods on the peer (for example, if we want to request a piece from a peer), those methods add on to
    a queue. The writer thread reads drains this queue and writes to the socket.
    """

    def __init__(self, peer_id, hostname, port, initially_has_file,
                 piece_count, file_size, piece_size):
        self.peer_id = peer_id
        self.hostname = hostname
        self.port = port
        self.initially_has_file = initially_has_file
        self.piece_count = piece_count
        self.file_size = file_size
        self.piece_size = piece_size

        #availability starts unknown/empty until a bitfield or have message
        self.bitfield = [False] * piece_count
        self.connected = False
        self.am_choking = True #WE are choking them
        self.peer_choking = True #THEY are choking us
        self.am_interested = False
        self.peer_interested = False
        self.is_preferred = False
        self.is_optimistic = False
        self.pending_request = None
        self.bytes_received_interval = 0

        self.socket = None
        self.reader_thread = None
        self.writer_thread = None
        self._outgoing = queue.Queue()
        self._stopped = threading.Event()
        self._socket_lock = threading.Lock()
        self._on_message = None
        self._on_disconnect = None
        self._first_message = True

    @staticmethod
    # builds the entire outgoing message, 4-byte length + 1-byte type + payload
    def _frame(message_type, payload=b""):
        return UINT32.pack(1 + len(payload)) + bytes([message_type]) + payload

    def _enqueue(self, message_type, payload=b""):
        # puts complete message into outgoing queue for writer to consume
        with self._socket_lock:
            if self.socket is None or self._stopped.is_set():
                raise ConnectionError(f"Peer {self.peer_id} is not connected")
            self._outgoing.put(self._frame(message_type, payload))

    def choke(self):
        self._enqueue(MESSAGE_TYPES["CHOKE"])

    def unchoke(self):
        self._enqueue(MESSAGE_TYPES["UNCHOKE"])

    def interested(self):
        self._enqueue(MESSAGE_TYPES["INTERESTED"])

    def not_interested(self):
        self._enqueue(MESSAGE_TYPES["NOT_INTERESTED"])

    def have(self, piece_index):
        self._check_index(piece_index)
        self._enqueue(MESSAGE_TYPES["HAVE"], UINT32.pack(piece_index))

    def request(self, piece_index):
        # Future process-level (as in peerProcess.py) code must check if already requested and reserve BEFORE this call.
        self._check_index(piece_index)
        self._enqueue(MESSAGE_TYPES["REQUEST"], UINT32.pack(piece_index))

    def piece(self, piece_index, data):
        # Future process-level code must check if choked first.
        self._check_index(piece_index)
        self._enqueue(MESSAGE_TYPES["PIECE"], UINT32.pack(piece_index) + data)

    def _check_index(self, index):
        if not 0 <= index < self.piece_count:
            raise ProtocolError("Piece index outside this file")

    def _piece_length(self, index):
        return min(self.piece_size, self.file_size - index * self.piece_size)

    def _read_message(self):
        length = UINT32.unpack(receive_exact(self.socket, 4))[0]
        kind = receive_exact(self.socket, 1)[0]
        payload = receive_exact(self.socket, length - 1) #length - 1 b/c we already read the one-byte type
        self._first_message = False
        if kind == MESSAGE_TYPES["BITFIELD"]:
            value = unpack_bitfield(payload, self.piece_count)
        elif kind in (MESSAGE_TYPES["HAVE"], MESSAGE_TYPES["REQUEST"], MESSAGE_TYPES["PIECE"]):
            index = UINT32.unpack(payload[:4])[0]
            self._check_index(index)
            value = index
            if kind == MESSAGE_TYPES["PIECE"]:
                value = (index, payload[4:])
        else:
            value = None
        return kind, value

    def start(self, sock, local_bits, on_message, on_disconnect):
        """Attach a socket after it's been identified and start its peer's two I/O threads.

        local_bits is our own local bitfield (every piece of the file we have on our own computer)
        on_message is a function passed in as a parameter that the reader thread later calls. the on_message function
        in peerProcess.py then dispatches to the necessary function, which just ends up writing to the queue and being
        consumed by the writer thread.

        on_disconnect does a similar thing, but for disconnecting from a peer.

        A peer instance represents one connection.
        """
        with self._socket_lock:
            if self.socket is not None or self._stopped.is_set():
                raise RuntimeError("This Peer already had a connection")
            self.socket = sock
            self._on_message = on_message
            self._on_disconnect = on_disconnect
            if any(local_bits):
                self._outgoing.put(self._frame(
                    MESSAGE_TYPES["BITFIELD"], pack_bitfield(local_bits)))

        self.writer_thread = threading.Thread(
            target=self._write_loop, name=f"writer-{self.peer_id}")
        self.reader_thread = threading.Thread(
            target=self._read_loop, name=f"reader-{self.peer_id}")
        self.writer_thread.start()
        self.reader_thread.start()


    def _read_loop(self):
        reason = "Connection closed"
        try:
            while not self._stopped.is_set():
                kind, value = self._read_message()
                self._on_message(self, kind, value)
        except (OSError, EOFError, ValueError, NotImplementedError) as error:
            reason = str(error)
        finally:
            self.close()
            self._on_disconnect(self, reason)

    def _write_loop(self):
        try:
            while not self._stopped.is_set():
                message = self._outgoing.get()
                if message is None:
                    break
                self.socket.sendall(message)
        except OSError:
            self.close()

    def close(self):
        #Socket close method for testing, not really gonna use this in the real implementation
        with self._socket_lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
            self._outgoing.put(None)
            sock = self.socket
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def join(self):
        #Called by process shutdown, just ends the reader and writer threads
        for thread in (self.reader_thread, self.writer_thread):
            if thread is not None and thread is not threading.current_thread():
                thread.join()
