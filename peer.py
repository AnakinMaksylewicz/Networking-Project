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
    if not 0 < peer_id <= 0xFFFFFFFF:
        raise ValueError("Peer ID must fit in a positive unsigned 4-byte integer")
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
    if data[18:28] != bytes(10):
        raise ProtocolError("Reserved handshake bytes must be zero")
    remote_id = UINT32.unpack(data[28:])[0]
    if remote_id <= 0 or remote_id == local_id:
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
    #Helper for unpacking bitfields we receive from peers
    if len(payload) != (piece_count + 7) // 8:
        raise ProtocolError("Incorrect bitfield payload length")
    spare = (-piece_count) % 8
    if spare and payload[-1] & ((1 << spare) - 1):
        raise ProtocolError("Bitfield padding bits must be zero")
    return [bool(payload[i // 8] & (1 << (7 - i % 8)))
            for i in range(piece_count)]