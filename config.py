#Parse both configs. peerProcess.py uses the results to create Peer objects.
def load_common(path):
    #TODO: Read Common.cfg and return a dictionary with these exact keys:
    #NumberOfPreferredNeighbors, UnchokingInterval, OptimisticUnchokingInterval,
    #FileName, FileSize, PieceSize. All values are ints except FileName (str).
    #Check positive sizes/intervals and a nonnegative preferred-neighbor count.
    raise NotImplementedError("Common.cfg parser is not implemented yet")


def load_peers(path):
    #TODO: Read PeerInfo.cfg and return a LIST of dictionaries in file order.
    #Each dictionary: peer_id (int), hostname (str), port (int), has_file (bool).
    #Convert the file's 0/1 into False/True. IDs must be unique positive integers.
    #Example row: {"peer_id": 1001, "hostname": "localhost", "port": 6001, "has_file": True}
    raise NotImplementedError("PeerInfo.cfg parser is not implemented yet")
