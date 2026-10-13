#Parse both configs. peerProcess.py uses the results to create Peer objects.

COMMON_INT_KEYS = (
    "NumberOfPreferredNeighbors",
    "UnchokingInterval",
    "OptimisticUnchokingInterval",
    "FileSize",
    "PieceSize",
)
COMMON_KEYS = COMMON_INT_KEYS + ("FileName",)


def _lines(path):
    #Yield (line_num, tokens) for every non-blank line, ignoring # comments.
    with open(path) as f:
        for line_num, raw in enumerate(f, 1):
            line = raw.split("#", 1)[0].strip()
            if line:
                yield line_num, line.split()


def _to_int(value, what, path, line_num):
    #int() with a readable error.
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{path}:{line_num}: {what} must be an integer, got {value!r}")
 
 
def load_common(path):
    #Reads Common.cfg and returns a dict keyed by the exact names in the file. 
    cfg = {}
    for line_num, tokens in _lines(path):
        if len(tokens) != 2:
            raise ValueError(f"{path}:{line_num}: expected 'Key Value', got {' '.join(tokens)!r}")
        key, value = tokens
        if key not in COMMON_KEYS:
            raise ValueError(f"{path}:{line_num}: unknown key {key!r}")
        if key in cfg:
            raise ValueError(f"{path}:{line_num}: duplicate key {key!r}")
        cfg[key] = _to_int(value, key, path, line_num) if key in COMMON_INT_KEYS else value
 
    missing = [k for k in COMMON_KEYS if k not in cfg]
    if missing:
        raise ValueError(f"{path}: missing keys: {', '.join(missing)}")
 
    if cfg["NumberOfPreferredNeighbors"] < 0:
        raise ValueError(f"{path}: NumberOfPreferredNeighbors must be >= 0")
    for key in ("UnchokingInterval", "OptimisticUnchokingInterval", "FileSize", "PieceSize"):
        if cfg[key] <= 0:
            raise ValueError(f"{path}: {key} must be > 0")
 
    return cfg
 
 
def load_peers(path):
    #Reads PeerInfo.cfg and returns a list of dicts, one per peer. 
    #Keep FILE ORDER: each peer connects to the peers listed above it, so don't sort this list.
    peers = []
    seen_ids = set()
    seen_addrs = set()
    for line_num, tokens in _lines(path):
        if len(tokens) != 4:
            raise ValueError(f"{path}:{line_num}: expected 'peerID host port hasFile', got {' '.join(tokens)!r}")
        raw_id, hostname, raw_port, raw_has = tokens
 
        peer_id = _to_int(raw_id, "peer ID", path, line_num)
        if peer_id <= 0:
            raise ValueError(f"{path}:{line_num}: peer ID must be positive, got {peer_id}")
        if peer_id in seen_ids:
            raise ValueError(f"{path}:{line_num}: duplicate peer ID {peer_id}")
 
        port = _to_int(raw_port, "port", path, line_num)
        if not 1 <= port <= 65535:
            raise ValueError(f"{path}:{line_num}: port out of range: {port}")
        if (hostname, port) in seen_addrs:
            raise ValueError(f"{path}:{line_num}: duplicate host/port {hostname}:{port}")
 
        if raw_has not in ("0", "1"):
            raise ValueError(f"{path}:{line_num}: hasFile must be 0 or 1, got {raw_has!r}")
 
        seen_ids.add(peer_id)
        seen_addrs.add((hostname, port))
        peers.append({
            "peer_id": peer_id,
            "hostname": hostname,
            "port": port,
            "has_file": raw_has == "1",
        })
 
    if not peers:
        raise ValueError(f"{path}: no peers listed")
    return peers