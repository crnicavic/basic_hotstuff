from concurrent.futures import ThreadPoolExecutor

N = 4
F = 1
QUORUM = N - F

class Block:
    cmds = []
    parent = None
    height = 0;

    def __init__(self, cmds, parent, view):
        self.cmds = cmds
        self.parent = parent
        self.height = view;

ZERO_BLOCK = Block([], None, 0)

class Message:
    type = None
    view_number = None
    block = None
    justify = None

    def __init__(self, type, view_number, block, qc):
        self.type = type
        self.view_number = view_number
        self.block = block
        self.justify = qc 


class QC:
    type = None
    view_number = None
    node = None
    sig = None

    def __init__(self, type, view_number, node, sig):
        self.type = type
        self.view_number = view_number
        self.node = node
        self.sig = sig

class Replica:
    log = []; # array of blocks (will change probably)
    # collection of local variables
    vheight = None
    b_exec = None
    b_lock = None


    def __init__(self, vheight, b_exec, b_lock):
        self.vheight = vheight
        self.b_exec = b_exec
        self.b_lock = b_lock

    def on_client_request(self, message):
        # new view
        pass

    def on_message(self, message):
        # check message type and do proper callback
        pass