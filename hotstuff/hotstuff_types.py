import asyncio
import hashlib
import pickle
import random
from enum import Enum
from typing import Optional, Dict, List
from collections import Counter

N = 4
F = 1
QUORUM = 2*F+1

# some numbers that aren't 0, 1 and so forth
class Protocol_phase(Enum):
	NEW_VIEW = 5421310
	PREPARE = 5421311
	PREPARE_VOTE = 5421312
	PRECOMMIT = 5421313
	PRECOMMIT_VOTE = 5421314
	COMMIT = 5421315
	COMMIT_VOTE = 5421316
	DECIDE = 5421317

	def __str__(self):
		return self.name

class Fault_types(Enum):
	HONEST = 1315420
	CRASH = 1315421		# once self.current_view reaches x, stop running
	DELAYED = 1315242	# delays every send by random amount
	MALICIOUS = 1315423	# propose different blocks to different replicas

	def __str__(self):
		return self.name

class Command:
	def __init__(self, op, args):
		self.op = op
		self.args = args

	def calculate_hash(self):
		h = hashlib.sha256()
		h.update(str(self.op).encode())
		h.update(str(self.args).encode())
		return h

class Block:
	def __init__(self, cmds, parent, view):
		self.cmds = cmds
		self.parent = parent
		self.view = view
		self.hash = self.compute_hash()

	def compute_hash(self):
		h = hashlib.sha256()
		h.update(str(self.cmds).encode())
		h.update(str(self.view).encode())
		parent_hash = self.parent.hash if self.parent else "genesis"
		h.update(parent_hash.encode())
		return h.hexdigest()
	
	def __str__(self):
		return f"Block(v:{self.view}, cmd:{self.cmds})"
	
	def __eq__(self, other):
		return isinstance(other, Block) and self.hash == other.hash

GENESIS_BLOCK = Block([], None, 0)

class Signature:
	def __init__(self, n, f):
		self.threshold = 2*f + 1
		self.total = n
		self.combined = []

	@staticmethod	
	def partial_sign(view, phase, block_hash):
		h = hashlib.sha256()
		h.update(str(view).encode())
		h.update(str(phase).encode())
		h.update(str(block_hash).encode())
		return h.hexdigest()

	# placeholder for potential implementation
	def combine(self, partial_sig):
		self.combined.append(partial_sig)

	def verify(self):
		# if there are enough values that match
		if max(Counter(self.combined).values()) >= self.threshold:
			return True
		return False

class QC:
	def __init__(self, phase, view_number, block):
		self.phase = phase
		self.view_number = view_number
		self.block = block
		self.signature = Signature(N, F)

	def __str__(self):
		return f"QC(type:{self.phase}, view:{self.view_number})"

def matching_qc(qc, t, v):
	return qc.phase == t and qc.view_number == v

GENESIS_QC = QC(Protocol_phase.PREPARE, 0, GENESIS_BLOCK)

class Protocol_message:
	def __init__(self, phase, view_number, block, qc, sig=None, sender=None):
		self.phase = phase
		self.view_number = view_number
		self.block = block
		self.justify = qc 
		self.partial_sig = sig
		self.sender = sender
	
	def __repr__(self):
		return f"Msg(type:{self.phase}, view:{self.view_number}, from:{self.sender})"

class Client_request:
	def __init__(self, cmd, sender=None):
		self.cmd = cmd
		self.sender = sender

def matching_msg(m, t, v):
	return m.phase == t and m.view_number == v

