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
class Message_types(Enum):
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
	SILENT = 1315242	# each send has a 70% of "failing"
	MALICIOUS = 1315423	# propose different blocks to different replicas

	def __str__(self):
		return self.name

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
	def partial_sign(view, msg_type, block_hash):
		h = hashlib.sha256()
		h.update(str(view).encode())
		h.update(str(msg_type).encode())
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
	def __init__(self, qc_type, view_number, block):
		self.qc_type = qc_type
		self.view_number = view_number
		self.block = block
		self.signature = Signature(N, F)

	def __str__(self):
		return f"QC(type:{self.qc_type}, view:{self.view_number})"

def matching_qc(qc, t, v):
	return qc.qc_type == t and qc.view_number == v

GENESIS_QC = QC(Message_types.PREPARE, 0, GENESIS_BLOCK)

class Message:
	def __init__(self, msg_type, view_number, block, qc, sig=None, sender=None):
		self.msg_type = msg_type
		self.view_number = view_number
		self.block = block
		self.justify = qc 
		self.partial_sig = sig
		self.sender = sender
	
	def __repr__(self):
		return f"Msg(type:{self.msg_type}, view:{self.view_number}, from:{self.sender})"

def matching_msg(m, t, v):
	return m.msg_type == t and m.view_number == v

# replica's way of talking with the world
class Network:
	def __init__(self, replica_id, host='127.0.0.1', port=50000):
		# the id of the replica who uses the object
		self.replica_id = replica_id
		self.inbox = asyncio.Queue()
		self.address_book = {} # replica_id -> (host: string, port: int)
		self.conns = {} # replica_id -> (reader, writer)
		self.server = None
		self.host = host
		self.port = port
	
	async def start_server(self):
		self.server = await asyncio.start_server(
			self.recv,
			self.host,
			self.port
		)
	
	async def connect(self, replica_id):
		if replica_id in self.conns:
			return True

		if replica_id not in self.address_book:
			return False
		
		for attempt in range(3):
			try:
				host, port = self.address_book[replica_id]
				self.conns[replica_id] = await asyncio.open_connection(host, port)
				return True
			except ConnectionRefusedError:
				await asyncio.sleep(0.2)
		return False

	async def recv(self, reader, writer):
		try:
			while True:
				# first 32 bits of message are the byte count
				msg_byte_count = await reader.readexactly(4)
				msg_byte_count = int.from_bytes(msg_byte_count, 'big')				

				packet = await reader.read(msg_byte_count)
				if not packet:
					continue
				msg = pickle.loads(packet)
				await self.inbox.put(msg)
		except asyncio.IncompleteReadError:
			pass
		except Exception as e:
			print(f"Network error! {e}")
		finally:
			writer.close()
			await writer.wait_closed()
	
	async def send(self, recipient_id, msg):
		if recipient_id == self.replica_id:
			await self.inbox.put(msg)
			return

		if not await self.connect(recipient_id):
			return

		_, writer = self.conns[recipient_id]
		
		packet = pickle.dumps(msg)
		msg_byte_count = len(packet)

		writer.write(msg_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()

	async def broadcast(self, msg: Message):
		tasks = []
		for replica_id in self.address_book:
			tasks.append(self.send(replica_id, msg))
		await asyncio.gather(*tasks)

	async def stop_server(self):
		self.server.close()
		for replica_id in self.conns:
			_, writer = self.conns[replica_id]
			writer.close()
			await writer.wait_closed()

class Pacemaker:
	def __init__(self, timeout, replica_callback):
		self.timeout = timeout
		self.current_view = 0
		self.task = None
		self.timer_running = False
		self.replica_callback = replica_callback

	def get_leader(self, view):
		return view % N

	# when no progress is made
	async def on_timeout(self):
		self.timer_running = True
		try:
			await asyncio.sleep(self.timeout)
			# force replica to move to next view
			print("TIMEOUT")
			self.timer_running = False
			self.current_view += 1
			await self.replica_callback(self.current_view)
		except asyncio.CancelledError:
			self.timer_running = False

	# call when moving on to next view
	def new_view(self, view):	
		self.current_view = view
		if self.task is not None and self.timer_running:
			self.task.cancel()
		self.task = asyncio.create_task(self.on_timeout())

	def stop(self):
		if self.task is not None and self.timer_running:
			self.task.cancel()

class Replica:
	def __init__(self, replica_id, network, fault_type=Fault_types.HONEST):
		self.replica_id = replica_id
		self.network = network
		self.current_view = 0
		self.current_proposal = None
		self.log = [GENESIS_BLOCK]
		
		self.new_view_msgs = {}
		self.prepare_votes = {}
		self.precommit_votes = {}
		self.commit_votes = {}
		
		self.high_prepare_qc = GENESIS_QC
		self.locked_qc = GENESIS_QC
		
		self.is_leader = False
		self.running = True

		self.pacemaker = Pacemaker(2.0, self.start_new_view)

		self.fault_type = fault_type
		if fault_type == Fault_types.CRASH:
			self.crash_view = 10
		elif fault_type == Fault_types.SILENT:
			self.drop_prob = 0.7

	def trace(self, string):
		print(f"[R{self.replica_id}][{self.fault_type}] {string}")

	def extends(self, new_block, from_block):
		current_block = new_block
		while current_block.hash != from_block.hash:
			if current_block.parent is None:
				return False
			current_block = current_block.parent
		return True

	def safe_block(self, block, qc):
		return (self.extends(block, self.locked_qc.block) or 
		        (qc.view_number > self.locked_qc.view_number))


	async def send(self, recipient_id, msg):
		if self.fault_type == Fault_types.SILENT:
			if random.random() < self.drop_prob:
				return
		msg.sender = self.replica_id
		await self.network.send(recipient_id, msg)

	async def broadcast(self, msg):
		if self.fault_type == Fault_types.SILENT:
			if random.random() < self.drop_prob:
				return
		msg.sender = self.replica_id
		await self.network.broadcast(msg)

	# NEW-VIEW - replica
	async def start_new_view(self, new_view):
		if new_view <= self.current_view:
			return
		
		self.current_view = new_view
		self.pacemaker.new_view(new_view)
		leader_id = self.pacemaker.get_leader(new_view)
		self.is_leader = (leader_id == self.replica_id)
		
		self.trace(f"Entering view {new_view} {'(LEADER)' if self.is_leader else ''}")
		
		msg = Message(
			Message_types.NEW_VIEW,
			new_view,
			None,
			self.high_prepare_qc,
			self.replica_id
		)
		await self.send(leader_id, msg)

	# PREPARE - leader
	async def handle_new_view(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.NEW_VIEW, self.current_view):
			return
		
		if msg.view_number not in self.new_view_msgs:
			self.new_view_msgs[msg.view_number] = []
		
		self.new_view_msgs[msg.view_number].append(msg)
		
		if len(self.new_view_msgs[msg.view_number]) == QUORUM:
			highest_qc = max(
				self.new_view_msgs[msg.view_number],
				key=lambda m: m.justify.view_number
			).justify
			
			proposal_block = Block(
				f"cmd_{self.current_view}",
				highest_qc.block,
				self.current_view
			)
			
			proposal_msg = Message(
				Message_types.PREPARE,
				self.current_view,
				proposal_block,
				highest_qc
			)
			
			self.trace(f"Leader proposing {proposal_block}")
			await self.broadcast(proposal_msg)

	# PREPARE - replica
	async def handle_prepare(self, msg):
		if not matching_msg(msg, Message_types.PREPARE, self.current_view):
			return
		
		if self.extends(msg.block, msg.justify.block) and self.safe_block(msg.block, msg.justify):
			self.trace(f"Voting for {msg.block}")
			self.current_proposal = msg.block

			partial_sig = Signature.partial_sign(
				self.current_view,
				Message_types.PREPARE_VOTE,
				msg.block.hash
			)

			vote_msg = Message(
				Message_types.PREPARE_VOTE,
				self.current_view,
				msg.block,
				None,
				partial_sig
			)
			
			leader_id = self.pacemaker.get_leader(self.current_view)
			await self.send(leader_id, vote_msg)

	# PRECOMMIT - leader
	async def handle_prepare_vote(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.PREPARE_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.prepare_votes:
			self.prepare_votes[msg.view_number] = []
		
		if msg.block.hash == self.current_proposal.hash:	
			self.prepare_votes[msg.view_number].append(msg)
		
		if len(self.prepare_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PREPARE,
				self.current_view,
				msg.block
			)
			for vote in self.prepare_votes[msg.view_number]:
				qc.signature.combine(vote.partial_sig)
			
			self.high_prepare_qc = qc
			
			self.trace(f"Leader formed {qc}")
			
			precommit_msg = Message(
				Message_types.PRECOMMIT,
				self.current_view,
				None,
				qc
			)
			await self.broadcast(precommit_msg)

	# PRECOMMIT - replica
	async def handle_precommit(self, msg):
		if not matching_qc(msg.justify, Message_types.PREPARE, self.current_view):
			return
		if not msg.justify.signature.verify():
			return
		
		if msg.justify.view_number > self.high_prepare_qc.view_number:
			self.high_prepare_qc = msg.justify

		partial_sig = Signature.partial_sign(
			self.current_view,
			Message_types.PRECOMMIT_VOTE,
			msg.justify.block.hash
		)
	
		vote_msg = Message(
			Message_types.PRECOMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			partial_sig
		)
		
		leader_id = self.pacemaker.get_leader(self.current_view)
		await self.send(leader_id, vote_msg)

	# COMMIT - leader
	async def handle_precommit_vote(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.PRECOMMIT_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.precommit_votes:
			self.precommit_votes[msg.view_number] = []
		
		#dont count bogus votes	
		if msg.block.hash == self.current_proposal.hash:
			self.precommit_votes[msg.view_number].append(msg)
		
		if len(self.precommit_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PRECOMMIT,
				self.current_view,
				msg.block
			)
			for vote in self.precommit_votes[msg.view_number]:
				qc.signature.combine(vote.partial_sig)
			
			self.trace(f"Leader formed {qc}")
			
			commit_msg = Message(
				Message_types.COMMIT,
				self.current_view,
				None,
				qc
			)
			await self.broadcast(commit_msg)

	# COMMIT - replica
	async def handle_commit(self, msg):
		if not matching_qc(msg.justify, Message_types.PRECOMMIT, self.current_view):
			return
		if not msg.justify.signature.verify():
			return
		
		if msg.justify.view_number > self.locked_qc.view_number:
			self.locked_qc = msg.justify
	
		partial_sig = Signature.partial_sign(
			self.current_view,
			Message_types.COMMIT_VOTE,
			msg.justify.block.hash
		)	
		vote_msg = Message(
			Message_types.COMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			partial_sig
		)
		
		leader_id = self.pacemaker.get_leader(self.current_view)
		await self.send(leader_id, vote_msg)

	# DECIDE - leader
	async def handle_commit_vote(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.COMMIT_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.commit_votes:
			self.commit_votes[msg.view_number] = []
		
		if msg.block.hash == self.current_proposal.hash:
			self.commit_votes[msg.view_number].append(msg)
		
		if len(self.commit_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.COMMIT,
				self.current_view,
				msg.block
			)
			for vote in self.commit_votes[msg.view_number]:
				qc.signature.combine(vote.partial_sig)
			
			self.trace(f"Leader formed {qc}")
			
			decide_msg = Message(
				Message_types.DECIDE,
				self.current_view,
				None,
				qc
			)
			await self.broadcast(decide_msg)

	# DECIDE - replica
	async def handle_decide(self, msg):
		if not matching_qc(msg.justify, Message_types.COMMIT, self.current_view):
			return
		if not msg.justify.signature.verify():
			return
		
		self.trace(f"Executing {msg.justify.block.cmds}")
		self.log.append(msg.justify.block)
		
		# let others catch-up
		await asyncio.sleep(0.1)
		await self.start_new_view(self.current_view + 1)

	async def message_handler(self):
		while self.running:
			if self.fault_type == Fault_types.CRASH and self.current_view == self.crash_view:
				self.trace(f"REPLICA CRASHED AT {self.current_view}")
				self.pacemaker.stop()
				self.running = False
			try:
				msg = await asyncio.wait_for(self.network.inbox.get(), timeout=1.0)
				
				match msg.msg_type:
					case Message_types.NEW_VIEW:
						await self.handle_new_view(msg)
					case Message_types.PREPARE:
						await self.handle_prepare(msg)
					case Message_types.PREPARE_VOTE:
						await self.handle_prepare_vote(msg)
					case Message_types.PRECOMMIT:
						await self.handle_precommit(msg)
					case Message_types.PRECOMMIT_VOTE:
						await self.handle_precommit_vote(msg)
					case Message_types.COMMIT:
						await self.handle_commit(msg)
					case Message_types.COMMIT_VOTE:
						await self.handle_commit_vote(msg)
					case Message_types.DECIDE:
						await self.handle_decide(msg)
					
			except asyncio.TimeoutError:
				continue

	async def run(self):
		await self.network.start_server()
		await asyncio.sleep(1)
		await self.start_new_view(1)
		await self.message_handler()

async def simulation():
	address_book = {
		0: ('127.0.0.1', 50000),
		1: ('127.0.0.1', 50001),
		2: ('127.0.0.1', 50002),
		3: ('127.0.0.1', 50003)
	}	
	replica_types = {
		0: Fault_types.HONEST,
		1: Fault_types.HONEST,
		2: Fault_types.HONEST,
		3: Fault_types.HONEST
	}
	replicas = []
	for i in range(N):
		network = Network(i, '127.0.0.1', 50000+i)
		network.address_book = address_book
		replica = Replica(i, network, replica_types[i])
		replicas.append(replica)
	
	tasks = [replica.run() for replica in replicas]
	
	try:
		# run for 5 secs
		await asyncio.wait_for(
			asyncio.gather(*tasks),
			timeout=20.0
		)
	except asyncio.TimeoutError:
		
		for replica in replicas:
			replica.running = False
		
		for replica in replicas:
			print(f"Replica {replica.replica_id}: log length={len(replica.log)}, "
			      f"locked view={replica.locked_qc.view_number}")
			await replica.network.stop_server()

async def main():
	await simulation()

if __name__ == "__main__":
	asyncio.run(main())
