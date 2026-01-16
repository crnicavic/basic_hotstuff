import asyncio
import hashlib
from enum import Enum
from typing import Optional, Dict, List

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

class QC:
	def __init__(self, qc_type, view_number, block):
		self.qc_type = qc_type
		self.view_number = view_number
		self.block = block
		self.voters = []

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

class Replica:
	def __init__(self, replica_id, network):
		self.replica_id = replica_id
		self.network = network
		self.current_view = 0
		self.log = [GENESIS_BLOCK]
		
		self.inbox = asyncio.Queue()
		
		self.new_view_msgs = {}
		self.prepare_votes = {}
		self.precommit_votes = {}
		self.commit_votes = {}
		
		self.high_prepare_qc = GENESIS_QC
		self.locked_qc = GENESIS_QC
		
		self.is_leader = False
		self.running = True

	def trace(self, string):
		print(f"[R{self.replica_id}] {string}")

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

	def get_leader(self, view):
		return view % N

	async def send(self, recipient_id, msg):
		msg.sender = self.replica_id
		await self.network.send(recipient_id, msg)

	async def broadcast(self, msg):
		msg.sender = self.replica_id
		await self.network.broadcast(msg)

	# NEW-VIEW - replica
	async def start_new_view(self, new_view):
		if new_view <= self.current_view:
			return
		
		self.current_view = new_view
		self.is_leader = (self.get_leader(new_view) == self.replica_id)
		
		self.trace(f"Entering view {new_view} {'(LEADER)' if self.is_leader else ''}")
		
		leader_id = self.get_leader(new_view)
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
			
			vote_msg = Message(
				Message_types.PREPARE_VOTE,
				self.current_view,
				msg.block,
				None,
				self.replica_id
			)
			
			leader_id = self.get_leader(self.current_view)
			await self.send(leader_id, vote_msg)

	# PRECOMMIT - leader
	async def handle_prepare_vote(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.PREPARE_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.prepare_votes:
			self.prepare_votes[msg.view_number] = []
		
		self.prepare_votes[msg.view_number].append(msg)
		
		if len(self.prepare_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PREPARE,
				self.current_view,
				msg.block
			)
			for vote in self.prepare_votes[msg.view_number]:
				qc.voters.append(vote.partial_sig)
			
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
		
		if msg.justify.view_number > self.high_prepare_qc.view_number:
			self.high_prepare_qc = msg.justify
		
		vote_msg = Message(
			Message_types.PRECOMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			self.replica_id
		)
		
		leader_id = self.get_leader(self.current_view)
		await self.send(leader_id, vote_msg)

	# COMMIT - leader
	async def handle_precommit_vote(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.PRECOMMIT_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.precommit_votes:
			self.precommit_votes[msg.view_number] = []
		
		self.precommit_votes[msg.view_number].append(msg)
		
		if len(self.precommit_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PRECOMMIT,
				self.current_view,
				msg.block
			)
			for vote in self.precommit_votes[msg.view_number]:
				qc.voters.append(vote.partial_sig)
			
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
		
		if msg.justify.view_number > self.locked_qc.view_number:
			self.locked_qc = msg.justify
		
		vote_msg = Message(
			Message_types.COMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			self.replica_id
		)
		
		leader_id = self.get_leader(self.current_view)
		await self.send(leader_id, vote_msg)

	# DECIDE - leader
	async def handle_commit_vote(self, msg):
		if not self.is_leader or not matching_msg(msg, Message_types.COMMIT_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.commit_votes:
			self.commit_votes[msg.view_number] = []
		
		self.commit_votes[msg.view_number].append(msg)
		
		if len(self.commit_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.COMMIT,
				self.current_view,
				msg.block
			)
			for vote in self.commit_votes[msg.view_number]:
				qc.voters.append(vote.partial_sig)
			
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
		
		self.trace(f"EXECUTING {msg.justify.block.cmds}")
		self.log.append(msg.justify.block)
		
		await asyncio.sleep(0.1)
		await self.start_new_view(self.current_view + 1)

	async def message_handler(self):
		while self.running:
			try:
				msg = await asyncio.wait_for(self.inbox.get(), timeout=1.0)
				
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
		await self.start_new_view(1)
		await self.message_handler()


class Network:
	def __init__(self, delay_ms=10):
		self.replicas: Dict[int, Replica] = {}
		self.delay_ms = delay_ms
	
	def register_replica(self, replica: Replica):
		self.replicas[replica.replica_id] = replica
	
	async def send(self, recipient_id: int, msg: Message):
		await asyncio.sleep(self.delay_ms / 1000.0)
		if recipient_id in self.replicas:
			await self.replicas[recipient_id].inbox.put(msg)
	
	async def broadcast(self, msg: Message):
		tasks = []
		for replica_id in self.replicas:
			tasks.append(self.send(replica_id, msg))
		await asyncio.gather(*tasks)


async def main():
	network = Network(delay_ms=10)
	
	replicas = []
	for i in range(N):
		replica = Replica(i, network)
		network.register_replica(replica)
		replicas.append(replica)
	
	tasks = [replica.run() for replica in replicas]
	
	try:
		# run for 5 secs
		await asyncio.wait_for(
			asyncio.gather(*tasks),
			timeout=5.0
		)
	except asyncio.TimeoutError:
		
		for replica in replicas:
			replica.running = False
		
		for replica in replicas:
			print(f"Replica {replica.replica_id}: Log length={len(replica.log)}, "
			      f"Locked view={replica.locked_qc.view_number}")


if __name__ == "__main__":
	asyncio.run(main())
