from enum import Enum
import hashlib

N = 4
F = 1
QUORUM = 2*F+1
NEW_VIEW = 5421315

class Message_types:
	NEW_VIEW = 5421310
	PREPARE = 5421311
	PREPARE_VOTE = 5421312
	PRECOMMIT = 5421313
	PRECOMMIT_VOTE = 5421314
	COMMIT = 5421315
	COMMIT_VOTE = 5421316
	DECIDE = 5421317

class Block:
	def __init__(self, cmds, parent, view):
		self.cmds = cmds
		self.parent = parent
		self.view = view;
		self.hash = self.compute_hash()

	def compute_hash(self):
		h = hashlib.sha256()
		# convert to bytestring
		h.update(str(self.cmds).encode())
		h.update(str(self.view).encode())
		parent_hash = self.parent.hash if self.parent else "genesis"
		h.update(parent_hash.encode())
		return h.hexdigest()
	
	def __str__(self):
		return f"Block(height:{self.view}, command: {self.cmds})"
	
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
		ret = f"QC(type:{self.qc_type};"
		ret += f"view_number:{self.view_number};"
		ret += f"block:{self.block};"
		ret += f"voters:{self.voters};)"
		return ret

def matching_qc(qc, t, v):
	return qc.qc_type == t and qc.view_number == v

GENESIS_QC = QC(
		Message_types.PREPARE,
		0,
		GENESIS_BLOCK
)

class Message:
	def __init__(self, msg_type, view_number, block, qc, sig=None):
		self.msg_type = msg_type
		self.view_number = view_number
		self.block = block
		self.justify = qc 
		self.partial_sig = sig 
	
	def __repr__(self):
		ret = f"Message(type:{self.msg_type};"
		ret += f"view_number:{self.view_number};"
		ret += f"block:{self.block};"
		ret += f"justify_qc:{self.justify};)"
		return ret

def matching_msg(m, t, v):
	return m.msg_type == t and m.view_number == v

class Replica:
	def __init__(self, replica_id):
		self.replica_id = replica_id
		self.current_view = 0
		self.log = [GENESIS_BLOCK]
		# dictionaries of arrays per view (later use only one array)
		self.new_view_msgs = {}
		self.prepare_votes = {}
		self.precommit_votes = {}
		self.commit_votes = {}
		self.high_prepare_qc = GENESIS_QC
		self.locked_qc = GENESIS_QC
		self.replicas = []

	def extends(self, new_block, from_block):
		# climb the starting with the new block and return true if from_block is a parent
		current_block = new_block
		while current_block.hash != from_block.hash:
			if current_block.parent is None:
				return False
			current_block = current_block.parent
		return True

	def safe_block(self, block, qc):
		return self.extends(block, self.locked_qc.block) or (qc.view_number > self.locked_qc.view_number)

	def on_client_request(self, view):
		# i don't want unpredictable behaviour in this model
		if view < self.current_view:
			return

		leader = self.replicas[(view + 1) % N]
		new_view_msg = Message(NEW_VIEW, self.current_view, None, self.high_prepare_qc)	
		return new_view_msg

	# PREPARE - LEADER
	def collect_new_view(self, msg):
		if not matching_msg(msg, Message_types.NEW_VIEW, self.current_view):
			return None
		elif msg.view_number not in self.new_view_msgs:
			self.new_view_msgs[msg.view_number] = [msg]
		else:
			self.new_view_msgs[msg.view_number].append(msg)

		if len(self.new_view_msgs[msg.view_number]) == QUORUM:
			self.current_view += 1
			sort_lambda = lambda m: m.justify.view_number 
			self.new_view_msgs[msg.view_number].sort(key=sort_lambda)
			self.high_prepare_qc = self.new_view_msgs[msg.view_number][-1].justify
			proposal_block = Block(
				f"cmd_{self.current_view}",
				self.high_prepare_qc.block,
				self.current_view
			)
			proposal_msg = Message(
				Message_types.PREPARE,
				self.current_view,
				proposal_block,
				self.high_prepare_qc
			)
			return proposal_msg
		return None

	# PREPARE - REPLICA
	def examine_prepare_proposal(self, msg):
		if not matching_msg(msg, Message_types.PREPARE, self.current_view):
			return
		
		if msg.view_number > self.current_view:
			self.current_view = msg.view_number

		leader = self.replicas[msg.view_number % N]
		print(f"received proposal for {msg.block} from {leader}")
		if self.extends(msg.block, msg.justify.block) and self.safe_block(msg.block, msg.justify):
			#send vote or rather send signature
			print(f"replica {self.replica_id} voted for {msg.block}")
			vote_msg = Message(
				Message_types.PREPARE_VOTE,
				self.current_view,
				msg.block,
				None, # specification doesnt send this
				self.replica_id # this is the signature :D
			)
			return vote_msg
	
	# PRECOMMIT-LEADER
	def collect_prepare_votes(self, vote_msg):
		if not matching_msg(vote_msg, Message_types.PREPARE_VOTE, self.current_view):
			return
		elif vote_msg.view_number not in self.prepare_votes:
			self.prepare_votes[vote_msg.view_number] = [vote_msg]
		else:
			self.prepare_votes[vote_msg.view_number].append(vote_msg)

		#TODO: there should be a check if the messages reference the same block
		# and view number
		if len(self.prepare_votes[vote_msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PREPARE,
				self.current_view,
				vote_msg.block
			)
			for m in self.prepare_votes[vote_msg.view_number]:
				qc.voters.append(m.partial_sig)
			self.high_prepare_qc = qc
			precommit_msg = Message(
				Message_types.PRECOMMIT,
				self.current_view,
				vote_msg.block,
				qc
			)
			print(f"leader formed prepare QC {qc}")
			return precommit_msg
		return None

	# PRECOMMIT - REPLICA
	def examine_precommit(self, msg):
		if not matching_qc(msg.justify, Message_types.PREPARE, self.current_view):
			return
		
		if msg.view_number > self.current_view:
			self.current_view = msg.view_number

		if msg.justify.view_number > self.high_prepare_qc.view_number:
			print(f"received new high_prepare_qc {msg.justify}")
			self.high_prepare_qc = msg.justify

		vote_msg = Message(
			Message_types.PRECOMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			self.replica_id
		)
		return vote_msg
	
	# COMMIT - LEADER
	def collect_precommit_votes(self, vote_msg):
		if not matching_msg(vote_msg, Message_types.PRECOMMIT_VOTE, self.current_view):
			return
		elif vote_msg.view_number not in self.precommit_votes:
			self.precommit_votes[vote_msg.view_number] = [vote_msg]
		else:
			self.precommit_votes[vote_msg.view_number].append(vote_msg)

		if len(self.precommit_votes[vote_msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PRECOMMIT,
				self.current_view,
				vote_msg.block
			)
			print(f"leader formed precommit QC {qc}")
			for m in self.precommit_votes[vote_msg.view_number]:
				qc.voters.append(m.partial_sig)
			commit_msg = Message(
				Message_types.COMMIT,
				self.current_view,
				None,
				qc
			)
			return commit_msg
		return None
	
	# COMMIT - replica
	def examine_commit(self, msg):
		if not matching_qc(msg.justify, Message_types.PRECOMMIT, self.current_view):
			return
		
		if msg.view_number > self.current_view:
			self.current_view = msg.view_number

		leader = self.replicas[msg.view_number % N]
		
		if msg.justify.view_number > self.locked_qc.view_number:
			print(f"updated locked_qc {self.locked_qc}")

		self.locked_qc = msg.justify
		vote_msg = Message(
			Message_types.COMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			self.replica_id
		)
		return vote_msg
	
	# DECIDE - LEADER
	def collect_commit_votes(self, vote_msg):
		if not matching_msg(vote_msg, Message_types.COMMIT_VOTE, self.current_view):
			return
		elif vote_msg.view_number not in self.commit_votes:
			self.commit_votes[vote_msg.view_number] = [vote_msg]
		else:
			self.commit_votes[vote_msg.view_number].append(vote_msg) 
		
		if len(self.commit_votes[vote_msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.COMMIT,
				self.current_view,
				vote_msg.block
			)
			print(f"leader formed precommit QC {qc}")
			for m in self.commit_votes[vote_msg.view_number]:
				qc.voters.append(m.partial_sig)
			decide_msg = Message(
				Message_types.DECIDE,
				self.current_view,
				None,
				qc
			)	
			return decide_msg
		return None
	
	# DECIDE - REPLICA
	def examine_decision(self, decide_msg):
		if not matching_qc(decide_msg.justify, Message_types.COMMIT, self.current_view):
			return
		print(f"executing {decide_msg.justify.block.cmds}")

	def __str__(self):
		ret = f"replica_id:{self.replica_id} on view:{self.current_view}"
		return ret
	

if __name__ == "__main__":

	view = 1
	replicas = [Replica(i) for i in range(N)] 
	for r in replicas:
		r.replicas = replicas
	
	leader = replicas[view % N]

	new_view_messages = []
	for r in replicas:
		new_view_messages.append(r.on_client_request(0)) 
	
	proposal_msg = None
	for m in new_view_messages:
		proposal_msg = leader.collect_new_view(m)
		if proposal_msg is not None:
			break

	prepare_votes = []
	for r in replicas:
		prepare_votes.append(r.examine_prepare_proposal(proposal_msg))

	precommit_msg = None
	for m in prepare_votes:
		precommit_msg = leader.collect_prepare_votes(m)
		if precommit_msg is not None:
			break
	
	precommit_votes = []	
	for r in replicas:
		precommit_votes.append(r.examine_precommit(precommit_msg))
	
	commit_msg = None
	for m in precommit_votes:
		commit_msg = leader.collect_precommit_votes(m)
		if commit_msg is not None:
			break	

	commit_votes = []
	for r in replicas:
		commit_votes.append(r.examine_commit(commit_msg))
	
	decide_msg = None	
	for m in commit_votes:
		decide_msg = leader.collect_commit_votes(m)
		if decide_msg is not None:
			break	

	for r in replicas:
		r.examine_decision(decide_msg)

