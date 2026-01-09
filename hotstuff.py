from enum import Enum

N = 4
F = 1
QUORUM = 2*F+1
NEW_VIEW = 5421315

class Message_types:
	NEW_VIEW = 5421315
	PREPARE = 5421316
	PREPARE_VOTE = 5421317
	PRECOMMIT = 5421318

class Block:
	def __init__(self, cmds, parent, view):
		self.cmds = cmds
		self.parent = parent
		self.height = view;
	
	def __str__(self):
		return f"Block(height:{self.height})"

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

class Replica:
	def __init__(self, replica_id):
		self.replica_id = replica_id
		self.current_view = 0
		self.log = [GENESIS_BLOCK]
		# dictionaries of arrays per view (later use only one array)
		self.new_view_msgs = {}
		self.prepare_votes = {}
		self.high_prepare_qc = GENESIS_QC
		self.replicas = []

	def extends(self, block):
		for b in self.log:
			# TODO: compare some sort of hash instead of height
			if b.height == block.parent.height:
				return True
		return False

	def on_client_request(self, view):
		#print("received request from client")
		# i don't want unpredictable behaviour in this model
		if view < self.current_view:
			return

		leader = self.replicas[(view + 1) % N]
		new_view_msg = Message(NEW_VIEW, self.current_view, None, self.high_prepare_qc)	
		leader.on_new_view(new_view_msg)
		return

	def on_new_view(self, msg):
		if msg.view_number not in self.new_view_msgs:
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
			for r in self.replicas:
				r.examine_prepare_proposal(proposal_msg)
		return

	def examine_prepare_proposal(self, msg):
		if msg.view_number > self.current_view:
			self.current_view = msg.view_number
		leader = self.replicas[msg.view_number % N]
		print(f"received proposal for {msg.block} from {leader}")
		if self.extends(msg.block):
			#send vote or rather send signature
			print(f"replica {self.replica_id} voted for {msg.block}")
			vote_msg = Message(
				Message_types.PREPARE_VOTE,
				self.current_view,
				msg.block,
				None, # specification doesnt send this
				self.replica_id # this is the signature :D
			)
			leader.on_prepare_vote(vote_msg)
	
	def on_prepare_vote(self, vote_msg):
		if vote_msg.view_number not in self.prepare_votes:
			self.prepare_votes[vote_msg.view_number] = [vote_msg]
		else:
			self.prepare_votes[vote_msg.view_number].append(vote_msg)

		if len(self.prepare_votes[vote_msg.view_number]) == QUORUM:
			qc = QC(
				Message_types.PRECOMMIT,
				self.current_view,
				vote_msg.block
			)
			for m in self.prepare_votes[vote_msg.view_number]:
				qc.voters.append(m.partial_sig)
			self.high_prepare_qc = qc
			print(f"leader formed QC {qc}")
			precommit_msg = Message(
				Message_types.PRECOMMIT,
				self.current_view,
				msg.block,
				qc
			)
			for r in self.replicas:
				r.examine_precommit(precommit_msg)

	def __str__(self):
		ret = f"replica_id:{self.replica_id} on view:{self.current_view}"
		return ret
	

if __name__ == "__main__":
	replicas = [Replica(i) for i in range(N)] 
	for r in replicas:
		r.replicas = replicas

	# simulate client sending a request
	for r in replicas:
		r.on_client_request(0) 
	
#	for r in replicas:
		#print(r)
		#print(r.new_view_msgs)
