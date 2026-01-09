from enum import Enum

N = 4
F = 1
QUORUM = 2*F+1
NEW_VIEW = 5421315

class Message_types:
	NEW_VIEW = 5421315
	PREPARE = 5421316

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
	def __init__(self, msg_type, view_number, block, qc):
		self.msg_type = msg_type
		self.view_number = view_number
		self.block = block
		self.justify = qc 
	
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
		self.new_view_msgs = {}
		self.high_prepare_qc = GENESIS_QC

	def on_client_request(self, replicas):
		print("received request from client")
		leader_id = (self.current_view + 1)% N
		new_view_msg = Message(NEW_VIEW, self.current_view, None, self.high_prepare_qc)	
		self.current_view += 1
		replicas[leader_id].on_new_view(new_view_msg, replicas)
		return

	def on_new_view(self, msg, replicas):
		if msg.view_number not in self.new_view_msgs:
			self.new_view_msgs[msg.view_number] = [msg]
		else:
			self.new_view_msgs[msg.view_number].append(msg)

		if len(self.new_view_msgs[msg.view_number]) == QUORUM:
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
			for r in replicas:
				if r.replica_id != self.replica_id:
					# also send who the message is from, so the replica knows
					# where to send the vote
					r.examine_prepare_proposal(proposal_msg, replicas[self.replica_id])
		return

	def examine_prepare_proposal(self, message, leader):
		print(f"received proposal for {message.block} from {leader}")

	def __str__(self):
		ret = f"replica_id:{self.replica_id} on view:{self.current_view}"
		return ret
	

if __name__ == "__main__":
	replicas = [Replica(i) for i in range(N)] 
	# simulate client sending a request
	for r in replicas:
		r.on_client_request(replicas) 
	
	for r in replicas:
		print(r)
		print(r.new_view_msgs)
