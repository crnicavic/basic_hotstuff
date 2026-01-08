from concurrent.futures import ThreadPoolExecutor

N = 4
F = 1
QUORUM = N - F
NEW_VIEW = 5421315

class Block:
	def __init__(self, cmds, parent, view):
		self.cmds = cmds
		self.parent = parent
		self.height = view;
	
	def __str__():
		return f"Block(height:{self.height})"

ZERO_BLOCK = Block([], None, 0)

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

class QC:
	def __init__(self, qc_type, view_number, node):
		self.qc_type = qc_type
		self.view_number = view_number
		self.node = node
		self.voters = []

	def __str__():
		ret = f"QC(type:{self.qc_type};"
		ret += f"view_number:{self.view_number};"
		ret += f"block:{self.node};"
		ret += f"voters:{self.voters};)"
		return ret

class Replica:
	log = []

	def __init__(self, replica_id):
		self.replica_id = replica_id
		self.current_view = 0
		self.log.append(ZERO_BLOCK)
		self.new_view_msgs = {}
		self.high_prepare_qc = None

	def on_client_request(self, replicas):
		print("received request from client")
		self.current_view += 1
		leader_id = self.current_view % N
		new_view_msg = Message(NEW_VIEW, self.current_view, None, self.high_prepare_qc)	
		if leader_id == self.replica_id:
			# leaders sends message to themselves
			if new_view_msg.view_number not in self.new_view_msgs:
				self.new_view_msgs[new_view_msg.view_number] = [new_view_msg]
			elif len(self.new_view_msgs[new_view_msg.view_number]) <= QUORUM:
				self.new_view_msgs[new_view_msg.view_number].append(new_view_msg)
			return
		
		replicas[leader_id].on_new_view(new_view_msg)
		return

	def on_new_view(self, msg):
		if msg.view_number not in self.new_view_msgs:
			self.new_view_msgs[msg.view_number] = [msg]
		elif len(self.new_view_msgs[msg.view_number]) <= QUORUM:
			self.new_view_msgs[msg.view_number].append(msg)
		else:
			self.new_view_msgs[msg.view_number].sort(key=lambda qc: qc.view_number)
		return

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
