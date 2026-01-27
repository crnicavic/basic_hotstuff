import asyncio
from hotstuff_types import *
from network import *

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

	def start_timer(self, view=None):	
		self.current_view = view if view is not None else self.current_view
		if self.task is not None and self.timer_running:
			self.task.cancel()
		self.task = asyncio.create_task(self.on_timeout())

	def stop_timer(self):
		if self.task is not None and self.timer_running:
			self.task.cancel()

class Replica:
	def __init__(self, replica_id, network):
		self.replica_id = replica_id
		self.network = network
		self.current_view = 0
		self.current_proposal = None
		self.log = [GENESIS_BLOCK]
		self.pending_reqs = []
		
		self.new_view_msgs = {}
		self.prepare_votes = {}
		self.precommit_votes = {}
		self.commit_votes = {}
		
		self.high_prepare_qc = GENESIS_QC
		self.locked_qc = GENESIS_QC
		
		self.is_leader = False
		self.running = True

		self.pacemaker = Pacemaker(2.0, self.start_new_view)

	def trace(self, string):
		print(f"[R{self.replica_id}][HONEST] {string}")

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
		msg.sender = self.replica_id
		await self.network.send(recipient_id, msg)

	async def broadcast(self, msg):
		msg.sender = self.replica_id
		await self.network.broadcast(msg)

	async def handle_client_req(self, msg):
		self.network.client_respond(msg)

	# NEW-VIEW - replica
	async def start_new_view(self, new_view):
		if new_view <= self.current_view:
			return
		
		self.current_view = new_view
		self.pacemaker.start_timer(new_view)
		leader_id = self.pacemaker.get_leader(new_view)
		self.is_leader = (leader_id == self.replica_id)
		
		self.trace(f"Entering view {new_view} {'(LEADER)' if self.is_leader else ''}")
		
		msg = Protocol_message(
			Protocol_phase.NEW_VIEW,
			new_view,
			None,
			self.high_prepare_qc,
			self.replica_id
		)
		await self.send(leader_id, msg)

	# PREPARE - leader
	async def handle_new_view(self, msg):
		if not self.is_leader or \
			not matching_msg(msg, Protocol_phase.NEW_VIEW, self.current_view):
			return
		
		if msg.view_number not in self.new_view_msgs:
			self.new_view_msgs[msg.view_number] = []
		
		self.new_view_msgs[msg.view_number].append(msg)
		
		if len(self.new_view_msgs[msg.view_number]) == QUORUM:
			highest_qc = max(
				self.new_view_msgs[msg.view_number],
				key=lambda m: m.justify.view_number
			).justify
			
			if len(self.pending_reqs) <= 0:
				return
			cmds = self.pending_reqs.pop(0) 
			proposal_block = Block(
				cmds,
				highest_qc.block,
				self.current_view
			)
			
			proposal_msg = Protocol_message(
				Protocol_phase.PREPARE,
				self.current_view,
				proposal_block,
				highest_qc
			)
			
			self.trace(f"Leader proposing {proposal_block}")
			await self.broadcast(proposal_msg)

	# PREPARE - replica
	async def handle_prepare(self, msg):
		if not matching_msg(msg, Protocol_phase.PREPARE, self.current_view):
			return
		
		if self.extends(msg.block, msg.justify.block) \
			and self.safe_block(msg.block, msg.justify):
			self.pacemaker.stop_timer()
			self.trace(f"Voting for {msg.block}")
			self.current_proposal = msg.block

			partial_sig = Signature.partial_sign(
				self.current_view,
				Protocol_phase.PREPARE_VOTE,
				msg.block.hash
			)

			vote_msg = Protocol_message(
				Protocol_phase.PREPARE_VOTE,
				self.current_view,
				msg.block,
				None,
				partial_sig
			)

			leader_id = self.pacemaker.get_leader(self.current_view)
			self.pacemaker.start_timer()
			await self.send(leader_id, vote_msg)

	# PRECOMMIT - leader
	async def handle_prepare_vote(self, msg):
		if not self.is_leader or \
			not matching_msg(msg, Protocol_phase.PREPARE_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.prepare_votes:
			self.prepare_votes[msg.view_number] = []
		
		if msg.block.hash == self.current_proposal.hash:	
			self.prepare_votes[msg.view_number].append(msg)
		
		if len(self.prepare_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Protocol_phase.PREPARE,
				self.current_view,
				msg.block
			)
			for vote in self.prepare_votes[msg.view_number]:
				qc.signature.combine(vote.partial_sig)
			
			self.high_prepare_qc = qc
			
			self.trace(f"Leader formed {qc}")
			
			precommit_msg = Protocol_message(
				Protocol_phase.PRECOMMIT,
				self.current_view,
				None,
				qc
			)
			await self.broadcast(precommit_msg)

	# PRECOMMIT - replica
	async def handle_precommit(self, msg):
		if not matching_qc(msg.justify, Protocol_phase.PREPARE, self.current_view):
			return
		if not msg.justify.signature.verify():
			return
		
		if msg.justify.view_number > self.high_prepare_qc.view_number:
			self.high_prepare_qc = msg.justify
		self.pacemaker.stop_timer()
		partial_sig = Signature.partial_sign(
			self.current_view,
			Protocol_phase.PRECOMMIT_VOTE,
			msg.justify.block.hash
		)
	
		vote_msg = Protocol_message(
			Protocol_phase.PRECOMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			partial_sig
		)
		
		leader_id = self.pacemaker.get_leader(self.current_view)
		self.pacemaker.start_timer()
		await self.send(leader_id, vote_msg)

	# COMMIT - leader
	async def handle_precommit_vote(self, msg):
		if not self.is_leader or \
			not matching_msg(msg, Protocol_phase.PRECOMMIT_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.precommit_votes:
			self.precommit_votes[msg.view_number] = []
		
		#dont count bogus votes	
		if msg.block.hash == self.current_proposal.hash:
			self.precommit_votes[msg.view_number].append(msg)
		
		if len(self.precommit_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Protocol_phase.PRECOMMIT,
				self.current_view,
				msg.block
			)
			for vote in self.precommit_votes[msg.view_number]:
				qc.signature.combine(vote.partial_sig)
			
			self.trace(f"Leader formed {qc}")
			
			commit_msg = Protocol_message(
				Protocol_phase.COMMIT,
				self.current_view,
				None,
				qc
			)
			await self.broadcast(commit_msg)

	# COMMIT - replica
	async def handle_commit(self, msg):
		if not msg.justify.signature.verify() or \
			not matching_qc(msg.justify, Protocol_phase.PRECOMMIT, self.current_view):
			return
		
		if msg.justify.view_number > self.locked_qc.view_number:
			self.locked_qc = msg.justify

		self.pacemaker.stop_timer()	
		partial_sig = Signature.partial_sign(
			self.current_view,
			Protocol_phase.COMMIT_VOTE,
			msg.justify.block.hash
		)	
		vote_msg = Protocol_message(
			Protocol_phase.COMMIT_VOTE,
			self.current_view,
			msg.justify.block,
			None,
			partial_sig
		)
		
		leader_id = self.pacemaker.get_leader(self.current_view)
		self.pacemaker.start_timer()
		await self.send(leader_id, vote_msg)

	# DECIDE - leader
	async def handle_commit_vote(self, msg):
		if not self.is_leader or \
			not matching_msg(msg, Protocol_phase.COMMIT_VOTE, self.current_view):
			return
		
		if msg.view_number not in self.commit_votes:
			self.commit_votes[msg.view_number] = []
		
		if msg.block.hash == self.current_proposal.hash:
			self.commit_votes[msg.view_number].append(msg)
		
		if len(self.commit_votes[msg.view_number]) == QUORUM:
			qc = QC(
				Protocol_phase.COMMIT,
				self.current_view,
				msg.block
			)
			for vote in self.commit_votes[msg.view_number]:
				qc.signature.combine(vote.partial_sig)
			
			self.trace(f"Leader formed {qc}")
			
			decide_msg = Protocol_message(
				Protocol_phase.DECIDE,
				self.current_view,
				None,
				qc
			)
			await self.broadcast(decide_msg)

	# DECIDE - replica
	async def handle_decide(self, msg):
		if not msg.justify.signature.verify() or \
			not matching_qc(msg.justify, Protocol_phase.COMMIT, self.current_view):
			return
		
		self.trace(f"Executing {msg.justify.block.cmds}")
		self.log.append(msg.justify.block)
		
		# let others catch-up
		await asyncio.sleep(0.1)
		await self.start_new_view(self.current_view + 1)

	async def message_handler(self):
		while self.running:
			try:
				msg = await asyncio.wait_for(self.network.inbox.get(), timeout=1.0)
				if isinstance(msg, Client_request):
					self.handle_client_req(msg)
					continue
				
				match msg.phase:
					case Protocol_phase.NEW_VIEW:
						await self.handle_new_view(msg)
					case Protocol_phase.PREPARE:
						await self.handle_prepare(msg)
					case Protocol_phase.PREPARE_VOTE:
						await self.handle_prepare_vote(msg)
					case Protocol_phase.PRECOMMIT:
						await self.handle_precommit(msg)
					case Protocol_phase.PRECOMMIT_VOTE:
						await self.handle_precommit_vote(msg)
					case Protocol_phase.COMMIT:
						await self.handle_commit(msg)
					case Protocol_phase.COMMIT_VOTE:
						await self.handle_commit_vote(msg)
					case Protocol_phase.DECIDE:
						await self.handle_decide(msg)
					
			except asyncio.TimeoutError:
				continue

	async def run(self):
		await self.network.start_server()
		await asyncio.sleep(1)
		await self.start_new_view(1)
		await self.message_handler()

