from network import *
from replica import *
from protocol_types import *

class Crash_replica(Replica):
	def __init__(self, replica_id, network, crash_view):
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

		self.crash_view = crash_view

	def trace(self, string):
		print(f"[R{self.replica_id}][CRASH] {string}")

	async def message_handler(self):
		while self.running:
			if self.current_view == self.crash_view:
				self.trace(f"REPLICA CRASHED AT {self.current_view}")
				self.pacemaker.stop_timer()
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

class Delayed_replica(Replica):
	def trace(self, string):
		print(f"[R{self.replica_id}][DELAYED] {string}")

class Malicious_replica(Replica):
	def trace(self, string):
		print(f"[R{self.replica_id}][MALICIOUS] {string}")

class Delayed_network(Network):
	async def send(self, recipient_id, msg):
		if recipient_id == self.replica_id:
			await self.inbox.put(msg)
			return

		if not await self.connect(recipient_id):
			return

		_, writer = self.replica_conns[recipient_id]
		
		packet = pickle.dumps(msg)
		msg_byte_count = len(packet)

		await asyncio.sleep(0.1)
		writer.write(msg_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()

class Malicious_network(Network):
	async def broadcast(self, msg):
		tasks = []
		for replica_id in self.replica_addresses:
			mal_block = Block(
					"malicious_cmd_for_{replica_id}",
					msg.justify.block,
					msg.view_number
			)

			mal_msg = Message(
					Message_types.PREPARE,
					msg.view_number,
					mal_block,
					msg.justify,
					msg.sender
			)
			tasks.append(self.send(replica_id, mal_msg))
		await asyncio.gather(*tasks)

