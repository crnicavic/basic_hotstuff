import copy
import random
from hotstuff.network import *
from hotstuff.replica import *
from hotstuff.hotstuff_types import *

class Crash_replica(Replica):
	def __init__(self, replica_id, network, crash_view):
		super(Crash_replica, self).__init__(replica_id, network)
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
				payload = await asyncio.wait_for(self.network.inbox.get(), timeout=1.0)
				if isinstance(payload, Command):
					await self.handle_client_cmd(payload)
					continue
				
				match payload.phase:
					case Protocol_phase.NEW_VIEW:
						await self.handle_new_view(payload)
					case Protocol_phase.PREPARE:
						await self.handle_prepare(payload)
					case Protocol_phase.PREPARE_VOTE:
						await self.handle_prepare_vote(payload)
					case Protocol_phase.PRECOMMIT:
						await self.handle_precommit(payload)
					case Protocol_phase.PRECOMMIT_VOTE:
						await self.handle_precommit_vote(payload)
					case Protocol_phase.COMMIT:
						await self.handle_commit(payload)
					case Protocol_phase.COMMIT_VOTE:
						await self.handle_commit_vote(payload)
					case Protocol_phase.DECIDE:
						await self.handle_decide(payload)
					
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

		await asyncio.sleep(0.01 * msg.view_number)
		writer.write(msg_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()

class Malicious_network(Network):
	async def broadcast(self, msg):
		tasks = []
		for replica_id in self.replica_addresses:
			cmd = msg.block.cmd
			mal_cmd = Command(cmd.op, copy.deepcopy(cmd.args), cmd.client_id)
			mal_cmd.args[1] = random.randint(1, 1000) 
			mal_cmd.hash = mal_cmd.calculate_hash()
			mal_block = Block(
					mal_cmd,
					msg.justify.block,
					msg.view_number
			)

			mal_msg = Message(
					Protocol_phase.PREPARE,
					msg.view_number,
					mal_block,
					msg.justify,
					msg.sender
			)
			tasks.append(self.send(replica_id, mal_msg))
		await asyncio.gather(*tasks)

