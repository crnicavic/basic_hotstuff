import asyncio
from hotstuff.hotstuff_types import *

class Client:
	def __init__(self, client_id, replica_addresses, timeout):
		self.client_id = client_id
		self.timeout = timeout
		self.replica_addresses = replica_addresses
		self.replica_conns = {}
		self.responses = []

	async def connect(self, replica_id):
		if replica_id in self.replica_conns:
			return True

		if replica_id not in self.replica_addresses:
			return False
		
		for attempt in range(3):
			try:
				host, port = self.replica_addresses[replica_id]
				self.replica_conns[replica_id] = await asyncio.open_connection(host, port)
				return True
			except ConnectionRefusedError:
				print("refused")
				await asyncio.sleep(0.2)
		return False

	def trace(self, string):
		print(f"[C{self.client_id}] {string}")

	async def send_cmd(self, recipient_id, cmd):
		# SEND
		if not await self.connect(recipient_id):
			return	
		reader, writer = self.replica_conns[recipient_id]
		
		packet = pickle.dumps(cmd)
		packet_byte_count = len(packet)

		writer.write(packet_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()
		# RECV
		# first 32 bits of message are the byte count
		packet_byte_count = await reader.readexactly(4)
		packet_byte_count = int.from_bytes(packet_byte_count, 'big')				

		packet = await reader.read(packet_byte_count)
		if not packet:
			return	

		response = pickle.loads(packet)
		return response

	async def broadcast_cmd(self, cmd):
		responses = []
		pending = []
		for replica_id in self.replica_addresses:
			pending.append(asyncio.create_task(self.send_cmd(replica_id, cmd)))

		while len(responses) < F + 1:
			done, pending = await asyncio.wait(
				pending,
				timeout=self.timeout,
				return_when=asyncio.FIRST_COMPLETED
			)
			for task in done:
				if task.exception() is None and task.result() is not None:
					responses.append(task.result())
			if not pending:
				break
		print(responses)

	async def run(self):
		while True:
			cmd = Command("SET", ["A", 10], self.client_id)
			await self.broadcast_cmd(cmd)
