import pickle
import asyncio
from hotstuff.hotstuff_types import *

# replica's way of talking with the world
class Network:
	def __init__(self, replica_id, host='127.0.0.1', port=50000):
		# the id of the replica who uses the object
		self.replica_id = replica_id
		self.inbox = asyncio.Queue()
		self.replica_addresses = {} # replica_id -> (host: string, port: int)
		self.replica_conns = {} # replica_id -> (reader, writer)
		self.client_conns = {} # client_id -> (reader, writer)
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
				await asyncio.sleep(0.2)
		return False

	async def recv(self, reader, writer):
		try:
			while True:
				# first 32 bits of message are the byte count
				packet_byte_count = await reader.readexactly(4)
				packet_byte_count = int.from_bytes(packet_byte_count, 'big')				

				packet = await reader.read(packet_byte_count)
				if not packet:
					continue
				payload = pickle.loads(packet)
				if isinstance(payload, Command):
					self.client_conns[payload.client_id] = (reader, writer)
				await self.inbox.put(payload)
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

		_, writer = self.replica_conns[recipient_id]
		
		packet = pickle.dumps(msg)
		msg_byte_count = len(packet)

		writer.write(msg_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()

	async def client_respond(self, cmd):
		if cmd.client_id not in self.client_conns:
			return
		reader, writer = self.client_conns[cmd.client_id]
		packet = pickle.dumps(cmd)
		cmd_byte_count = len(packet)
		writer.write(cmd_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()

	async def broadcast(self, msg):
		tasks = []
		for replica_id in self.replica_addresses:
			tasks.append(self.send(replica_id, msg))
		await asyncio.gather(*tasks)

	async def stop_server(self):
		self.server.close()
		for replica_id in self.replica_conns:
			_, writer = self.replica_conns[replica_id]
			writer.close()
			await writer.wait_closed()

