import asyncio
import hashlib
import random
from collections import Counter
from hotstuff_types import *
from network import *
from replica import *
from byzantine import *

class Client:
	def __init__(self, client_id, replica_addresses):
		self.client_id = client_id
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

	async def trace(self, string):
		print(f"[C{self.client_id}] {string}")

	async def send_request(self, recipient_id, req):
		# SEND
		if not await self.connect(recipient_id):
			return	
		reader, writer = self.replica_conns[recipient_id]
		
		packet = pickle.dumps(req)
		packet_byte_count = len(packet)

		writer.write(packet_byte_count.to_bytes(4, 'big'))
		writer.write(packet)
		await writer.drain()
		# RECV
		# first 32 bits of message are the byte count
		packet_byte_count = await reader.readexactly(4)
		packet_byte_count = int.from_bytes(msg_byte_count, 'big')				

		packet = await reader.read(msg_byte_count)
		if not packet:
			return	

		response = pickle.loads(packet)
		self.responses.append(response)

	async def broadcast_request(self, req):
		tasks = []
		for replica_id in self.replica_addresses:
			tasks.append(self.send_request(replica_id, req))
		await asyncio.gather(*tasks)

async def simulation():
	replica_addresses = {
		0: ('127.0.0.1', 50000),
		1: ('127.0.0.1', 50001),
		2: ('127.0.0.1', 50002),
		3: ('127.0.0.1', 50003)
	}	
	replica_types = {
		0: Fault_types.CRASH,
		1: Fault_types.HONEST,
		2: Fault_types.HONEST,
		3: Fault_types.HONEST
	}
	replicas = []
	for i in range(N):
		crash_view = 10
		if replica_types[i] == Fault_types.HONEST:
			network = Network(i, '127.0.0.1', 50000+i)
			network.replica_addresses = replica_addresses
			replica = Replica(i, network)
		elif replica_types[i] == Fault_types.CRASH:
			network = Network(i, '127.0.0.1', 50000+i)
			network.replica_addresses = replica_addresses
			replica = Crash_replica(i, network, crash_view)
		elif replica_types[i] == Fault_types.DELAYED:
			network = Delayed_network(i, '127.0.0.1', 50000+i)
			network.replica_addresses = replica_addresses
			replica = Delayed_replica(i, network)
		elif replica_types[i] == Fault_types.MALICIOUS:
			network = Malicious_network(i, '127.0.0.1', 50000+i)
			network.replica_addresses = replica_addresses
			replica = Malicious_replica(i, network)
		replicas.append(replica)
	
	client = Client(0, replica_addresses) 
	tasks = [replica.run() for replica in replicas]
	cmd = Command("SET", ["A", 10], client.client_id)
	tasks.append(client.send_request(2, cmd))
	try:
		# run for 5 secs
		await asyncio.wait_for(
			asyncio.gather(*tasks),
			timeout=10.0
		)
	except asyncio.TimeoutError:
		
		for replica in replicas:
			replica.running = False
		
		for replica in replicas:
			print(f"Replica {replica.replica_id}: log length={len(replica.log)}, "
			      f"locked view={replica.locked_qc.view_number}")
			print(f"STATE: {replica.state}")
			await replica.network.stop_server()

async def main():
	await simulation()

if __name__ == "__main__":
	asyncio.run(main())
