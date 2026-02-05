import asyncio
from hotstuff.hotstuff_types import *
from hotstuff.network import *
from hotstuff.replica import *
from hotstuff.byzantine import *
from hotstuff.client import *

N = 90

async def main():
	replica_addresses = {}
	for i in range(N):
		replica_addresses[i] = ('127.0.0.1', 50000 + i)

	replicas = []
	for replica_id in replica_addresses:
		address, port = replica_addresses[replica_id]
		network = Network(replica_id, address, port)
		network.replica_addresses = replica_addresses
		replica = Replica(replica_id, network, 100)
		replicas.append(replica)

	client = Client(0, replica_addresses, 100.0) 
	tasks = [replica.run() for replica in replicas]
	tasks.append(client.run())
	try:
		await asyncio.wait_for(
			asyncio.gather(*tasks),
			timeout=40.0
		)
	except asyncio.TimeoutError:
		
		for replica in replicas:
			replica.running = False
		
		for replica in replicas:
			print(f"Replica {replica.replica_id}: log length={len(replica.log)}, "
			      f"locked view={replica.locked_qc.view_number}")
			print(f"STATE: {replica.state}")
			await replica.network.stop_server()


if __name__ == "__main__":
	asyncio.run(main())
