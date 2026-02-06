import asyncio
from hotstuff.hotstuff_types import *
from hotstuff.network import *
from hotstuff.replica import *
from hotstuff.byzantine import *
from hotstuff.client import *

N = 4

async def main():
	replica_addresses = {
		0: ('127.0.0.1', 50000),
		1: ('127.0.0.1', 50001),
		2: ('127.0.0.1', 50002),
		3: ('127.0.0.1', 50003)
	}	
	replica_types = {
		0: Fault_types.DELAYED,
		1: Fault_types.HONEST,
		2: Fault_types.HONEST,
		3: Fault_types.HONEST
	}
	replicas = []
	for i in range(N):
		address, port = replica_addresses[i]
		if replica_types[i] == Fault_types.HONEST:
			network = Network(i, replica_addresses, address, port)
			replica = Replica(i, network)
		elif replica_types[i] == Fault_types.DELAYED:
			network = Delayed_network(i, replica_addresses, address, port)
			replica = Delayed_replica(i, network)
		replicas.append(replica)
	
	client = Client(0, replica_addresses, 3.0) 
	tasks = [replica.run() for replica in replicas]
	tasks.append(client.run())
	try:
		# run for 5 secs
		await asyncio.wait_for(
			asyncio.gather(*tasks),
			timeout=20.0
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
