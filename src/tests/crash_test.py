N = 4
F = 1
QUORUM = 2*F+1

import asyncio
from hotstuff.hotstuff_types import *
from hotstuff.network import *
from hotstuff.replica import *
from hotstuff.byzantine import *
from hotstuff.client import *

async def main():
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
	
	client = Client(0, replica_addresses, 3.0) 
	tasks = [replica.run() for replica in replicas]
	tasks.append(client.run())
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

if __name__ == "__main__":
	asyncio.run(main())
