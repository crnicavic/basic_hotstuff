import pickle
import ggmpc
import ggmpc.curves as curves

mpc = ggmpc.Eddsa(curves.ed25519)

"""
	key_share return a dict with 3 parts, one part of it's own private key,
	one part of the remaining keys.
	in context of B:
	{
		'1': private key part for A
		'2': own private key part
		'3': private key part for C
	}
"""
A = mpc.key_share(1, 2, 3)
B = mpc.key_share(2, 2, 3)
C = mpc.key_share(3, 2, 3)

"""
	key_combine takes the shares, and it creates a full private key
	for each of the participantsi n the threshold scheme
"""
A, B, C = \
  mpc.key_combine((A[1], B[1], C[1])), \
  mpc.key_combine((A[2], B[2], C[2])), \
  mpc.key_combine((A[3], B[3], C[3]))
print(A, B)
M = b'MPC on a Friday night'

"""
	sign_share does the same shit as key share only for the signature,
	it generates parts of the signature, one part for itself, one part for
	the other signer.

	NOTE: It does so from it's own private key!
	It also generates something called a nonce, which seems to be a very
	important random number, that is used to generate the encrypted message.
	
	IT CANNOT BE REUSED, because if that happens, the private key can be
	extracted from the signature!
"""
A, B = mpc.sign_share(M, (A[1], A[2])), mpc.sign_share(M, (B[1], B[2]))

"""
	Actually contribute the part of the signature
"""
A, B = mpc.sign(M, (A[1], B[1])), mpc.sign(M, (A[2], B[2]))

print(A, B)

sig = mpc.sign_combine((A, B))

print(mpc.verify(M, sig))
