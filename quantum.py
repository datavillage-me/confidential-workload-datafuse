import oqs

# Parameters for commutative encryption
MODULUS = 3329  # Must be a prime number
MULT_KEY = 17   # A fixed key for commutative encryption, must be coprime to MODULUS

def commutative_encrypt(message, key):
    """
    Encrypts a message using a commutative encryption scheme.
    The operation is simple modular multiplication.
    """
    return (message * key) % MODULUS

def commutative_decrypt(encrypted_message, key):
    """
    Decrypts a message encrypted using commutative encryption.
    Requires the modular multiplicative inverse of the key.
    """
    # Compute modular multiplicative inverse of key modulo MODULUS
    key_inv = pow(key, -1, MODULUS)
    return (encrypted_message * key_inv) % MODULUS

def kyber_key_exchange():
    """
    Demonstrates Kyber key encapsulation to establish shared keys.
    """
    # Initialize Kyber key encapsulation mechanism
    kem = oqs.KeyEncapsulation("Kyber512")
    
    # Generate keypair (party A)
    public_key_A, secret_key_A = kem.generate_keypair()
    
    # Party B encapsulates a shared secret using A's public key
    ciphertext, shared_secret_B = kem.encap_secret(public_key_A)
    
    # Party A decapsulates the ciphertext to get the shared secret
    shared_secret_A = kem.decap_secret(ciphertext, secret_key_A)
    
    assert shared_secret_A == shared_secret_B, "Shared secrets do not match!"
    return shared_secret_A  # Both parties now share this secret

# Kyber-based key exchange
shared_key = kyber_key_exchange()

# Convert shared key to integer for commutative encryption
shared_key_int = int.from_bytes(shared_key[:4], 'big')  # Use first 4 bytes as an integer
print(f"Shared Key (integer): {shared_key_int}")

# Message to encrypt
message = 123  # Example plaintext message

# Commutative encryption
encrypted_message = commutative_encrypt(message, shared_key_int)
print(f"Encrypted Message: {encrypted_message}")

# Commutative decryption
decrypted_message = commutative_decrypt(encrypted_message, shared_key_int)
print(f"Decrypted Message: {decrypted_message}")

# Verify correctness
assert message == decrypted_message, "Decryption failed!"
