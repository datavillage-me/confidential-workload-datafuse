from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
import os

# Step 1: Generate encryption keys (simplified for demonstration)
def generate_rsa_keypair():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    return private_key, public_key

def serialize_key(key, private=False):
    if private:
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    return key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

# Step 2: Commutative encryption of PII
def commutative_encrypt(data, public_key):
    return public_key.encrypt(
        data,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

def commutative_decrypt(data, private_key):
    return private_key.decrypt(
        data,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

# Step 3: Example TEE-secure processing (mocked for demonstration)
def secure_processing_in_tee(encrypted_data, model):
    # Mock TEE environment: Decrypt data securely
    processed_data = f"Processed: {encrypted_data.decode()}"
    return processed_data.encode()

# Simulating the flow
if __name__ == "__main__":
    # Generate keypairs for Bank and Regulator
    bank_private_key, bank_public_key = generate_rsa_keypair()
    regulator_private_key, regulator_public_key = generate_rsa_keypair()

    # Original data (PII)
    pii_data = b"CustomerID: 12345, Name: John Doe, Account: 987654321"

    # Bank encrypts the data
    encrypted_by_bank = commutative_encrypt(pii_data, bank_public_key)

    # Regulator adds its own encryption layer
    doubly_encrypted_data = commutative_encrypt(encrypted_by_bank, regulator_public_key)

    # Secure processing within TEE
    # TEE would internally decrypt in reverse order
    decrypted_by_regulator = commutative_decrypt(doubly_encrypted_data, regulator_private_key)
    decrypted_by_bank = commutative_decrypt(decrypted_by_regulator, bank_private_key)

    # Simulate TEE processing on decrypted data
    processed_result = secure_processing_in_tee(decrypted_by_bank, "ExampleModel")

    print("Original Data:", pii_data)
    print("Processed Result:", processed_result.decode())
