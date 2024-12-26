from charm.toolbox.pairinggroup import PairingGroup
from charm.toolbox.ABE import CPabe_BSW07

# Initialize Pairing Group and ABE Scheme
group = PairingGroup('SS512')  # Choose a pairing-friendly curve
abe = CPabe_BSW07(group)

# Setup: Generate master public and secret keys
public_key, master_secret_key = abe.setup()

print("Public Key:", public_key)
print("Master Secret Key (stored securely):", master_secret_key)

# Key Generation: Generate private keys for participants based on their attributes
def generate_private_key(attribute):
    """
    Generate a private key for a participant with the given attribute.
    """
    private_key = abe.keygen(public_key, master_secret_key, [attribute])
    print(f"Private Key for attribute '{attribute}': {private_key}")
    return private_key

# Encryption: Encrypt IBAN based on access policy
def encrypt_iban(iban, access_policy):
    """
    Encrypt an IBAN using the public key and access policy.
    """
    plaintext = group.hash(iban, group.GT)  # Hash the IBAN into a group element
    ciphertext = abe.encrypt(public_key, plaintext, access_policy)
    print(f"Ciphertext for IBAN '{iban}': {ciphertext}")
    return ciphertext

# Decryption: Decrypt IBAN using a private key
def decrypt_iban(ciphertext, private_key):
    """
    Decrypt a ciphertext using the participant's private key.
    """
    try:
        decrypted_plaintext = abe.decrypt(public_key, private_key, ciphertext)
        return decrypted_plaintext
    except Exception as e:
        print(f"Decryption failed: {e}")
        return None

# Example Workflow
if __name__ == "__main__":
    # Attributes and access policy
    participant_attribute = "BIC:Participant1"
    access_policy = "(BIC:Participant1 OR BIC:Participant2)"

    # Step 1: Generate private key for the participant
    private_key = generate_private_key(participant_attribute)

    # Step 2: Encrypt an IBAN
    iban = "IBAN123456789"
    ciphertext = encrypt_iban(iban, access_policy)

    # Step 3: Decrypt the ciphertext
    decrypted_plaintext = decrypt_iban(ciphertext, private_key)

    # Verify if decryption was successful
    if decrypted_plaintext == group.hash(iban, group.GT):
        print(f"Decryption successful! IBAN: {iban}")
    else:
        print("Decryption failed or unauthorized access.")