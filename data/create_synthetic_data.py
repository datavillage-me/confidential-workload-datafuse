"""
Create synthetic data for the demo use case
"""

import random
import json

import duckdb
from duckdb.typing import *

from faker import Faker
from hashlib import sha256



# Locales for Europe, the UK, and North America
locales = [
    # Europe
    'bg_BG',  # Bulgarian (Bulgaria)
    'cs_CZ',  # Czech (Czech Republic)
    'da_DK',  # Danish (Denmark)
    'de_AT',  # German (Austria)
    'de_CH',  # German (Switzerland)
    'de_DE',  # German (Germany)
    'el_GR',  # Greek (Greece)
    'en_IE',  # English (Ireland)
    'en_GB',  # English (United Kingdom)
    'es_ES',  # Spanish (Spain)
    'et_EE',  # Estonian (Estonia)
    'fi_FI',  # Finnish (Finland)
    'fr_BE',  # French (Belgium)
    'fr_FR',  # French (France)
    'fr_CH',  # French (Switzerland)
    'hr_HR',  # Croatian (Croatia)
    'hu_HU',  # Hungarian (Hungary)
    'it_IT',  # Italian (Italy)
    'lt_LT',  # Lithuanian (Lithuania)
    'lv_LV',  # Latvian (Latvia)
    'nl_BE',  # Dutch (Belgium)
    'nl_NL',  # Dutch (Netherlands)
    'no_NO',  # Norwegian (Norway)
    'pl_PL',  # Polish (Poland)
    'pt_PT',  # Portuguese (Portugal)
    'ro_RO',  # Romanian (Romania)
    'ru_RU',  # Russian (Russia)
    'sk_SK',  # Slovak (Slovakia)
    'sl_SI',  # Slovenian (Slovenia)
    'sv_SE',  # Swedish (Sweden)
    'uk_UA',  # Ukrainian (Ukraine)

    # UK
    'en_GB',  # English (United Kingdom)

    # North America
    'en_CA',  # English (Canada)
    'en_US',  # English (United States)
    'es_MX',  # Spanish (Mexico)
    'fr_CA',  # French (Canada)
]

currentLocale="fr_CA"

# Commutative encryption: E_k(x) = x^k mod n
def commutative_encrypt(value, key, n):
    return pow(value, key, n)

# Securely hash email to integers
def hash_email_to_int(email, n):
    digest = sha256(email.encode('utf-8')).digest()
    return int.from_bytes(digest, 'big') % n

# Encrypt email 
def encrypt_email(email):
    email_int = hash_email_to_int(email, encrypt_n)
    return commutative_encrypt(email_int, public_key, encrypt_n)


def random_id(n):
    return random.randrange(1000,9999999999999)


def random_email(n):
    i=random.randrange(0, 36)
    currentLocale=locales[i]
    fake = Faker(currentLocale)
    fake.seed_instance(int(n*10))

    email=fake.unique.ascii_email()
    #force at least one common item
    if n==1:
        email="john.doe@example.com"
    return str(email)

duckdb.create_function("id", random_id, [DOUBLE], VARCHAR)
duckdb.create_function("email", random_email, [DOUBLE], VARCHAR)

participants=["66e1a579eb0cbee048a2bd04","66e1a4eaeb0cbee048a2bcf3"]
encryption_keys = {}
encryption_keys["66e1a579eb0cbee048a2bd04"]="GZs0DsMHdXr39mzkFwHwTHvCuUlID3HB"
encryption_keys["66e1a4eaeb0cbee048a2bcf3"]="8SX9rT9VSHohHgEz2qRer5oCoid2RUAS"

# Generate synthetic datasets
numberOfRecords=2

with open('tests/fixtures/shared_modulus.json') as f:
    d = json.load(f)
encrypt_n=d["n"]

with open('tests/fixtures/public_keys.json') as f:
    d = json.load(f)
public_keys=d
i=0
for participant in participants:
    public_key=public_keys[participant]
    #create temp table
    query="SELECT id(i) as customer_id, email(i) as customer_email FROM generate_series(1, "+str(numberOfRecords)+") s(i)"
    res=duckdb.sql("CREATE OR REPLACE TABLE customers_list AS "+query) 

    #create parquet file non encrypted
    query="COPY customers_list TO 'data/customers-list"+str(i)+".parquet'  (FORMAT 'parquet')"
    res = duckdb.sql(query)

    #create encrypted parquet file
    account_ids=duckdb.sql("SELECT customer_email from customers_list").df()
    for index, row in account_ids.iterrows():
        query="UPDATE customers_list SET customer_email = '"+str(encrypt_email(row['customer_email']))+"' WHERE customers_list.customer_email='"+row['customer_email']+"'"
        res=duckdb.sql(query)

    key = encryption_keys[participant]
    keyName="dataset"+participant
    res=duckdb.sql("PRAGMA add_parquet_key('"+keyName+"','"+key+"')")
    res=duckdb.sql("COPY customers_list TO './data/customers-list"+str(i)+"-encrypted.parquet' (ENCRYPTION_CONFIG {footer_key: '"+keyName+"'})")
    i=i+1

df = duckdb.sql("SELECT * FROM read_parquet('data/customers-list0.parquet')").df()
print (df)
df = duckdb.sql("SELECT * FROM read_parquet('data/customers-list1.parquet')").df()
print (df)


# duckdb.sql("IMPORT DATABASE 'tests/fixtures'")
# df=duckdb.sql("SELECT * FROM customers_list_0").df()
# print (df)
# df=duckdb.sql("SELECT * FROM customers_list_1").df()
# print (df)


    