"""
DataFuse enables datasets to be matched across multiple parties without exposing sensitive information. 
Unlike classical hash mechanisms, where a leaked salt can compromise data security, 
DataFuse ensures that if one party’s encryption keys are leaked, the other parties remain unaffected. 
Their data remains fully protected.
"""

import logging
import time
import json
import duckdb
import secrets 
from math import gcd
from sympy import nextprime

from dv_utils import default_settings, Client, ContractManager,audit_log,LogLevel

logger = logging.getLogger(__name__)

# let the log go to stdout, as it will be captured by the cage operator
logging.basicConfig(
    level=default_settings.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# define an event processing function
def event_processor(evt: dict):
    """
    Process an incoming event
    Exception raised by this function are handled by the default event listener and reported in the logs.
    """
    logger.info(f"Processing event {evt}")
    
    # dispatch events according to their type
    evt_type =evt.get("type", "")

    if evt_type == "INITIALIZE":
        # use the INITIALIZE event processor dedicated function
        logger.info(f"Use the initialize event processor")
        initialize_event_processor(evt)
    elif evt_type == "FUSE":
        # use the FUSE event processor dedicated function
        logger.info(f"Use the fuse event processor")
        fuse_event_processor(evt)
    elif evt_type == "CHECK_DATA_QUALITY":
        # use the CHECK_DATA_QUALITY event processor dedicated function
        logger.info(f"Use the check data quality event processor")
        check_data_quality_contracts_event_processor(evt)
    elif evt_type == "CHECK_COMMON_DEMO_CUSTOMERS":
        # use the CHECK_COMMON__DEMO_CUSTOMERS event processor dedicated function
        logger.info(f"Use the check common customers demo event processor")
        check_common_customers_demo_event_processor(evt)
    else:
        # use the GENERIC event processor function, that basicaly does nothing
        logger.info(f"Unhandled message type, use the generic event processor")
        generic_event_processor(evt)


def generic_event_processor(evt: dict):
    pass

# Generate large prime modulus n and phi(n)
def tee_generate_shared_modulus():
    p = nextprime(secrets.randbelow(2**1023) + 2**1022)
    q = nextprime(secrets.randbelow(2**1023) + 2**1022)
    n = p * q
    phi = (p - 1) * (q - 1)
    return n, phi

# Generate commutative encryption keys (k, d such that k * d = 1 mod phi)
def generate_unique_commutative_keys(phi, num_keys):
    """
    Generate a specified number of unique commutative key pairs (k, d) 
    for the same modulus φ(n).
    """
    used_keys = set()
    keys = []

    while len(keys) < num_keys:
        # Generate random k
        k = secrets.randbelow(phi - 2) + 2  # Ensure k is in range [2, φ(n)-1]
        if gcd(k, phi) == 1 and k not in used_keys:  # Ensure k is coprime and unique
            d = pow(k, -1, phi)  # Modular inverse of k
            keys.append(d)
            used_keys.add(k)  # Mark k as used

    return keys

# TEE generates and distributes keys
def tee_initialize(participants_ids):
    """
    TEE generates keys for each company and shares n and public keys.
    """
    n, phi = tee_generate_shared_modulus()
    public_keys = {}
    keys=generate_unique_commutative_keys(phi,len(participants_ids))
    i=0
    for participant in participants_ids:
       # _, public_key = generate_commutative_key(phi)  # Generate public key
        public_keys[participant] = keys[i]
        i=i+1
    return n, phi, public_keys  # Only public keys are shared

def initialize_event_processor(evt: dict):
    logger.info(f"---------------------------------------------------------")
    logger.info(f"|                    START PROCESSING                   |")
    logger.info(f"|                                                       |")
    start_time = time.time()
    logger.info(f"|    Start time:  {start_time} secs               |")
    logger.info(f"|                                                       |")
    audit_log(f"Start processing event: {evt.get('type', '')}.",LogLevel.INFO)
    try:
        collaboration_space_id=default_settings.collaboration_space_id
        logger.info(f"| 1. Get participants                                   |")
        logger.info(f"|                                                       |")
        client=Client()
        participants=client.get_list_of_participants(collaboration_space_id,None)
        if participants != None and len(participants)>0:
            participants_ids=[]
            #get all participants with role != code provider
            for participant in participants:
                if participant["role"]!="CodeProvider":
                    participants_ids.append(participant["clientId"])
            logger.info(f"| 2. Initialize keys                                    |")
            logger.info(f"|                                                       |")
            n, phi, public_keys = tee_initialize(participants_ids)
            #store public keys and n for each participants
            for participant_id in participants_ids:
                public_key={}
                public_key["n"]=n
                public_key["public-key"]=public_keys[participant_id]
                with open(default_settings.data_user_output_location+'/'+participant_id+'_keys.json', 'w', newline='') as file:
                    file.write(json.dumps(public_key, indent=4))
            
            #store shared modulus in secret store 
            shared_modulus={}
            shared_modulus["phi"]=phi
            shared_modulus["n"]=n
            with open(default_settings.data_connector_config_location+'/shared_modulus.json', 'w', newline='') as file:
                file.write(json.dumps(shared_modulus, indent=4))
            #store all public keys in secret store
            with open(default_settings.data_connector_config_location+'/public_keys.json', 'w', newline='') as file:
                file.write(json.dumps(public_keys, indent=4))

            logger.info(f"|                                                       |")
            execution_time=(time.time() - start_time)
            logger.info(f"|    Execution time:  {execution_time} secs           |")
            logger.info(f"|                                                       |")
            logger.info(f"--------------------------------------------------------")
        else:
            logger.error(f"No participants available for collaboration_space_id: {collaboration_space_id}")
    except Exception as e:
        logger.error(e) 

# Commutative encryption: E_k(x) = x^k mod n
def commutative_encrypt(value, key, n):
    return pow(value, key, n)

def tee_commutative_encrypt(data,company, public_keys, n):
    """
    TEE applies additional rounds of commutative encryption using public keys.
    """
    # Apply additional commutative encryptions
    value=data
    for other_company, public_key in public_keys.items():
        if other_company != company:
            value = commutative_encrypt(int(value), public_key, n)
    return value


def fuse_event_processor(evt: dict):
    logger.info(f"---------------------------------------------------------")
    logger.info(f"|                    START PROCESSING                   |")
    logger.info(f"|                                                       |")
    start_time = time.time()
    logger.info(f"|    Start time:  {start_time} secs               |")
    logger.info(f"|                                                       |")
    audit_log(f"Start processing event: {evt.get('type', '')}.",LogLevel.INFO)
    try:
        logger.info(f"| 2. Load keys                                          |")
        logger.info(f"|                                                       |")
        with open(default_settings.data_connector_config_location+'/shared_modulus.json') as f:
            n = json.load(f)["n"] 
        with open(default_settings.data_connector_config_location+'/public_keys.json') as f:
            public_keys = json.load(f)

        logger.info(f"| 2. Get data contracts                                 |")
        logger.info(f"|                                                       |")
        collaboration_space_id=default_settings.collaboration_space_id
        contractManager=ContractManager()
        data_contracts=contractManager.get_contracts_for_collaboration_space(collaboration_space_id)
        if data_contracts != None and len(data_contracts)>0:
            #Create in memory duckdb (encrypted memory on confidential computing)
            con = duckdb.connect(database=":memory:")
            
            #Add connector settings to duckdb con for all data contracts (2 data contracts in this example)
            con = data_contracts[0].connector.add_duck_db_connection(con)
            con = data_contracts[1].connector.add_duck_db_connection(con)
            logger.info(f"| 3. Start fusing process                               |")
            logger.info(f"|                                                       |")
            i=0
            for data_contract in data_contracts:
                query=f"SELECT * FROM {data_contract.connector.get_duckdb_source()}"
                res=con.sql("CREATE OR REPLACE TABLE customers_list_" + str(i) + " AS "+query) 
                audit_log(f"Read data from: {data_contract.data_descriptor_id}.",LogLevel.INFO)
                res=con.sql("ALTER TABLE customers_list_"+str(i)+" ADD COLUMN commutative_id VARCHAR")
                encrypted_data=con.sql("SELECT customer_email from customers_list_"+str(i)).df()
                participant=list(public_keys)[i]
                for index, row in encrypted_data.iterrows():
                    commutative_encrypt=tee_commutative_encrypt(row['customer_email'],participant,public_keys,n)
                    query="UPDATE customers_list_"+str(i)+" SET commutative_id = '"+str(commutative_encrypt)+"' WHERE customers_list_"+str(i)+".customer_email='"+row['customer_email']+"'"
                    res=con.sql(query)
                i=i+1
            
            #Common customers by email
            #Create duckdb query
            query="SELECT COUNT(*) as total FROM customers_list_0,customers_list_1 WHERE (customers_list_0.commutative_id=customers_list_1.commutative_id)"
            df = con.sql(query).df()
            common_customers_by_email=df["total"].to_string(index=False)
            print(common_customers_by_email)

            query="SELECT * FROM customers_list_0"
            df = con.sql(query).df()
            print(df)
            query="SELECT * FROM customers_list_1"
            df = con.sql(query).df()
            print(df)
            
        else:
            logger.error(f"No data contract available for collaboration_space_id: {collaboration_space_id}")
    except Exception as e:
        logger.error(e) 

def check_data_quality_contracts_event_processor(evt: dict):
    #audit logs are generated by the dv_utils sdk
    try:
        contractManager=ContractManager()
        contractManager.check_contracts_for_collaboration_space(default_settings.collaboration_space_id)
    except Exception as e:
        logger.error(e)

def check_common_customers_demo_event_processor(evt: dict):
    logger.info(f"---------------------------------------------------------")
    logger.info(f"|                    START PROCESSING                   |")
    logger.info(f"|                                                       |")
    start_time = time.time()
    logger.info(f"|    Start time:  {start_time} secs               |")
    logger.info(f"|                                                       |")
    audit_log(f"Start processing event: {evt.get('type', '')}.",LogLevel.INFO)
    try:
       
        logger.info(f"| 1. Evaluate common customers                          |")
        logger.info(f"|                                                       |")
        #create db connection to in memory encrypted database
        con = duckdb.connect(database=":memory:")
        #check if tables exist in memory
        query="SELECT table_name FROM information_schema.tables WHERE table_name = 'customers_list_0' AND table_schema = 'main';"
        table0 = con.sql(query).df()
        query="SELECT table_name FROM information_schema.tables WHERE table_name = 'customers_list_01' AND table_schema = 'main';"
        table1 = con.sql(query).df()
        if len(table0)>0 and len(table1)>0:
           #check common customers by email in the database in memory
        #Common customers by email
        #Create duckdb query
        query="SELECT COUNT(*) as total FROM customers_list_0,customers_list_1 WHERE (customers_list_0.commutative_id=customers_list_1.commutative_id)"
        df = con.sql(query).df()
        common_customers_by_email=df["total"].to_string(index=False)

        #Write outputs for data user
        #For now the output is written in an encrypted drive only accessible for data user
        #TODO Connector for data users (write) have to be created
        logger.info(f"| 3. Send output                                        |")
        output_json={}
        output_json["common_customers"]={"by_email":common_customers_by_email}
        with open(default_settings.data_user_output_location+'/report.json', 'w', newline='') as file:
                file.write(json.dumps(output_json, indent=4))
        logger.info(f"|                                                       |")
        execution_time=(time.time() - start_time)
        logger.info(f"|    Execution time:  {execution_time} secs           |")
        logger.info(f"|                                                       |")
        logger.info(f"--------------------------------------------------------")

        else:
            logger.error(f"No table exist in memory, please initialise the fusion")
    except Exception as e:
        logger.error(e) 
