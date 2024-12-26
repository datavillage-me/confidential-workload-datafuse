"""
Unit test.
"""

# Read env variables from a local .env file, to fake the variables normally provided by the confidential environment
import dotenv
dotenv.load_dotenv('.env')
import unittest
import logging
import process

class Test(unittest.TestCase):
    # def test_initialize(self):
    #     """
    #     Try the process to initialize
    #     """
    #     test_event = {
    #         'type': 'INITIALIZE'
    #     }

    #     process.event_processor(test_event)
    
    def test_fuse(self):
        """
        Try the process to initialize
        """
        test_event = {
            'type': 'FUSE'
        }

        process.event_processor(test_event)

    # def test_data_quality_check(self):
    #     """
    #     Try the process to check data quality
    #     """
    #     test_event = {
    #         'type': 'CHECK_DATA_QUALITY'
    #     }

    #     process.event_processor(test_event)
    
    # def test_common_customers(self):
    #     """
    #     Try the process  without going through the redis queue
    #     """
    #     test_event = {
    #         'type': 'CHECK_COMMON_CUSTOMERS'
    #     }
        
    #     process.event_processor(test_event)
    
    # def test_valid_customer(self):
    #     """
    #     Try the process  without going through the redis queue
    #     """
    #     test_event = {
    #         'type': 'CHECK_VALID_CUSTOMER',
    #         'email': "691673898843968854734317270616041944235022397737718120661065632728283589020365827270235696533440324076344122767863278700613793778741948102402293910670205897459611108131328652902112313227670804935908600313385988908052514552442493141652755765217959472051094521548542100674000698760113597459012551176459606917562657913254796581597200396294258417918317238102801817154486660516360102644400238861735805514272843715775934377833428025777267178421590858279820459982740984113066169948931894020781367027445912739707560790447932173557774370519421516940398461480978067798569652788292888137014346891157486360529321830236979398106",
    #     }
        
    #     process.event_processor(test_event)