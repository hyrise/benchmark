import os
import random

from datetime import datetime

import constants

class HyriseDriver(object):

    queries = {}

    def __init__(self, scalefactor, hyrise_builddir, hyrise_tabledir, querydir, uniform_sids):
        self.hyrise_builddir = hyrise_builddir
        self.table_location = hyrise_tabledir
        self.tables = constants.ALL_TABLES
        self.query_directory = querydir
        self.uniform_sids = uniform_sids
        self.conn = None

        self.population = constants.BASE_POPULATION * scalefactor
        if not HyriseDriver.queries:
            HyriseDriver.queries = self.loadQueryfiles(self.query_directory, constants.QUERY_FILES)

    #def loadConfig(self, config):
    #    for k,v in config:
    #        if hasattr(self, k):
    #            setattr(self, k, v)

    def set_connection(self, connection):
        self.conn = connection

    def get_sid(self):
        if self.uniform_sids:
            return random.randrange(self.population)+1
        else:
            A = 2097151
            if self.population <= 1000000:
                A = 65535
            elif self.population <= 10000000:
                A = 1048575
            return self.NURand(A, 0, self.population)

    def NURand(self, A, x, y):
        return ((random.randrange(A+1) | random.randint(x, y+1))) % (y - x + 1) + x

    def loadQueryfiles(self, querydir, mapping):
        queries = {}
        for query_type, query_dict in mapping.iteritems():
            for query_name, filename in query_dict.iteritems():
                with open(os.path.abspath(os.path.join(querydir, filename)), 'r') as jsonfile:
                    queries.setdefault(query_type,{})[query_name] = jsonfile.read()
        return queries

    def doOne(self):
        """Selects and executes a transaction at random.
        Read Transactions (80%):
        GET_SUBSCRIBER_DATA 35%
        GET_NEW_DESTINATION 10%
        GET_ACCESS_DATA 35%
        Write Transactions (20%):
        UPDATE_SUBSCRIBER_DATA 2%
        UPDATE_LOCATION 14%
        INSERT_CALL_FORWARDING 2%
        DELETE_CALL_FORWARDING 2%
        """

        x = random.randrange(100)
        params = None
        txn = None
        if x < 35: ## 35%
            txn, params = (constants.TransactionTypes.GET_SUBSCRIBER_DATA, self.generateGetSubscriberDataParams())
        elif x < 35 + 10: ## 10%
            txn, params = (constants.TransactionTypes.GET_NEW_DESTINATION, self.generateGetNewDestinationParams())
        elif x < 35 + 10 + 35: ## 35%
            txn, params = (constants.TransactionTypes.GET_ACCESS_DATA, self.generateGetAccessDataParams())
        elif x <  35 + 10 + 35 + 2: ## 2%
            txn, params = (constants.TransactionTypes.UPDATE_SUBSCRIBER_DATA, self.generateUpdateSubscriberDataParams())
        elif x <  35 + 10 + 35 + 2 + 14: ## 14%
            txn, params = (constants.TransactionTypes.UPDATE_LOCATION, self.generateUpdateLocationParams())
        elif x <  35 + 10 + 35 + 2 + 14 + 2: ## 2%
            txn, params = (constants.TransactionTypes.INSERT_CALL_FORWARDING, self.generateInsertCallForwardingParams())
        else: ## 2%
            assert x >= 100 - 2
            txn, params = (constants.TransactionTypes.DELETE_CALL_FORWARDING, self.generateDeleteCallForwardingParams())

        return (txn, params)

    def executeTransaction(self, txn, params, use_stored_procedure):
        """Execute a transaction based on the given name"""

        if constants.TransactionTypes.GET_SUBSCRIBER_DATA == txn:
            result = self.doGetSubscriberData(params, use_stored_procedure)
        elif constants.TransactionTypes.GET_NEW_DESTINATION == txn:
            result = self.doGetNewDestination(params, use_stored_procedure)
        elif constants.TransactionTypes.GET_ACCESS_DATA == txn:
            result = self.doGetAccessData(params, use_stored_procedure)
        elif constants.TransactionTypes.UPDATE_SUBSCRIBER_DATA == txn:
            result = self.doUpdateSubscriberData(params, use_stored_procedure)
        elif constants.TransactionTypes.UPDATE_LOCATION == txn:
            result = self.doUpdateLocation(params, use_stored_procedure)
        elif constants.TransactionTypes.INSERT_CALL_FORWARDING == txn:
            result = self.doInsertCallForwarding(params, use_stored_procedure)
        elif constants.TransactionTypes.DELETE_CALL_FORWARDING == txn:
            result = self.doDeleteCallForwarding(params, use_stored_procedure)
        else:
            assert False, "Unexpected TransactionType: " + txn
        return result

    def doGetSubscriberData(self, params, use_stored_procedure=True):
        q = self.queries["GET_SUBSCRIBER_DATA"]

        result = []
        self.conn.query(q["GetSubscriberData"], params)
        self.conn.commit()
        return result

    def doGetNewDestination(self, params, use_stored_procedure=True):
        q = self.queries["GET_NEW_DESTINATION"]

        result = []
        self.conn.query(q["GetNewDestination"], params)
        self.conn.commit()
        return result


    def doGetAccessData(self, params, use_stored_procedure=True):
        q = self.queries["GET_ACCESS_DATA"]

        result = []
        self.conn.query(q["GetAccessData"], params)
        self.conn.commit()
        return result

    def doUpdateSubscriberData(self, params, use_stored_procedure=True):
        q = self.queries["UPDATE_SUBSCRIBER_DATA"]

        result = []
        self.conn.query(q["UpdateSubscriberData"], params)
        self.conn.commit()
        return result

    def doUpdateLocation(self, params, use_stored_procedure=True):
        q = self.queries["UPDATE_LOCATION"]

        result = []
        self.conn.query(q["UpdateLocation"] , params)
        self.conn.commit()
        return result

    def doInsertCallForwarding(self, params, use_stored_procedure=True):
        q = self.queries["INSERT_CALL_FORWARDING"]

        result = []
        self.conn.query(q["GetSubscriberId"], params)
        temp_res = self.conn.fetchone_as_dict()

        if not temp_res:
            return []

        s_id = temp_res['S_ID']
        self.conn.query(q["GetFacilityType"], {'s_id':s_id})

        # Even though the previous query project the SF_TYPE as result,
        # the benchmark uses a RANDOM sf type for the insert query.
        # Therefore, the following sf_type assignment is correct.
        # Compare to the TATP Benchmark Description by IBM:
        # http://tatpbenchmark.sourceforge.net/TATP_Description.pdf
        #sf_type = self.conn.fetchone_as_dict['sf_type']
        sf_type = params['sf_type']

        self.conn.query(q["InsertCallForwarding"],
            {'s_id':s_id,
            'sf_type':sf_type,
            'start_time':params['start_time'],
            'end_time':params['end_time'],
            'numberx':params['numberx']})

        self.conn.commit()
        return result

    def doDeleteCallForwarding(self, params, use_stored_procedure=True):
        q = self.queries["DELETE_CALL_FORWARDING"]

        result = []
        self.conn.query(q["GetSubscriberId"], params)
        temp_res = self.conn.fetchone_as_dict()

        if not temp_res:
            return []

        s_id = temp_res['S_ID']

        self.conn.query(q["DeleteCallForwarding"],
            {'s_id':s_id,
            'sf_type':sf_type,
            'start_time':params['start_time']})


        self.conn.commit()
        return result

    def generateGetSubscriberDataParams(self):
        return {'s_id': self.get_sid()}

    def generateGetNewDestinationParams(self):
        return {'s_id': self.get_sid(),
                'sf_type': random.randrange(4)+1,
                'start_time': random.choice([0,8,16]),
                'end_time': random.randrange(24)+1
                }

    def generateGetAccessDataParams(self):
        return {'s_id': self.get_sid(),
                'ai_type': random.randrange(4)+1
                }

    def generateUpdateSubscriberDataParams(self):
        return {'bit_1': random.choice([0,1]),
                's_id': self.get_sid(),
                'data_a': random.randrange(256),
                'sf_type': random.randrange(4)+1
                }

    def generateUpdateLocationParams(self):
        return {'sub_nbr': "{0:015d}".format(self.get_sid()+1),
                'vlr_location': random.randrange(4294967295)+1
                }
    def generateInsertCallForwardingParams(self):
        return {'sub_nbr': "{0:015d}".format(self.get_sid()+1),
                'sf_type': random.randrange(4)+1,
                'start_time': random.choice([0,8,16]),
                'end_time': random.randrange(24)+1,
                'numberx': "{0:015d}".format(self.get_sid()+1)
                }

    def generateDeleteCallForwardingParams(self):
        return {'sub_nbr': "{0:015d}".format(self.get_sid()+1),
                'sf_type': random.randrange(4)+1,
                'start_time': random.choice([0,8,16])
                }
