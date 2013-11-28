import urllib3
import json
from functools import wraps

CONN = urllib3.connection_from_url('http://localhost:7474')

# Should either Session and/or Transaction implement the context-manager
# interface (for use with 'with')?  I'm not sure.

# class Session(object):
#     pass


def _assert_operable(func):
    '''Wrapper for Transaction methods to prevent action on already committed 
    or rolled-back transactions.
    '''
    @wraps(func)
    def wrapper(self, *args, **kwds):
        if self._committed or self._rolled_back:
            raise TransactionClosedError(
                'Attempted to call %s() on an already %s transaction' 
                % (
                    func.__name__, 
                    'committed' if self._committed else 'rolled-back'
                    )
                )
        return func(self, *args, **kwds)
    return wrapper


class Transaction(object):
    '''The Transaction class is responsible for handling all
    interactions with the database.

    Statements that are to be executed against the database are added
    either via add_statement() or directly assigning to the
    instance.statements attribute.

    Results can be obtained by calling either
    execute(): To run the current statements but leave the
               transaction open.
    commit():  To run the current statements and commit the 
               entire transaction.

    Once a transaction has been committed or rolled-back it can no
    longer be operated on.  Attempts to do so will raise a
    TransactionClosedError.

    TODO: Deal with various responses.
    '''

    BASE_PATH = '/db/data/transaction'
    TRANSACTION_TIMED_OUT_CODE = 'Neo.ClientError.Transaction.UnknownId'

    def __init__(self, connection):
        self._connection = connection
        self._location = self.BASE_PATH
        self._started = False
        self._committed = False
        self._rolled_back = False
        self._executed_statements = []
        self.statements = []
        # Python 2 or 3 style supers?
        # super().__init__(*args, **kwargs)
        # super(Transaction).__init__(self, *args, **kwargs)

    @_assert_operable
    def add_statement(self, cypher, params=None):
        '''Append the given cypher statement to this Transaction's
        list of statements.

        These statements will be sent to the db on the next execute()
        or commit() call.
        '''
        statement = {
            'statement':cypher
            }
        if params:
            statement['parameters'] = params
        self.statements.append(statement)

    @_assert_operable
    def execute(self):
        '''Send the current list of cypher statements to the db and
        return the parsed response data.

        If there are no pending statements, returns None.
        '''
        if not self.statements:
            return None

        response = self._make_request(
            'POST', 
            self._location, 
            self._get_prepared_statements()
            )

        self._archive_current_statements()
        return response


    @_assert_operable
    def commit(self):
        '''Commit this transaction.

        This will send the current list of cypher statements to the db
        and return the parsed response data in addition to committing
        the entire transaction.
        '''
        path = self._location + '/commit'

        response = self._make_request(
            'POST', 
            path, 
            self._get_prepared_statements()
            )

        self._archive_current_statements()
        self._committed = True
        return response


    @_assert_operable
    def rollback(self):
        '''Rollback this transaction.'''
        # Should this raise an error or just successfully do nothing?
        if not self._started:
            raise TransactionClosedError(
                'Cannot rollback non-started transaction.'
                )

        response = self._make_request(
            'DELETE', 
            self._location
            )

        self._rolled_back = True
        return response


    def _archive_current_statements(self):
        if not self.statements:
            return
        self._executed_statements.extend(self.statements)
        self.statements = []

    def _get_prepared_statements(self):
        return {'statements':self.statements}

    def _make_request(self, method, path, data=None):
        body = None
        if data:
            body = json.dumps(data)
        
        return self._process_response(
            self._connection.urlopen(
                method, 
                path, 
                body=body
                )
            )

    def _process_response(self, response):
        # On posting a new transaction, neo4j will respond with a
        # location header telling us where to keep accessing this the
        # transaction.
        if 'location' in response.headers:
            self._location = response.headers['location']
            self._started = True

        # I'm not sure I want to do this here... should be doing
        # streaming rather than loading the whole reponse into an
        # object!  Although, unless the user application is streaming
        # out too, the data will probably all end up in memory at some
        # stage...
        data_str = response.data.decode(response.headers['content-encoding'])
        data = json.loads(data_str)        

        errors = data.get('errors')
        if errors:
            # If there were errors, then neo4j have rolled-back the 
            # transaction.
            self._rolled_back = True

            # If the transaction didn't run at all due to a closed
            # (timed-out) transaction, raise exception to alert the
            # user application.
            if errors[0].get('code') == self.TRANSACTION_TIMED_OUT_CODE:
                raise TransactionClosedError(errors[0].get('message'))

        return data


class CypherAlchemyError(Exception):
    '''Base CypherAlchemy Exception.'''
    pass

class TransactionClosedError(CypherAlchemyError):
    '''An attempt was made to act on an already committed or
    rolled-back transaction.
    '''
    pass

