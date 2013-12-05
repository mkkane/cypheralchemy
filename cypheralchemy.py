import urllib3
import json
from functools import wraps

DEFAULT_NEO4J_URL = 'http://localhost:7474'

class Engine(object):
    '''The Engine class is responsible for generating Transactions
    that the Session can use to do it's work.
    '''
    def __init__(self, url=DEFAULT_NEO4J_URL):
        self._connection = self._generate_connection(url)

    def create_transaction(self):
        '''Create a new Transaction with our current urllib3 connection.'''
        return Transaction(self._connection)

    def _generate_connection(self, url):
        '''Generate a urllib3 connection that will be passed to the
        Transaction class when creating new Transactions.
        '''
        return urllib3.connection_from_url(
            'http://localhost:7474', 
            headers={
                'Accept': 'application/json',
                'X-Stream': 'true'
                }
            )


class Session(object):
    def __init__(self, engine):
        self._engine = engine

    @property
    def transaction(self):
        '''Return the current transaction.

        This will generate a new transaction when needed.
        '''
        if (
            not getattr(self, '_transaction', False) 
            or not self._transaction.is_operable()
            ):
            self._transaction = self._engine.create_transaction()
        return self._transaction

    def query(self, model_class=None):
        return Query(self, model_class)

    def add(self, model):
        pass

    def delete(self, model):
        pass

    def flush(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass



class Query(object):
    def __init__(self, session, model_class=None):
        self._session = session
        self._model_class = model_class

    def cypher(self, cypher_string, params={}, raw=False):
        # Just testing.  Obviously shouldn't be grabbing and commiting
        # the sessions transaction from here!
        t = self._session.transaction
        t.add_statement(cypher_string, params)
        response = t.commit()
        if raw:
            return response

        errors = response.get('errors')
        if errors:
            # If neo4j reported a syntax error, raise an appropriate exception 
            if errors[0].get('code') == CYPHER_SYNTAX_ERROR_CODE:
                raise CypherSyntaxError(errors[0].get('message'))

            # If we don't know about this neo4j error, raise a general esception
            raise RequestError('{code}: {message}'.format(**errors[0]))

        return [r['row'] for r in response['results'][0]['data']]


def _assert_operable(func):
    '''Wrapper for Transaction methods to prevent action on already committed 
    or rolled-back transactions.
    '''
    @wraps(func)
    def wrapper(self, *args, **kwds):
        if not self.is_operable():
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

    def __init__(self, connection):
        '''Initialise a transaction with a urllib3 connection object.'''
        self._connection = connection
        self._location = self.BASE_PATH
        self._started = False
        self._committed = False
        self._rolled_back = False
        self._executed_statements = []
        self.statements = []

    def is_operable(self):
        '''Can this transaction be operated on.

        Note: this is a best guess, the Transaction may have timed-out
        and been rolled back by the server without us being aware till
        we try to operate on it.  (TODO, store the 'expires' data sent
        back by the server so we can more accurately know if this
        transaction is still open.)
        '''
        return not (self._committed or self._rolled_back)

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
        '''Return all current statements in a format usable as the
        body of a json request to the neo4j transaction endpoint.
        '''
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
            if errors[0].get('code') == TRANSACTION_TIMED_OUT_CODE:
                raise TransactionClosedError(errors[0].get('message'))

        return data


class CypherAlchemyError(Exception):
    '''Base CypherAlchemy Exception.'''
    pass

class RequestError(CypherAlchemyError):
    '''There was some problem with an attempted request to neo4j.'''
    pass

class TransactionClosedError(RequestError):
    '''An attempt was made to act on an already committed or
    rolled-back transaction.
    '''
    pass

class CypherSyntaxError(RequestError):
    '''Neo4j reported a syntax error with the request.'''
    pass

TRANSACTION_TIMED_OUT_CODE = 'Neo.ClientError.Transaction.UnknownId'
CYPHER_SYNTAX_ERROR_CODE = 'Neo.ClientError.Statement.InvalidSyntax'
