import logging
import asyncio

from handlers.jobs.db_connector import PostgresConnector

logger = logging.getLogger('osr')

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class DbTasksSchduler:
    def __init__(self, db_conn_pool):
        self.db_conn = PostgresConnector(db_conn_pool)

        self._enable = False

        logger.info('Tasks scheduler started')

    def check_expired_tokens(self):
        token_ids = self.db_conn.execute_query("""SELECT id FROM session WHERE expired_date < CURRENT_TIMESTAMP;""",
                                               fetchall=True)
        token_ids = [t[0] for t in token_ids] if token_ids else []
        return token_ids

    def delete_tokens(self, token_ids):
        token_ids = tuple(token_ids)
        # self.db_conn.execute_query("""DELETE FROM session WHERE id IN %s;""" % (token_ids,),
        #                            with_commit=True, with_fetch=False)
        if len(token_ids) > 1:
            self.db_conn.execute_query("""DELETE FROM session WHERE id IN %s;""" % (token_ids,),
                                       with_commit=True, with_fetch=False)
        else:
            self.db_conn.execute_query("""DELETE FROM session WHERE id is %s;""" % (token_ids[0],),
                                       with_commit=True, with_fetch=False)

    async def scheduler(self):
        """
        Runs endless loop with jobs check and execute code in it.

        :return:        None
        """
        while self._enable:
            tokens = self.check_expired_tokens()
            if tokens:
                self.delete_tokens(tokens)
                logger.info(f'Deleted tokens: {tokens}')
            else:
                await asyncio.sleep(60)
        logger.info('Scheduler was stopped')

    def start(self):
        """
        Starts manager work.

        :return:        None
        """
        self._enable = True
        asyncio.ensure_future(self.scheduler())

    def stop(self):
        """
        Stops manager work.

        :return:        None
        """
        self._enable = False
