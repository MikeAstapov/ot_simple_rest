from abc import abstractmethod
from typing import Dict

import notifications.codes
from notifications.format import Notification
from handlers.jobs.db_connector import PostgresConnector

class AbstractNotificationHandler:

    @abstractmethod
    def check(self, *args, **kwargs) -> Dict[str, str]:
        pass


class TooManyJobsNotification(AbstractNotificationHandler):

    DEFAULT_THRESHOLD = 8
    NOTIFICATION_CODE = notifications.codes.TOO_MANY_JOBS

    def __init__(self, db_pool, conf:dict) -> None:
        super().__init__()
        self.db = PostgresConnector(db_pool)
        self.conf = conf

    def check(self, *args, **kwargs) -> Dict[str, str]:
        if self.db and self.conf:
            running_jobs_counter = self.db.get_running_jobs_num()
            if running_jobs_counter >= int(self.conf.get('jobs_queue_threshold', self.DEFAULT_THRESHOLD)):
                return Notification(code=self.NOTIFICATION_CODE, value=running_jobs_counter).as_dict()

        return {}

class LimitedDataNotification(AbstractNotificationHandler):

    DEFAULT_THRESHOLD = 10000
    NOTIFICATION_CODE = notifications.codes.LIMITED_DATA
    
    def __init__(self, threshold = DEFAULT_THRESHOLD) -> None:
        super().__init__()
        self.threshold = threshold

    def check(self, *args, **kwargs) -> Dict[str, str]:
        strnum = kwargs.get('lines_total')
        if strnum==self.threshold:
            return Notification(code=self.NOTIFICATION_CODE, value=strnum).as_dict()

        return {}