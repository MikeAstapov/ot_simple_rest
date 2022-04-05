from abc import abstractmethod
from typing import Dict

import notifications.codes
from notifications.format import Notification


class AbstractNotificationHandler:

    @abstractmethod
    def check(self, *args, **kwargs) -> Dict[str, str]:
        pass


class TooManyJobsNotification(AbstractNotificationHandler):

    DEFAULT_THRESHOLD = 8
    NOTIFICATION_CODE = notifications.codes.TOO_MANY_JOBS

    def check(self, *args, **kwargs) -> Dict[str, str]:
        db = kwargs.get('db')
        conf: dict = kwargs.get('notification_conf')
        if db and conf:
            running_jobs_counter = db.get_running_jobs_num()
            if running_jobs_counter >= int(conf.get('jobs_queue_threshold', self.DEFAULT_THRESHOLD)):
                return Notification(code=self.NOTIFICATION_CODE, value=running_jobs_counter).as_dict()

        return {}