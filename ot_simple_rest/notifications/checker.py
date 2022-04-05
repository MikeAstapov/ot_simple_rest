from typing import List

from handlers.jobs.db_connector import PostgresConnector

from notifications.handlers import AbstractNotificationHandler, TooManyJobsNotification


NOTIFICATION_HANDLERS: List[AbstractNotificationHandler] = [
    TooManyJobsNotification(),
]


class NotificationChecker:

    def __init__(self, notification_conf, db_conn_pool):
        self.notification_conf = notification_conf
        self.db = PostgresConnector(db_conn_pool)

    def check_notifications(self):
        """
        gathers and returns list of all notifications where every element is a structure

        """
        notifications = []

        for handler in NOTIFICATION_HANDLERS:
            notification = handler.check(db=self.db, conf=self.notification_conf)
            if notification:
                notifications.append(notification)
        return notifications
