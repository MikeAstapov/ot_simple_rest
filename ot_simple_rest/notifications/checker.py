from typing import List

from notifications.handlers import AbstractNotificationHandler, TooManyJobsNotification


NOTIFICATION_HANDLERS: List[AbstractNotificationHandler] = [
    TooManyJobsNotification(),
]


class NotificationChecker:

    def __init__(self, handlers=NOTIFICATION_HANDLERS):
        self.handlers = handlers

    def check_notifications(self, **kwargs):
        """
        gathers and returns list of all notifications where every element is a structure

        """
        notifications = []

        for handler in NOTIFICATION_HANDLERS:
            notification = handler.check(**kwargs)
            if notification:
                notifications.append(notification)
        return notifications
