from typing import List

from notifications.handlers import AbstractNotificationHandler, TooManyJobsNotification


class NotificationChecker:

    def __init__(self, handlers : List[AbstractNotificationHandler]):
        self.handlers = handlers

    def check_notifications(self, **kwargs):
        """
        gathers and returns list of all notifications where every element is a structure

        """
        notifications = []

        for handler in self.handlers:
            notification = handler.check(**kwargs)
            if notification:
                notifications.append(notification)
        return notifications
