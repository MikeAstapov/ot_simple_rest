from .db_connector import PostgresConnector


class NotificationType:
    TOO_MANY_JOBS = 1


class NotificationChecker:

    def __init__(self, notification_conf, db_conn_pool):
        self.notification_conf = notification_conf
        self.db = PostgresConnector(db_conn_pool)
        self.default_too_many_jobs = 8

    def check_notifications(self):
        """
        gathers and returns list of all notifications where every element is a structure
        :code: message code
        :value: additional info if necessary or None
        """
        running_jobs_counter = self.db.get_running_jobs_num()
        notifications = []
        if running_jobs_counter >= int(self.notification_conf.get('jobs_queue_threshold',
                                                                  self.default_too_many_jobs)):
            notifications.append({'code': NotificationType.TOO_MANY_JOBS, 'value': running_jobs_counter})
        return notifications
