from tools.pg_connector import PGConnector


class NotificationType:

    TOO_MANY_JOBS = 1


class NotificationChecker:

    def __init__(self, notification_conf, db_conn_pool):
        self.notification_conf = notification_conf
        self.db = PGConnector(db_conn_pool)
        self.get_current_running_jobs_number = "SELECT COUNT(*) FROM otlqueries WHERE status = 'running';"  # SQL query
        self.default_too_many_jobs = 8

    def check_too_many_jobs(self, response):
        running_jobs_counter = self.db.execute_query(self.get_current_running_jobs_number)
        if running_jobs_counter[0] >= int(self.notification_conf.get('jobs_queue_threshold',
                                                                     self.default_too_many_jobs)):
            response['notification'] = NotificationType.TOO_MANY_JOBS  # message code