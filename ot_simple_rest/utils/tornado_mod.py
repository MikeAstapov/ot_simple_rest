from tornado.log import access_log
from tornado.web import Application, RequestHandler

from utils.primitives import RestUser


def concat(*args):
    return ' '.join(args)


def format_args(args):
    return ', '.join([f'{key}: {", ".join([v.decode() for v in value])}' for key, value in args])


class Tornado(Application):
    """
    Class extending Tornado web app with additional request logging
    """

    def log_request(self, handler: RequestHandler) -> None:
        """Writes a completed HTTP request to the logs.

        By default writes to the python root logger.  To change
        this behavior either subclass Application and override this method,
        or pass a function in the application settings dictionary as
        ``log_function``.
        """
        if "log_function" in self.settings:
            self.settings["log_function"](handler)
            return
        if handler.get_status() < 400:
            log_method = access_log.info
        elif handler.get_status() < 500:
            log_method = access_log.warning
        else:
            log_method = access_log.error
        request_time = 1000.0 * handler.request.request_time()
        log_method(
            "%d %s %.2fms",
            handler.get_status(),
            handler._request_summary(),
            request_time,
        )

        if self.settings.get('log_user_activity'):
            if hasattr(handler.request, 'user'):
                user: RestUser = handler.request.user
                log_method(f'Request from authenticated {user}')
            log_method(concat('Headers ', str(handler.request.headers).replace('\n', ' ')))
            log_method(
                concat('Body ', handler.request.body.decode().replace('\n', ' '))) if handler.request.body else None
            log_method(
                concat('Args ', format_args(handler.request.arguments.items()))) if handler.request.arguments else None


