import inspect
import logging
import logging.config
import os

from opensearch_logger import OpenSearchHandler
from pythonjsonlogger import jsonlogger


class SimpleLogger:
    """A simple logger wrapper to log messages in JSON format.

    :param obj: the class object requesting the logger or a string
    :param log_level: the log level.
    """

    def __init__(self, obj: object, os_logs_index_name: str = None, level=logging.INFO):
        """A simple logger wrapper to log messages in JSON format.

        :param obj: either a class instance or a string to name the logger
        :param general_info: if a string is provided, a new field will be added to the log record
        containing the string
        :param level: the minimun log level to log, defaults to logging.INFO
        """
        name = "SimpleLogger"
        name = obj if isinstance(obj, str) else obj.__class__.__name__
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self.handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(filename)s %(module)s %(funcName)s %(lineno)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"},
        )
        self.handler.setFormatter(formatter)
        self._logger.addHandler(self.handler)
        self._logger.addFilter(SimpleLogger.FunctionNameFilter())
        if os_logs_index_name:
            self._add_opensearch_handler(os_logs_index_name)

    def _add_opensearch_handler(self, os_logs_index_name: str):
        """Add the opensearch logging handler.

        Be aware that the host must start with 'https://' when using the streaming interface.
        see: https://pypi.org/project/opensearch-logger/, Using Data Streams.
        """
        try:
            host = os.environ["OPENSEARCH_HOST"]
            if not host.startswith("https://"):
                host = f"https://{host}"
            opensearch_user = os.environ["OPENSEARCH_USER"]
            opensearch_pw = os.environ["OPENSEARCH_PW"]
            self.os_handler = OpenSearchHandler(
                index_name=os_logs_index_name,
                is_data_stream=True,
                hosts=[host],
                http_auth=(opensearch_user, opensearch_pw),
                http_compress=True,
                use_ssl=True,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
            )
            if self.os_handler.test_opensearch_connection():
                self._logger.addHandler(self.os_handler)
            else:
                self.warning("Unable to connect to opensearch, the opensearch logger will be disabled.")
        except KeyError as err:
            os_env_vars = "OPENSEARCH_HOST, OPENSEARCH_USER, OPENSEARCH_PW"
            self.warning("The following environment variables must be set: %s \n %s", os_env_vars, err)
        except Exception as err:
            self.warning("Unable to use opensearch logging because %s.", err)

    def warning(self, msg, *args, **kwargs) -> None:
        """Log a warning message.

        :param message: the message to log
        """
        self._logger.warning(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs) -> None:
        """Log an info message.

        :param message: the message to log
        """
        self._logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs) -> None:
        """Log an error message.

        :param message: the message to log
        """
        self._logger.error(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs) -> None:
        """Log a debug message.

        :param message: the message to log
        """
        self._logger.debug(msg, *args, **kwargs)

    def add_gen_info(self, info: str) -> None:
        """Checks filters for an existing GenInfoFilter.

        If the filter exists, removes it, and adds a new GenInfoFilter with the provided info.
        :param info: the extra information to add.
        """
        for filter in self._logger.filters:
            if isinstance(filter, SimpleLogger.GenInfoFilter):
                self._logger.removeFilter(filter)
        gen_info_filter = SimpleLogger.GenInfoFilter(info)
        self._logger.addFilter(gen_info_filter)

    @property
    def name(self):
        return self._logger.name

    class GenInfoFilter(logging.Filter):
        """A filter to add a field to the log record that contains extra information."""

        def __init__(self, info: str) -> None:
            super().__init__()
            self.gen_info = info

        def filter(self, record: logging.LogRecord) -> bool:
            """Add the service name to the log record..

            :param record: the log record to add the service name to
            :return: always returns True.
            """
            record.gen_info = self.gen_info
            return True

    class FunctionNameFilter(logging.Filter):
        """Adds caller function name and line number to the log.

        Those are added to the log record instead of the this classes' logging method(warning, info, error, and debug)
        names and line numbers.
        """

        def filter(self, record):
            """Sets up the filter.

            Grabs the current stack and retrieves the function name and line number from the
            outer frame. This will be the caller of the logging method.
            :param record: the log record to update the function name and line number in
            :return: always returns True.
            """
            curr_frame = inspect.currentframe()
            outer_frame = inspect.getouterframes(curr_frame, 2)
            record.filename = outer_frame[6][1]
            record.lineno = outer_frame[6][2]
            record.funcName = outer_frame[6][3]
            module_class = outer_frame[6][0].f_locals.get("self")
            record.module = module_class.__class__.__name__
            return True
