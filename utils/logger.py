import logging
import os
import sys
from pathlib import Path

import structlog
from loguru import logger


class Logger:
    def __init__(self, config):
        self.config = config

    def get_logger(self, log_level=None):
        # default logger
        # loguru seems 'better' 'simpler' than logstruct
        return self.get_loguru_logger(log_level)

    def get_format(self):
        # default
        # add(sink, *, level='DEBUG', format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>', filter=None, colorize=None, serialize=False, backtrace=True, diagnose=True, enqueue=False, context=None, catch=True, **kwargs)

        log_format = ('<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | '
                      '<level>{level: <8}</level> | ')

        if self.config.log_extra_info == "function_line":
            log_format += '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - '

        log_format += '<level>{message}</level> <white><dim>{extra}</dim></white>'

        return log_format

    def get_loguru_logger(self, log_level=None):
        # https://loguru.readthedocs.io/en/stable/index.html

        if log_level is not None:
            min_log_level = log_level
        else:
            min_log_level = self.config.log_level.upper()
        log_dir = "logs"
        log_to_file = True

        # can check tty, but not pycharm
        # in_ide = True
        # if in_ide or sys.stderr.isatty():

        # True: json, but verbose, prob want to customize for external sink
        log_serialize = False

        # remove defaults
        logger.remove()

        log_format = self.get_format()

        if log_to_file:
            os.makedirs(log_dir, exist_ok=True)
            logger.add(f"{log_dir}/app.log", level=min_log_level, format=log_format,
                       rotation="1 day", retention=30,
                       filter=None, colorize=None, serialize=log_serialize,
                       backtrace=True, diagnose=True, enqueue=True, context=None, catch=True
                       )

        logger.add(sys.stderr, level=min_log_level, format=log_format,
                   filter=None, colorize=None, serialize=log_serialize,
                   backtrace=True, diagnose=True, enqueue=True, context=None, catch=True
                   )

        return logger

    def get_structlog_logger(self):
        # TODO not updated with latest log changes, may be removed later
        # https://www.structlog.org/en/stable/index.html

        # consider installing 'colorama' for colors in windows, 'rich' for nice exceptions

        # TODO make configurable
        min_log_level = logging.NOTSET
        log_dir = "logs"
        # can check tty, but not pycharm
        in_ide = True

        os.makedirs(log_dir, exist_ok=True)
        logger_factory = structlog.WriteLoggerFactory(
            file=Path(f"{log_dir}/app").with_suffix(".log").open("wt")
        )

        processors = [
            # Processors that have nothing to do with output,
            # e.g., add timestamps or log level names.
            structlog.contextvars.merge_contextvars,
            # Adds logger=module_name (e.g __main__)
            # structlog.stdlib.add_logger_name,
            # Adds level=info, debug, etc.
            structlog.stdlib.add_log_level,
            # Performs the % string interpolation as expected
            structlog.stdlib.PositionalArgumentsFormatter(),
            # Include the stack when stack_info=True
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            # Include the exception when exc_info=True
            # e.g log.exception() or log.warning(exc_info=True)'s behavior
            # prevents Rich exceptions
            # structlog.processors.format_exc_info,
            # Transform event dict into `logging.Logger` method arguments.
            # "event" becomes "msg" and the rest is passed as a dict in
            # "extra". IMPORTANT: This means that the standard library MUST
            # render "extra" for the context to appear in log entries!
            # structlog.stdlib.render_to_log_kwargs,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
            # structlog.dev.ConsoleRenderer(),
            # structlog.processors.dict_tracebacks,
            # structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ]

        if in_ide or sys.stderr.isatty():
            # Pretty printing when we run in a terminal session.
            # Automatically prints pretty tracebacks when "rich" is installed
            processors = processors + [
                structlog.dev.ConsoleRenderer(),
            ]
        else:
            # Print JSON when we run, e.g., in a Docker container.
            # Also print structured tracebacks.
            processors = processors + [
                # structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ]

        structlog.configure(
            processors,
            wrapper_class=structlog.make_filtering_bound_logger(min_log_level),
            # logger_factory=logger_factory,
            # Our "event_dict" is explicitly a dict
            # There's also structlog.threadlocal.wrap_dict(dict) in some examples
            # which keeps global context as well as thread locals
            # context_class=dict,
            # logger_factory=structlog.PrintLoggerFactory(),
            # logger_factory=structlog.WriteLoggerFactory(),
            cache_logger_on_first_use=True
        )
        logger = structlog.get_logger()
        log = logger.bind()
        return log


def main():
    print("not directly callable")


if __name__ == "__main__":
    main()
