#  Copyright 2025 ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import sys
from typing import Optional


DEFAULT_LOG_FORMAT = "[%(asctime)s.%(msecs)03d] [%(levelname)s] %(name)s - %(lineno)d - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


class ExtendedLogger(logging.Logger):
    """
    Custom logger class that supports TRACE level logging.
    """
    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(TRACE_LEVEL):
            self._log(TRACE_LEVEL, message, args, **kwargs)

logging.setLoggerClass(ExtendedLogger)


def configure_logging(level: int = logging.INFO,
                      log_format: str = DEFAULT_LOG_FORMAT,
                      date_format: str = DEFAULT_DATE_FORMAT,
                      stream=sys.stdout):
    """
    Configures the root logger with a stream handler and standardized format.
    Should be called once during app startup.
    """
    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(stream)
        ]
    )


def get_logger(name: Optional[str] = None) -> ExtendedLogger:
    """
    Returns a logger instance with the given name.
    """
    return logging.getLogger(name or __name__)  # noqa
