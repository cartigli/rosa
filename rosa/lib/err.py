"""Standardized error handling & ExitCodes."""

import os
import sys
import logging

logger = logging.getLogger('rosa.log')

class exCode:
     SUCCESS = 0
     KEYBOARD_INTERRUPT = 1
     CONNECTION_ERROR = 2
     FILENOTFOUND_ERROR = 3
     PERMISSION_ERROR = 4
     CONFIGURATION_ERROR = 5
     VERSION_MISALIGNMENT = 6
     INDEXNOTFOUND_ERROR = 7
     UNKNOWN_ERROR = 8
     NO_ERROR = 9

class Error:
     """Error handler & code manager.

     Attributes:
          context (str): Optional error message for logging.

     Static Methods:
          Keyboard(): KeyboardInterrupt handling.
          Connection(): ConnectionError handling.
          FileNotFound(): FileNotFoundError handling.
          Permission(): PermissionError handling.
          IndexNotFound(): IndexNotFoundError handling.
          MisalignedVersions(): MisalignedVersionError handling.
     """
     def __init__(self, context: str = ""):
          """Initiates context for instance of Error."""
          self.context = context

     @staticmethod
     def Keyboard():
          """Handler for manual intervention during runtime.

          Args:
               None
          
          Returns:
               None
          """
          message: str = f"boss killed the process"
          level: str = "warning"
          if self.context:
               message: str += f" during {self.context}"

          Exit(exCode.KEYBOARD_INTERRUPT, message, level)

     @staticmethod
     def Connection():
          """Handler for Connection Errors during runtime.

          Args:
               None

          Returns:
               None
          """
          message: str = f"Connection Error encountered"
          if self.context:
               message: str += f" due to: {self.context}"

          Exit(exCode.CONNECTION_ERROR, message)

     @staticmethod
     def FileNotFound():
          """Handler for FileNotFound Errors during runtime.

          Args:
               None

          Returns:
               None
          """
          message: str = f"FileNotFoundError"
          if self.context:
               message: str += f" during {self.context}"

          Exit(exCode.FILENOTFOUND_ERROR, message, xi=True)

     @staticmethod
     def Permission():
          """Handler for Permission Errors during runtime.

          Args:
               Nnoe

          Returns:
               None
          """
          message: str = f"PermissionError"
          if self.context:
               message: str += f" during {self.context}"

          Exit(exCode.PERMISSION_ERROR, message, xi=True)

     @staticmethod
     def IndexNotFound():
          """Handler for FileNotFound Errors during runtime.

          Args:
               None

          Returns:
               None
          """
          level: str = "warning"
          message: str = "IndexNotFoundError"

          if context:
               message: str += f" during {context}"

          Exit(exCode.INDEXNOTFOUND_ERROR, message, level="warning")

     @staticmethod
     def MisalignedVersions(vss: tuple = None):
          """Handler for Version Errors during runtime.

          Args:
               vss (tuple): The misaligned versions.

          Returns:
               None
          """
          message: str = f"Versions misaligned"

          if vss:
               message: str += f": (remote: {vss[0]}), (local: {vss[1]})"
          else:
               message: str += f"!"

          Exit(exCode.VERSION_MISALIGNMENT, message, level="critical")


def Exit(code: int = None, message: str = None, level: str = None, xi: bool = False):
     """Logs error and exits with a specific Error code.
     
     Args:
          code (int): Error code give.
          message (str): Message passed for logging.
          level (str): Logging level, if given.
          xi (bool): Include exc_info.

     Returns:
          None
     """
     levels: dict = {
          "info": logger.info,
          "warning": logger.warning,
          "critical": logger.critical
     }
     if level:
          level: str = level.lower()
     log_fx = levels.get(level, logger.error)

     if not message:
          message: str = "[no message provided]"

     log_fx(message, exc_info=xi)

     sys.exit(code)


class Task:

     def __init__(self, string: str = ""):
          self.core = string + "!"
          self.location = f"hometown: {string}"
     
     def go_home(self):
          print(self.core, "is going to ", self.location)

me = Task("Jones")

me.go_home()

Task("Jones").go_home()