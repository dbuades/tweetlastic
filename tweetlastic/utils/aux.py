# Standard
import sys
import logging

def set_logging_level(logging_level):
    '''
    We load the string from the settings file, we parse it as a variable.
    sys.tracebacklimit = 0 is for not polluting the log file with tracebacks, except when debugging.
    '''

    if logging_level == "DEBUG":
        logging_level = logging.DEBUG

    elif logging_level == "INFO":
        logging_level = logging.INFO
        sys.tracebacklimit = 0
    
    elif logging_level == "WARNING":
        logging_level = logging.WARNING
        sys.tracebacklimit = 0

    elif logging_level == "ERROR":
        logging_level = logging.ERROR
        sys.tracebacklimit = 0

    return logging_level