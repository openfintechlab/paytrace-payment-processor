# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Service Template / starter code for PayTrace SCA Service.
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/12
"""

from utilities.Logging      import Logging
from utilities.ConfigLoader import ConfigLoader 
from utilities.DBHelper     import DBHelper
from contextlib             import asynccontextmanager
import sys


def initialize_service():
    try:        
        result = DBHelper.initialize_connection()        
        if not result:
            Logging.info("Database connection is not established.")
            Logging.error("Failed to initialize database connection during startup.")
            raise RuntimeError("Database initialization failed. Service startup aborted.")
    except Exception as ex:
        Logging.info("Database connection is not established.")
        Logging.error(f"Database startup error: {ex}")
        raise

# Default variables
_DEFAULT_LOG_FORMAT = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
_DEFAULT_LOG_LEVEL  = "INFO"
# END;


def displayBanner():
    Logging.info("===============================================")
    Logging.info("Starting PayTrace Payment Processor")
    Logging.info(f"Version: {ConfigLoader.get('OFTL_SCA_VERSION', 'N/A')}")
    Logging.info(f"Database: {ConfigLoader.get('OFTL_POSTGRESDB_NAME', "N/A")}")
    Logging.info(f"Database Host: {ConfigLoader.get('OFTL_POSTGRESDB_HOST', "N/A")}")
    Logging.info(f"Log Level: {ConfigLoader.get('OFTL_LOG_LEVEL', _DEFAULT_LOG_LEVEL)}")
    Logging.info("===============================================")

    pass


if __name__ == "__main__":
    try:
        displayBanner()
        initialize_service()
        Logging.info("PayTrace Payment Processor started successfully.")
        # TODO: Add main processing logic here
        # For now, just keep running or exit
        import time
        while True:
            time.sleep(60)  # Keep alive, replace with actual logic
    except Exception as e:
        Logging.error(f"Error starting Payment Processor")  
        Logging.error(str(e))
        sys.exit(91)
