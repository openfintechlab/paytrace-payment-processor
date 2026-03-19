# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Service Template / starter code for PayTrace SCA Service.
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/14
"""

import sys
import time

from utilities.Logging          import Logging
from utilities.ConfigLoader     import ConfigLoader
from utilities.DBHelper         import DBHelper
from utilities.RabbitMQHelper   import RabbitMQConnectionError, RabbitMQHelper



def initialize_service():
    try:
        result = DBHelper.initialize_connection()
        if not result:
            Logging.info("Database connection is not established.")
            Logging.error("Failed to initialize database connection during startup.")
            raise RuntimeError("Database initialization failed. Service startup aborted.")
        RabbitMQHelper.initialize_connection()
        RabbitMQHelper.start_listener()
    except Exception as ex:
        Logging.info("Service dependencies are not fully established.")
        Logging.error(f"Service startup error: {ex}")
        raise

# Default variables
_DEFAULT_LOG_LEVEL  = "INFO"
# END;


def displayBanner():
    Logging.info("===============================================")
    Logging.info("Starting PayTrace Payment Processor")
    Logging.info(f"Version: {ConfigLoader.get('OFTL_SCA_VERSION', 'N/A')}")
    Logging.info(f"Database: {ConfigLoader.get('OFTL_POSTGRESDB_NAME', 'N/A')}")
    Logging.info(f"Database Host: {ConfigLoader.get('OFTL_POSTGRESDB_HOST', 'N/A')}")
    Logging.info(
        f"RabbitMQ Domestic Queue: "
        f"{ConfigLoader.get('OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE', 'CSV.PAYMENTS.DOMESTIC.REQ')}"
    )
    Logging.info(
        f"RabbitMQ Cross Border Queue: "
        f"{ConfigLoader.get('OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE', 'CSV.PAYMENTS.CROSS_BORDER.REQ')}"
    )
    Logging.info(f"Log Level: {ConfigLoader.get('OFTL_LOG_LEVEL', _DEFAULT_LOG_LEVEL)}")
    Logging.info("===============================================")

    pass


if __name__ == "__main__":
    try:
        displayBanner()
        initialize_service()
        Logging.info("PayTrace Payment Processor started successfully.")        
        while True:
            time.sleep(60)  # Keep alive, replace with actual logic
    except RabbitMQConnectionError as e:
        Logging.error("Error starting Payment Processor")
        Logging.error(str(e))
        RabbitMQHelper.stop_listener()
        sys.exit(99)
    except Exception as e:
        Logging.error("Error starting Payment Processor")
        Logging.error(str(e))
        RabbitMQHelper.stop_listener()
        sys.exit(91)
    finally:
        RabbitMQHelper.stop_listener()
