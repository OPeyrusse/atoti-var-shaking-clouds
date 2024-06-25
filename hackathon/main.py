import json
import logging
import sys
from pathlib import Path

import atoti as tt
from ._env import env

# Session configuration
_JAVA_OPTIONS = ["-Xmx10g", "-XX:MaxDirectMemorySize=20g"]

# Tables
_BOOK_TABLE_NAME = "BOOKS"
_PNL_TABLE_NAME = "TRADE_PNLS"

# Columns
_BOOK_ID_COLUMN_NAME = "BOOKID"

# Hierarchies and Levels
_BOOK_ID_HIERARCHY_NAME = "BOOKID"
_AS_OF_DATE_HIERARCHY_NAME = "ASOFDATE"
_TRADE_ID_HIERARCHY_NAME = "TRADEID"
_BUSINESS_UNIT_HIERARCHY = "BUSINESS_UNIT"
_SUB_BUSINESS_UNIT_HIERARCHY = "SUB_BUSINESS_UNIT"
_TRADING_DESK_HIERARCHY = "TRADING_DESK"
_BOOK_HIERARCHY = "BOOK"
_TRADING_BOOK_HIERARCHY_NAME = "Trading Book Hierarchy"
_BUSINESS_UNIT = "Business Unit"
_SUB_BUSINESS_UNIT = "Sub Business Unit"
_DESK = "Desk"
_BOOK = "Book"

# Measures
_PNL_VECTOR = "PNL_VECTOR"
_PNL_VECTOR_SUM = "PNL_VECTOR.SUM"
_VAR_95 = "VaR95"
_VAR_95_QUANTILE = 1 - 0.95
_VAR_99 = "VaR99"
_VAR_99_QUANTILE = 1 - 0.99
_EXPECTED_SHORTFALL = "ES"
_EXPECTED_SHORTFALL_ELEMENTS = 12
_PARENT_PNL_VECTOR_EX = "Parent PnL Vector Ex"
_PARENT_VAR_95 = "Parent VaR95"
_PARENT_VAR_95_EX = "Parent VaR95 Ex"
_INCREMENTAL_VAR_95 = "Incremental VaR95"


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(process)s --- [%(threadName)s] %(name)s : %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_LOGGER = logging.getLogger(__name__)


def _setup_session(session: tt.Session, /):
    _LOGGER.info("Setting up session")
    # bigquery_connection = _connect_to_snowflake(session)
    _LOGGER.info("Adding external tables")


def local_main():
    with tt.Session(java_options=_JAVA_OPTIONS) as session:
        _setup_session(session)
        session.wait()


def paas_main():
    with tt.Session._connect("127.0.0.1") as session:
        _setup_session(session)