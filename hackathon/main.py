import json
import logging
import sys
from pathlib import Path

import atoti as tt
from ._env import env

# Session configuration
_JAVA_OPTIONS = ["-Xmx10g", "-XX:MaxDirectMemorySize=20g"]

# BigQuery connection configuration
_BIG_QUERY_CONNECTION_CONFIG = {
    "type": "service_account",
    "project_id": "cubulus-tests",
    "client_email": "minimal-bigquery-permissions@cubulus-tests.iam.gserviceaccount.com",
    "client_id": "108522642720406669397",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/minimal-bigquery-permissions%40cubulus-tests.iam.gserviceaccount.com",
}
_BIG_QUERY_CONNECTION_CONFIG_PATH = Path("GoogleBigQuery.json")

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


# def _build_connection_info_file() -> None:
#     big_query_connection_config = {
#         **_BIG_QUERY_CONNECTION_CONFIG,
#         "private_key": env.private_key,
#         "private_key_id": env.private_key_id,
#     }
#     with _BIG_QUERY_CONNECTION_CONFIG_PATH.open("w") as f:
#         f.write(json.dumps(big_query_connection_config))
# 
# 
# def _get_bigquery_connection_info() -> tt.BigqueryConnectionConfig:
#     return tt.BigqueryConnectionConfig(credentials=_BIG_QUERY_CONNECTION_CONFIG_PATH)
# 
# 
# def _connect_to_bigquery(session: tt.Session) -> tt.ExternalDatabaseConnection:
#     _LOGGER.info("Connecting to BigQuery database")
#     _build_connection_info_file()
#     connection_info = _get_bigquery_connection_info()
#     big_query_connection = session.connect_to_external_database(connection_info)
#     _LOGGER.info("Connected to BigQuery database")
#     return big_query_connection
# 
# 
# def _add_external_table(
#     *,
#     session: tt.Session,
#     table_name: str,
#     bigquery_connection: tt.ExternalDatabaseConnection,
#     table_options: tt.BigqueryTableConfig| None = None,
# ) -> tt.Table:
#     _LOGGER.info("Adding external table %s", table_name)
#     db_table = bigquery_connection.tables[table_name]
#     if table_options:
#         table = session.add_external_table(db_table, config=table_options)
#     else:
#         table = session.add_external_table(db_table)
#     _LOGGER.info("External table %s added", table_name)
#     return table
# 
# 
# def _setup_hierarchies(*, cube: tt.Cube, pnl_table: tt.Table):
#     _LOGGER.info("Setting up hierarchies")
#     h, lvl = cube.hierarchies, cube.levels
#     h[_BOOK_ID_HIERARCHY_NAME] = [pnl_table[_BOOK_ID_COLUMN_NAME]]
#     h[_AS_OF_DATE_HIERARCHY_NAME].slicing = True
#     h[_AS_OF_DATE_HIERARCHY_NAME].order = tt.NaturalOrder(ascending=False)
#     h[_TRADE_ID_HIERARCHY_NAME].virtual = True
#     h[_TRADING_BOOK_HIERARCHY_NAME] = {
#         _BUSINESS_UNIT: lvl[_BUSINESS_UNIT_HIERARCHY],
#         _SUB_BUSINESS_UNIT: lvl[_SUB_BUSINESS_UNIT_HIERARCHY],
#         _DESK: lvl[_TRADING_DESK_HIERARCHY],
#         _BOOK: lvl[_BOOK_HIERARCHY],
#     }
#     del h[_BUSINESS_UNIT_HIERARCHY]
#     del h[_SUB_BUSINESS_UNIT_HIERARCHY]
#     del h[_TRADING_DESK_HIERARCHY]
#     del h[_BOOK_HIERARCHY]
#     _LOGGER.info("Finished setting up hierarchies")
# 
# 
# def _setup_measures(*, cube: tt.Cube):
#     _LOGGER.info("Setting up measures")
#     h, m = cube.hierarchies, cube.measures
#     m[_PNL_VECTOR] = m[_PNL_VECTOR_SUM]
#     m[_VAR_95] = tt.array.quantile(m[_PNL_VECTOR], _VAR_95_QUANTILE)
#     m[_VAR_99] = tt.array.quantile(m[_PNL_VECTOR], _VAR_99_QUANTILE)
#     m[_EXPECTED_SHORTFALL] = tt.array.mean(
#         tt.array.n_lowest(m[_PNL_VECTOR], n=_EXPECTED_SHORTFALL_ELEMENTS)
#     )
#     m[_PARENT_PNL_VECTOR_EX] = tt.agg.sum(
#         m[_PNL_VECTOR],
#         scope=tt.SiblingsScope(
#             hierarchy=h[_TRADING_BOOK_HIERARCHY_NAME],
#             exclude_self=True,
#         ),
#     )
#     m[_PARENT_VAR_95] = tt.parent_value(
#         m[_VAR_95],
#         degrees={
#             h[_TRADE_ID_HIERARCHY_NAME]: 1,
#         },
#     )
#     m[_PARENT_VAR_95_EX] = tt.array.quantile(m[_PARENT_PNL_VECTOR_EX], _VAR_95_QUANTILE)
#     m[_INCREMENTAL_VAR_95] = m[_VAR_95] - m[_PARENT_VAR_95_EX]
#     _LOGGER.info("Finished setting up measures")


def _setup_session(session: tt.Session, /):
    _LOGGER.info("Setting up session")
    # bigquery_connection = _connect_to_snowflake(session)
    _LOGGER.info("Adding external tables")
    # pnl_table = _add_external_table(
    #     session=session,
    #     table_name=_PNL_TABLE_NAME,
    #     bigquery_connection=bigquery_connection,
    #     table_options=tt.BigqueryTableConfig(
    #         array_conversion=tt.MultiColumnArrayConversion(
    #             column_prefixes=["PNL_VECTOR"],
    #         ),
    #     ),
    # )
    # book_table = _add_external_table(
    #     session=session,
    #     table_name=_BOOK_TABLE_NAME,
    #     bigquery_connection=bigquery_connection,
    # )
    # _LOGGER.info("Joining tables")
    # pnl_table.join(
    #     book_table, pnl_table[_BOOK_ID_COLUMN_NAME] == book_table[_BOOK_ID_COLUMN_NAME]
    # )
    # _LOGGER.info("Creating cube")
    # cube = session.create_cube(pnl_table)
    # _setup_hierarchies(cube=cube, pnl_table=pnl_table)
    # _setup_measures(cube=cube)
    # _LOGGER.info("Session running and available at localhost:%i", session.port)


def local_main():
    with tt.Session(java_options=_JAVA_OPTIONS) as session:
        _setup_session(session)
        session.wait()


def paas_main():
    with tt.Session._connect("127.0.0.1") as session:
        _setup_session(session)