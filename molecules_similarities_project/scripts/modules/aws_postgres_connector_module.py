import logging

import psycopg2
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.orm import sessionmaker


class PostgresConnectorBaseException(
    Exception
):
    pass


class PostgresConnectorIntegrityError(
    PostgresConnectorBaseException
):
    pass


class PostgresConnectorConnectionError(
    PostgresConnectorBaseException
):
    pass


class PostgresConnectorSqlRelatedError(
    PostgresConnectorBaseException
):
    pass


class PostgresConnector:
    """
    The class contains different Postgres database connection methods.
    """

    def __init__(self, pg_database, pg_host, pg_user, pg_password, pg_port):

        self.pg_database = pg_database
        self.pg_host = pg_host
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_port = pg_port
        self.conn = self.get_connection()

    def get_connection(self):
        """
        This method is designed to create and return a connection object
        to a specified PostgreSQL database, facilitating the execution of
        SQL queries and other database operations.
        """
        conn = psycopg2.connect(
            dbname=self.pg_database,
            user=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port
        )
        return conn

    def connect_conn(self, query):
        """
        The method allows executing a SQL query on a PostgreSQL database and
        fetching all results.
        :param query: SQL query to execute
        :return: fetched rows from the executed query
        """
        cursor = self.conn.cursor()
        cursor.execute(query)

        rows = cursor.fetchall()

        cursor.close()
        self.conn.close()

        return rows

    def connect_for_bulk_load(self, df, base_class, table_to_insert):
        """
        The method allows to perform Bulk insert using ORM model.
        :param df: a piece of data to insert
        :param base_class: structure definition of a table in which
         data should be inserted. All tables definitions are stored in
         chembl_tables_definition_class.py module.
        :param table_to_insert: database table name
        """
        engine = create_engine(
            f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}: \
                           {self.pg_port}/{self.pg_database}"
        )
        Session = sessionmaker(
            bind=engine
        )
        session = Session()

        data = df.to_dict(
            orient="records"
        )
        try:

            session.bulk_insert_mappings(
                base_class,
                data
            )
            session.commit()
            logging.info(
                f"Inserted {len(data)} rows into Postgres table {table_to_insert}"
            )

        except IntegrityError as ie:
            session.rollback()
            raise PostgresConnectorIntegrityError(
                f"IntegrityError occurred: {ie}"
            )

        except OperationalError as oe:
            session.rollback()
            raise PostgresConnectorConnectionError(
                f"Operational Error occurred: {oe}"
                f"Check your connection settings."
            )

        except ProgrammingError as pe:
            session.rollback()
            raise PostgresConnectorSqlRelatedError(
                f"Programming Error occurred: {pe}"
            )

        finally:
            session.close()

    def connect_for_row_by_row_load(self, df, table_name, unique_constraint):
        """
        The method inserts rows from a DataFrame into a PostgreSQL table,
        handling conflicts by doing nothing for existing rows. This method
        is useful for inserting data row-by-row into a table and avoiding
        duplicate entries.
        :param df: A Pandas DataFrame containing the data to be inserted.
        :param table_name: The name of the PostgreSQL table where the data
               will be inserted.
        :param unique_constraint: The column name or index element used to
               identify unique entries and avoid duplicates.
        """
        engine = create_engine(
            f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}: \
                                   {self.pg_port}/{self.pg_database}"
        )

        metadata = MetaData()
        table_to_insert = Table(
            table_name,
            metadata,
            autoload_with=engine
        )

        with engine.begin() as connection:
            for index, row in df.iterrows():
                stmt = pg_insert(table_to_insert).values(**row.to_dict())
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[unique_constraint]
                )
                connection.execute(stmt)
