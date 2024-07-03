import logging

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PostgresConnector:

    """
      The method allows executing a SQL query on a PostgreSQL database and
      fetching all results.
      :param query: SQL query to execute
      :return: fetched rows from the executed query
      """
    def __init__(self, pg_database, pg_host, pg_user, pg_password, pg_port):

        self.pg_database = pg_database
        self.pg_host = pg_host
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_port = pg_port

    def connect_conn(self, query):

        conn = psycopg2.connect(
            dbname=self.pg_database,
            user=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port
        )
        cursor = conn.cursor()
        cursor.execute(
            query
        )

        rows = cursor.fetchall()

        cursor.close()
        conn.close()

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
            orient='records'
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

        except Exception as e:
            session.rollback()
            logging.error(f"Error occurred: {e}")

        finally:
            session.close()
