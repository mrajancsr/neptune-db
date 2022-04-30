# pyre-strict
# Notes
# To run the file, follow the following steps:
# -- 1) ensure there is a sql_config directory in phobos-lab directory
# -- 2) make a environment variable named PHOBOSSQLCONFIGPATH
#    that points to location of sql_config dir
# -- 3) Ensure timescale.pem is located in users Home dir

# -- Currently only darwin and linux platforms are supported


import os
import tempfile
from configparser import ConfigParser
from dataclasses import dataclass, field
from sys import platform
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

import asyncpg
import pandas as pd
import psycopg2
from paramiko import Ed25519Key
from psycopg2.extras import DictCursor, execute_values
from sshtunnel import SSHTunnelForwarder


@dataclass
class PlatformNotSupportedError(Exception):
    pass


@dataclass
class SectionNotExists(Exception):
    pass


# -- aws credentials
AURORAENDPOINT = os.environ.get("AURORAENDPOINT")
AURORADB = os.environ.get("AURORADB")
AURORAUSER = os.environ.get("AURORAUSER")
AURORAPASSWORD = os.environ.get("AURORAPASSWORD")

# -- local credentials
LOCALHOST = os.environ.get("PGLOCALHOST")
POSTGRESUSER = os.environ.get("PGLOCALUSER")
POSTGRESDB = os.environ.get("POSTGRESDB")
POSTGRESPASSWORD = os.environ.get("POSTGRESPASSWORD")

if platform == "darwin":
    path_to_secret_key = os.path.expanduser("~/timescale.pem")
elif platform == "linux":
    path_to_secret_key = None
else:
    raise PlatformNotSupportedError("Only supported in linux or darwin systems")

# custom Type to represent psycopg2 connection and sshtunnel
connection = TypeVar("connection")
sshtunnel = TypeVar("sshtunnel")


@dataclass
class DBReader:
    section: str = field(init=False, default="neptunequantdev-dev")
    tunnel: Optional[sshtunnel] = field(init=False, default=None)
    pkey: Optional[Ed25519Key] = field(init=False, default=None)
    column_names: List[str] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        if platform == "darwin":
            self.pkey = Ed25519Key.from_private_key_file(path_to_secret_key)
            self.tunnel = self._create_tunnel()
            self.tunnel.start()
        elif platform == "linux" and not AURORAENDPOINT:
            self.section = "neptunequantdev-prod"
        elif platform == "linux" and AURORAENDPOINT:
            self.section = "neptunequantdev-awsauroraprod"

    async def __aenter__(self):
        self.tunnel = self._create_tunnel()
        self.tunnel.start()
        self.conn = await self.async_connect()
        return self

    async def __aexit__(self, *args):
        await self.conn.close()
        self.tunnel.close()

    def _create_tunnel(self) -> sshtunnel:
        tunnel = SSHTunnelForwarder(
            ("204.236.250.15", 22),
            ssh_username="ubuntu",
            ssh_pkey=self.pkey,
            remote_bind_address=("localhost", 5432),
        )
        return tunnel


    async def async_connect(self) -> Optional[connection]:
        """Connects to the postgresql securities_master db

        Returns
        -------
        Optional[connection]
            a connection object or None if failed to connect
        """
        try:
            params = self.get_credentials()
            conn = await asyncpg.connect(**params)
            if conn:
                print("Connected to Postgres")
                return conn
        except asyncpg.ConnectionDoesNotExistError as error:
            print(error)

    async def async_push(
        self, data: Iterator[Tuple[str, ...]], table_name: str, columns: List[str]
    ) -> None:
        """Pushes data to postgresql database
        Parameters
        ----------
        data : Iterator[Dict[str, Any]]
            to push to postgresql database
        table_name : str
            the table to push to
        columns : List[str]
            column names of the data
        Raises
        ------
        asyncpg.DatabaseError
            if the table doesn't exist or incorrect data format
        """
        # insert data into database and close connection
        try:
            await self.conn.copy_records_to_table(table_name, records=data, columns=columns)
        except Exception as e:
            print("error: ", e)

    def get_credentials(self) -> Optional[Dict[str, Any]]:
        if platform == "darwin" and self.tunnel.is_active:
            port = self.tunnel.local_bind_port
        elif platform == "linux":
            port = "5432"
        params = {}
        if AURORAENDPOINT:
            # gets the credentials from .aws/credentials
            params["host"] = AURORAENDPOINT
            params["database"] = AURORADB
            params["user"] = AURORAUSER
            params["password"] = AURORAPASSWORD
        elif LOCALHOST:
            # get credentials from localhost
            params["host"] = LOCALHOST
            params["database"] = POSTGRESDB
            params["user"] = POSTGRESUSER
            params["password"] = POSTGRESPASSWORD
        params["port"] = port
        return params

    def connect(self) -> Optional[connection]:
        """Connects to the postgresql securities_master db

        Returns
        -------
        Optional[connection]
            a connection object or None if failed to connect
        """
        try:
            params = self.get_credentials()
            conn = psycopg2.connect(**params)
            if conn:
                print("Connected to PostgresDB")
            return conn
        except psycopg2.DatabaseError as error:
            print(error)

    async def async_fetch(self, query: str):
        rows = await self.conn.fetch(query)
        return rows

    def fetch(self, query: str) -> Optional[Tuple[Dict[str, Any]]]:
        """Returns data associated with the table

        Parameters
        ----------
        query : str
            [description]

        Returns
        -------
        Optional[Tuple[Dict[str, Any]]]
            Tuple of Table rows
        """

        try:
            conn = self.connect()
            if conn:
                with conn.cursor(cursor_factory=DictCursor) as curr:
                    curr.execute(query)
                    self.column_names = [col.name for col in curr.description]
                    rows = curr.fetchall()
                conn.close()
                return rows
        except psycopg2.DatabaseError as error:
            print(error)

    def fetchdf(self, query: str) -> pd.DataFrame:
        """Returns a pandas dataframe of the db query"""
        return pd.DataFrame(self.fetch(query), columns=self.column_names)

    def drop(self, table_name: str) -> None:
        """removes table given by table_name from dev db

        Parameters
        ----------
        table_name : str
            the table in database
        """
        self.execute(f"drop table {table_name};")

    def execute(self, query: Union[str, Tuple[str]]) -> None:
        """Executes an query statement

        Parameters
        ----------
        query : Union[str, Tuple[str]]
            a single statement or tuple of queries
        """
        try:
            conn = self.connect()
            if conn:
                with conn.cursor() as curr:
                    if isinstance(query, str):
                        curr.execute(query)
                    elif isinstance(query, tuple):
                        for q in query:
                            curr.execute(q)
                conn.commit()
                conn.close()
        except Exception as e:
            print(e)

    def push(
        self,
        data: Iterator[Tuple[str, ...]],
        table_name: str,
        columns: List[str],
    ) -> None:
        """Pushes data to postgresql database
        Parameters
        ----------
        data : Iterator[Dict[str, Any]]
            to push to postgresql database
        table_name : str
            the table to push to
        columns : List[str]
            column names of the data
        Raises
        ------
        psycopg2.DatabaseError
            if the table doesn't exist or incorrect data format
        """
        conn = self.connect()
        cursor = conn.cursor()
        # get the column names
        col_names = ",".join(columns)
        query = f"""INSERT INTO {table_name} ({col_names}) values %s"""
        # insert data into database and close connection
        try:
            execute_values(cursor, query, data)
            conn.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as e:
            print("error: ", e)
            conn.rollback()
            cursor.close()
        finally:
            conn.close()

    def copy_from_csv(
        self, data: pd.DataFrame, table_name: str, name: Optional[str] = ""
    ) -> None:
        """Copies data to table_name in securities_master db

        Parameters
        ----------
        data : pd.DataFrame
            large data to be pushed to table_name
        table_name : str
            table in securities_master
        name : Optional[str],
            name of the data, default=''
        """
        if data is None:
            return
        size = data.shape[0]
        print(f"Ready to push {('' if not name else name)} with size {size} rows")
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_path = os.path.join(tmp_dir, "output.csv")
            data.to_csv(out_path, index=False, header=False)
            with open(out_path, "r") as f:
                next(f)
                conn = self.connect()
                cursor = conn.cursor()
                try:
                    cursor.copy_from(f, table_name, sep=",", null="")
                    conn.commit()
                    cursor.close()
                except (Exception, psycopg2.DatabaseError) as e:
                    print("error: ", e)
                    conn.rollback()
                    cursor.close()
                finally:
                    conn.close()

        print(f"pushed {('' if not name else name)} to {table_name} table")


if __name__ == "__main__":
    db = DBReader()
    print(db.fetchdf("select * from min_bars limit 30"))
