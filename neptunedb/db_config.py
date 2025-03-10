import os
from dataclasses import dataclass


@dataclass
class DBConfig:
    host: str
    database: str
    user: str
    password: str
    port: int = 5432

    @staticmethod
    def from_env() -> "DBConfig":
        return DBConfig(
            host=os.getenv("PGLOCALHOST"),
            database=os.getenv("POSTGRESDB"),
            user=os.getenv("PGLOCALUSER"),
            password=os.getenv("POSTGRESPASSWORD"),
        )
