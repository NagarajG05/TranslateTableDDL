from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def generate_db_engine(db_type: str, kwargs: dict):
    if db_type == 'snowflake':
        db_engine = create_engine(URL(
            drivername='snowflake',
            account=kwargs.get('account'),
            user=kwargs.get('username'),
            password=kwargs.get('password'),
            authenticator=kwargs.get('authenticator'),
            database=kwargs.get('database'),
            warehouse=kwargs.get('warehouse'),
            schema=kwargs.get('schema'),
            role=kwargs.get('role')
        ))
    elif db_type == 'hana':
        db_engine = create_engine(
            f"hana://{kwargs.get('username')}@{kwargs.get('host')}/{kwargs.get('database')}"
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    return db_engine
