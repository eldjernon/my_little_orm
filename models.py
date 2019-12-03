from typing import Dict, Optional, Any
import sqlite3
import psycopg2
import psycopg2.extras
import re

SQLITE = 'sqlite'
POSTGRESQL = 'postgresql'

SUPPORTED_DBS = (
    SQLITE,
    POSTGRESQL
)

pattern = "(\w+):?(.*)@(\w+):?(\w*)\/(\w+)"


def init_database(uri: str):
    if uri.startswith(SQLITE):
        database = uri.split(':///')[1]
        connection = sqlite3.connect(database=database)
        connection.row_factory = sqlite3.Row
        return Database(engine=SQLiteEngine(connection=connection), kind=SQLITE)
    elif uri.startswith(POSTGRESQL):
        connection_str = uri.split('://')[1]
        username, password, host, port, database = re.match(pattern, connection_str).groups()
        connection = psycopg2.connect(
            f"dbname={database} user={username} password={password} host={host} port={port}"
        )
        return Database(engine=PostgresqlEngine(connection=connection), kind=POSTGRESQL)
    raise ValueError(f"Unsupported uri {str}, supported dbs: {SUPPORTED_DBS}")


class Engine:
    connection = None

    def close(self):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def execute(self, query, args):
        raise NotImplementedError()


class SQLiteEngine(Engine):

    def __init__(self, connection):
        self.connection = connection

    def close(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def execute(self, query, args):
        return self.connection.execute(query, args)


class PostgresqlEngine(Engine):

    def __init__(self, connection):
        self.connection = connection
        self.cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def close(self):
        self.cur.close()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def execute(self, query, args):
        self.cur.execute(query, args)
        return self.cur


class Database:

    def __init__(self, kind: str, engine: Engine):
        self.kind = kind
        self.engine = engine
        self.connected = True
        setattr(Model, 'db', self)

    def close(self):
        ''' Close SQL connection '''
        if self.connected:
            self.engine.close()
        self.connected = False

    def commit(self):
        ''' Commit SQL changes '''
        self.engine.commit()

    def execute(self, query, *args):
        ''' Execute SQL '''
        return self.engine.execute(query, args)


class DataMapper:

    def __init__(self, table_name, db):
        self.table_name = table_name
        self.db = db

    def get(self, id) -> Optional[Dict]:
        raise NotImplementedError()

    def all(self):
        raise NotImplementedError()

    def save(self, columns_and_values):
        raise NotImplementedError()

    def update(self, columns_and_values, id):
        raise NotImplementedError()

    def delete(self, id):
        raise NotImplementedError()


class SQLDataMapper(DataMapper):
    placeholder = None

    def get(self, id) -> Optional[Dict]:
        query = f"SELECT * FROM {self.table_name} WHERE id = {self.placeholder}"
        result = self.db.execute(query, id)
        return result.fetchone()

    def all(self):
        query = f"SELECT * FROM {self.table_name}"
        result = self.db.execute(query)
        return result.fetchall()

    def save(self, columns_and_values):
        column_names = ", ".join([column for column, _ in columns_and_values])
        column_refs = ", ".join([self.placeholder for _ in range(len(columns_and_values))])
        query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({column_refs})"
        cur = self.db.execute(query, *[val for col, val in columns_and_values])
        return cur.lastrowid

    def update(self, columns_and_values, id):
        column_names = [column for column, _ in columns_and_values]
        where_expressions = f'= {self.placeholder}, '.join(column_names) + f'= {self.placeholder}'
        query = f'UPDATE {self.table_name} SET {where_expressions} WHERE id = {self.placeholder}'
        column_values = [val for _, val in columns_and_values] + [id]
        result = self.db.execute(query, *column_values)
        return result.lastrowid

    def delete(self, id):
        query = f'DELETE from {self.table_name} WHERE id = {self.placeholder}'
        self.db.execute(query, id)


class SQLiteDataMapper(SQLDataMapper):
    placeholder = '?'


class PostgresqlDataMapper(SQLDataMapper):
    placeholder = '%s'


class DataMapperFactory:

    @classmethod
    def setup(cls, kind, table_name, db):
        if kind == SQLITE:
            return SQLiteDataMapper(table_name, db)
        elif kind == POSTGRESQL:
            return PostgresqlDataMapper(table_name, db)
        raise ValueError(f"Unsupported database type {kind}")


class Manager:
    """
    Date mapper interface
    """

    def __init__(self, db: Database, model):
        self.model = model
        self.data_mapper = DataMapperFactory.setup(db.kind, table_name=model._table_name(), db=db)

    def get(self, id):
        row = self.data_mapper.get(id)
        if not row:
            raise ValueError("Does not exists")
        return self.model(**row)

    def all(self):
        return [
            self.model(**row) for row in
            self.data_mapper.all()
        ]

    def save(self, obj):
        """
        if id is not presented creates a new row, else updates existing
        :param obj:
        :return:
        """
        columns_and_values = [(column, getattr(obj, column)) for column, _ in obj._fields()]
        if obj.id is None:
            obj.id = self.data_mapper.save(columns_and_values)
        else:
            self.data_mapper.update(columns_and_values, obj.id)
        return obj

    def delete(self, obj):
        """
        delete
        :param obj:
        :return:
        """
        self.data_mapper.delete(obj.id)


class Field:
    pass


class Model:
    """
    Abstract class to declare models
    """
    db = None

    def __init__(self, **kwargs):
        for field_name, _ in self._fields():
            if field_name not in kwargs:
                raise AttributeError(f"You must set a value for '{field_name}' field")
            else:
                setattr(self, field_name, kwargs[field_name])
        self.id = kwargs.get('id')

    def save(self):
        self.manager().save(self)

    def delete(self):
        self.manager().delete(self)

    @classmethod
    def manager(cls, db=None):
        db = db if db else cls.db
        if not db:
            raise ValueError("Database not initialized")
        return Manager(db=db, model=cls)

    @classmethod
    def _fields(cls):
        return [(field_name, value) for field_name, value in cls.__dict__.items() if isinstance(value, Field)]

    @classmethod
    def _table_name(cls):
        return cls.__name__.lower()

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id})"
