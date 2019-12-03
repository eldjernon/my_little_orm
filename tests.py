from models import (
    Model,
    Field,
    Database,
    SQLITE,
    POSTGRESQL,
    init_database,
    PostgresqlEngine,
    DataMapperFactory,
    SQLDataMapper,
    PostgresqlDataMapper
)
import pytest


class Person(Model):
    name = Field()
    surname = Field()


@pytest.fixture()
def db(mocker):
    return Database(kind=SQLITE, engine=mocker.Mock())


@pytest.fixture()
def postgres_engine(mocker):
    return PostgresqlEngine(connection=mocker.Mock())


def test_get(mocker, db):
    row = {"name": "john", "surname": "smith", "id": 1}
    result_mock = mocker.Mock(**{"fetchone.return_value": row})
    db.engine.execute.return_value = result_mock
    person = Person.manager().get(id=1)
    assert person.name == row['name']
    assert person.surname == row['surname']
    assert person.id == row['id']
    db.engine.execute.assert_called_once_with(
        'SELECT * FROM person WHERE id = ?', (1,)
    )


def test_all(mocker, db):
    row = {"name": "john", "surname": "smith", "id": 1}
    result_mock = mocker.Mock(**{"fetchall.return_value": [row]})
    db.engine.execute.return_value = result_mock
    persons = Person.manager().all()
    assert len(persons) == 1
    person = persons[0]
    assert person.name == row['name']
    assert person.surname == row['surname']
    assert person.id == row['id']
    db.engine.execute.assert_called_once_with('SELECT * FROM person', ())


def test_save(db):
    person = Person(name='john', surname='smith')
    person.save()
    db.engine.execute.assert_called_once_with(
        'INSERT INTO person (name, surname) VALUES (?, ?)', ('john', 'smith')
    )


def test_update(db):
    person = Person(id=1, name='john', surname='smith')
    person.save()
    db.engine.execute.assert_called_once_with(
        'UPDATE person SET name= ?, surname= ? WHERE id = ?', ('john', 'smith', 1)
    )


def test_delete(db):
    person = Person(id=1, name='john', surname='smith')
    person.delete()
    db.engine.execute.assert_called_once_with('DELETE from person WHERE id = ?', (1,))


def test_should_raise_error_for_unsupported_databases():
    with pytest.raises(ValueError):
        init_database("")


def test_model_manager_should_check_db():
    Person.db = None
    with pytest.raises(ValueError):
        Person.manager()


def tests_init_database_sql_alchemy(mocker):
    sqlite3_mock = mocker.patch('models.sqlite3')
    uri = "sqlite:///foo.db"
    init_database(uri)
    sqlite3_mock.connect.assert_called_once_with(database='foo.db')


@pytest.mark.parametrize("uri,called_string", [
    (
            "postgresql://user:2349i$5@localhost/db_name",
            'dbname=db_name user=user password=2349i$5 host=localhost port='
    ),
    (
            "postgresql://user@localhost/db_name",
            'dbname=db_name user=user password= host=localhost port='
    ),
    (
            "postgresql://user:2349i$5@localhost:1234/db_name",
            'dbname=db_name user=user password=2349i$5 host=localhost port=1234'
    ),
    (
            "postgresql://user@localhost:1234/db_name",
            'dbname=db_name user=user password= host=localhost port=1234'
    ),
])
def test_init_database_postgresql(uri, called_string, mocker):
    psycopg2_mock = mocker.patch('models.psycopg2')
    init_database(uri)
    psycopg2_mock.connect.assert_called_once_with(called_string)


def test_postgresql_engine_close(postgres_engine):
    postgres_engine.close()
    postgres_engine.connection.close.assert_called_once()
    postgres_engine.cur.close.assert_called_once()


def test_postgresql_engine_commit(postgres_engine):
    postgres_engine.commit()
    postgres_engine.connection.commit.assert_called_once()


def test_postgresql_engine_execute(postgres_engine):
    postgres_engine.execute("test", "test")
    postgres_engine.cur.execute.assert_called_once_with("test", "test")


@pytest.mark.parametrize("kind,data_mapper_class",
                         [
                             (SQLITE, SQLDataMapper),
                             (POSTGRESQL, PostgresqlDataMapper)
                         ])
def test_data_mapper_factory(kind, data_mapper_class, mocker):
    db_mock = mocker.Mock()
    data_mapper = DataMapperFactory.setup(kind, "test", db_mock)
    assert isinstance(data_mapper, data_mapper_class)


def test_data_mapper_should_raise_exception_for_unsupported_database_kind():
    with pytest.raises(ValueError):
        DataMapperFactory.setup("test", None, None)
