from models import Model, Field, Database, SQLITE, init_database
import pytest

class Person(Model):
    name = Field()
    surname = Field()


@pytest.fixture()
def db(mocker):
    return Database(kind=SQLITE, engine=mocker.Mock())


def test_get(mocker, db):
    row = {"name": "john", "surname":"smith", "id":1}
    result_mock = mocker.Mock(**{"fetchone.return_value": row})
    db.engine.execute.return_value = result_mock
    person = Person.manager().get(id=1)
    assert person.name == row['name']
    assert person.surname == row['surname']
    assert person.id == row['id']
    db.engine.execute.assert_called_once_with(
        'SELECT * FROM person WHERE id = ?', (1,)
    )


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