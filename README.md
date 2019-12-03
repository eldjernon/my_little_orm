# my_little_orm

### Example of using:

```python
from models import Model, Field, init_database

class Person(Model):
    name = Field()
    surname = Field()

db = init_database("sqlite:///foo.db")
p = Person(name='almaz', surname="galiev")
p.save()
db.commit()
p = Person.manager().get(p.id)
persons = Person.manager().all()
p.delete()
db.commit()
```


### TODO

- [x] Implement models declaration layer (1h) [1h]
- [x] Implement database management layer (1h) [1h]
- [x] Implement layers of connection to databases and expression building (4h) [5h]
- [x] Write Tests (2h) [3h]
- [x] Add Postresql support (3h) [3h]
- [ ] Increase coverage to 85 percent (2h)
