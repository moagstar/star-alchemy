# star-alchemy

Extends SQLAlchemy to make it easy to deal with star, snowflake and
galaxy schemas.

## Features

- Define topology using nested dictionaries

```python
>>> schema = StarSchema.from_dicts({
...     tables.sale: {
...         tables.product: {
...             tables.category: {},
...         },
...         tables.employee: {
...             tables.department: {},
...             tables.location.alias('employee_location'): {},
...         },
...         tables.customer: {
...             tables.location.alias('customer_location'): {},
...         },
...     },
... })
```

- Don't worry about which joins to make:

```python
>>> normalize_query(query_str(
...      schema.select([schema.tables["department"].c.id])
... ))
SELECT department.id
FROM sale
LEFT JOIN employee ON sale.employee_id == employee.id
LEFT JOIN department ON employee.department_id == department.id
```

- Detach to create smaller sub schemas..

```python
>>> employee = schema.detach('employee')
>>> normalize_query(query_str(
...      employee.select([employee.tables["department"].c.id])
... ))
SELECT department.id
FROM employee
LEFT JOIN department ON employee.department_id == department.id
```

- Compose to create larger schemas...

(TODO)

## Why would I want to use this?

You might be wondering why you would want to use this, doesn't
SQLAlchemy follow foreign key relationships anyway to determine the
joins that should be made? The answer is that while SQLAlchemy will
follow foreign key relationships it doesn't magically know how your data
schema is set up, particularly when working with star and snowflake
schemas common in OLAP. For example when the same table is the target of
multiple foreign keys the basic functionality of SQLAlchemy breaks down.
Also since you might have sub-queries or CTEs which you might want to
attach to your schema it's not always possible to use foreign keys.
