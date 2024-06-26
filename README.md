# star-alchemy

Extends SQLAlchemy to make it easy to deal with star, snowflake and
galaxy schemas.

## Features

- Define topology using nested dictionaries

```python
>>> from star_alchemy import Schema
>>> from tests import tables
>>> schema = Schema({
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
>>> from tests.util import assert_query_equal
>>> assert_query_equal(
...      schema.select(schema.tables.department.c.id),
...      """
...        SELECT department.id
...        FROM sale
...        LEFT OUTER JOIN employee ON sale.employee_id = employee.id
...        LEFT OUTER JOIN department ON employee.department_id = department.id
...      """
... )
```

- Customise the joins if necessary:

```python
>>> from star_alchemy import Schema, Join
>>> from tests import tables
>>> definition = {
...     tables.sale: {
...         tables.product: {},
...     },
... }
>>> joins = {
...     (tables.sale, tables.product): Join(isouter=False)
... }
>>> schema = Schema(definition, joins)
```

- Detach to create smaller sub schemas..

(TODO)

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

TODO: standard schema diagram, explain difference

![star schema](doc/sales_schema.png "example star schema")

## Dev-guide

* Add pypi api token (https://pypi.org/help/#apitoken)...

```
poetry config pypi-token.pypi my-token
```

* To prepare a release (run inside virtual env, since tests will be run)...

```
./tools/release
```

Then create a PR for this version, and merge to main

* To publish a release (run inside virtual env, since tests will be run)...

```
./tools/publish
```
