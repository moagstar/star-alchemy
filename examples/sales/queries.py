import sqlalchemy as sa
from examples.sales import cube, schema


def select_high_value_non_us_sales_per_employee():

    return cube.select(
        schema,
        dimensions=[
            schema.tables['employee'].c.id,
        ],
        measures=[
            sa.func.count(sa.func.distinct(schema.tables['sale'].c.id)),
        ],
        filters=[
            schema.tables['product'].c.unit_price > 20,
            schema.tables['customer_location'].c.country != 'US',
        ],
    )
