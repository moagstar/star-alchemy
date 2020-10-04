import sqlalchemy as sa
from examples.sales import cube, schema


def select_high_value_non_us_sales_per_employee():

    return cube.select(
        schema,
        dimensions=[
            schema.employee.c.id,
        ],
        measures=[
            sa.func.count(sa.func.distinct(schema.sale.c.id)),
        ],
        filters=[
            schema.product.c.unit_price > 20,
            schema.customer_location.c.country != 'US',
        ],
    )

