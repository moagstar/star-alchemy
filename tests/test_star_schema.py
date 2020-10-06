import pytest
from star_alchemy._star_alchemy import StarSchema
from examples.sales import tables
from tests import tables
from tests.util import assert_query_equals


@pytest.fixture()
def sales():
    return StarSchema.from_dicts({
        tables.sale: {
            tables.product: {
                tables.category: {},
            },
            tables.employee: {
                tables.department: {},
                tables.location.alias('employee_location'): {},
            },
            tables.customer: {
                tables.location.alias('customer_location'): {},
            },
        },
    })


def test_simple_query(sales):
    query = sales.select([sales.tables['sale'].c.id])
    assert_query_equals(
        query,
        """
            SELECT sale.id 
            FROM sale
        """
    )


def test_single_join(sales):
    assert_query_equals(
        sales.select([sales.tables['employee'].c.id]),
        """
            SELECT employee.id 
            FROM sale 
            LEFT OUTER JOIN employee ON employee.id = sale.employee_id
        """
    )