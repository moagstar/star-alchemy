import pytest

from star_alchemy._star_alchemy import StarSchema
from examples.sales import tables
from tests import tables
from tests.util import assert_query_equals, query_str


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
    assert_query_equals(
        query_str(
            sales.select([sales.tables['sale'].c.id]),
        ),
        """
            SELECT sale.id 
            FROM sale
        """
    )


def test_single_join(sales):
    assert_query_equals(
        query_str(
            sales.select([sales.tables['employee'].c.id])
        ),
        """
            SELECT employee.id 
            FROM sale 
            LEFT OUTER JOIN employee ON sale.employee_id = employee.id
        """
    )


def test_multi_join(sales):
    assert_query_equals(
        query_str(
            sales.select([sales.tables['employee_location'].c.id]),
        ),
        """
            SELECT employee_location.id 
            FROM sale 
            LEFT OUTER JOIN employee ON sale.employee_id = employee.id 
            LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
        """
    )


def test_branching_join(sales):
    assert_query_equals(
        query_str(
            sales.select([
                sales.tables['employee_location'].c.id,
                sales.tables['category'].c.id,
            ])
        ),
        """
            SELECT employee_location.id, category.id 
            FROM sale 
            LEFT OUTER JOIN employee ON sale.employee_id = employee.id
            LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
            LEFT OUTER JOIN product ON sale.product_id = product.id
            LEFT OUTER JOIN category ON product.category_id = category.id
        """
    )


def test_branching_join_duplicate_underlying_table(sales):
    assert_query_equals(
        query_str(
            sales.select([
                sales.tables['employee_location'].c.id,
                sales.tables['customer_location'].c.id,
            ])
        ),
        """
            SELECT employee_location.id, customer_location.id 
            FROM sale 
            LEFT OUTER JOIN employee ON sale.employee_id = employee.id 
            LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id 
            LEFT OUTER JOIN customer ON sale.customer_id = customer.id
            LEFT OUTER JOIN location AS customer_location ON customer.location_id = customer_location.id
        """
    )
