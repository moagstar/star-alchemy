"""
Verify that the examples do what they say they do
"""
from unittest import TestCase

from examples import sales
from tests.util import AssertQueryEqualMixin, normalize_query, query_test


class ExampleTestCase(TestCase, AssertQueryEqualMixin):

    @query_test(expected="""
        SELECT employee.id, count(distinct(sale.id)) AS count_1
        FROM sale
        LEFT OUTER JOIN employee ON sale.employee_id = employee.id
        LEFT OUTER JOIN product ON sale.product_id = product.id
        LEFT OUTER JOIN customer ON sale.customer_id = customer.id
        LEFT OUTER JOIN location AS customer_location ON customer.location_id = customer_location.id
        WHERE product.unit_price > 20
          AND customer_location.country != 'US'
        GROUP BY CUBE(employee.id)
    """)
    def test_select_high_value_sales_outside_us_per_employee(self):
        return sales.queries.select_high_value_non_us_sales_per_employee()
