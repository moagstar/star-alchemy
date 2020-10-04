"""
Verify that the examples do what they say they do
"""

from tests import util
from examples import sales


def test_select_high_value_sales_outside_us_per_employee():
    util.assert_query_equals(
        sales.queries.select_high_value_non_us_sales_per_employee(),
        """
            SELECT employee.id, count(distinct(sale.id)) AS count_1
            FROM sale
            LEFT OUTER JOIN employee ON employee.id = sale.employee_id
            LEFT OUTER JOIN product ON product.id = sale.product_id
            LEFT OUTER JOIN customer ON customer.id = sale.customer_id
            LEFT OUTER JOIN location AS customer_location ON customer_location.id = customer.location_id
            WHERE product.unit_price > 20
              AND customer_location.country != 'US'
            GROUP BY CUBE(employee.id)
        """
    )
