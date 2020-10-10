from unittest import TestCase
import sqlalchemy as sa

from star_alchemy._star_alchemy import StarSchema, Join
from examples.sales import tables
from tests import tables
from tests.util import normalize_query


class StarSchemaTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        product_info = sa.select([tables.product.c.id, sa.func.count()])
        product_info_sub = product_info.alias('product_info_sub')
        product_info_cte = product_info.cte('product_info_cte')

        cls.sales = StarSchema.from_dicts({
            tables.sale: {
                tables.product: {
                    tables.category: {},
                    Join(product_info_sub, lambda l, r: l.c.id == r.c.id): {},
                    Join(product_info_cte, lambda l, r: l.c.id == r.c.id): {},
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

    def test_no_table(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([sa.func.count()])
            ),
            normalize_query("""
                SELECT count(*) AS count_1 
                FROM sale
            """),
        )

    def test_no_join(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([self.sales.tables['sale'].c.id]),
            ),
            normalize_query("""
                SELECT sale.id 
                FROM sale
            """),
        )

    def test_join_internal_node(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([self.sales.tables['employee'].c.id])
            ),
            normalize_query("""
                SELECT employee.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id
            """),
        )

    def test_join_leaf_node(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([self.sales.tables['category'].c.id]),
            ),
            normalize_query("""
                SELECT category.id 
                FROM sale 
                LEFT OUTER JOIN product ON sale.product_id = product.id 
                LEFT OUTER JOIN category ON product.category_id = category.id
            """),
        )

    def test_join_leaf_node_alias(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([self.sales.tables['employee_location'].c.id]),
            ),
            normalize_query("""
                SELECT employee_location.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id 
                LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
            """),
        )

    def test_join_branching(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([
                    self.sales.tables['employee_location'].c.id,
                    self.sales.tables['category'].c.id,
                ])
            ),
            normalize_query("""
                SELECT employee_location.id, category.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id
                LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
                LEFT OUTER JOIN product ON sale.product_id = product.id
                LEFT OUTER JOIN category ON product.category_id = category.id
            """),
        )

    def test_branching_join_duplicate_underlying_table(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([
                    self.sales.tables['employee_location'].c.id,
                    self.sales.tables['customer_location'].c.id,
                ])
            ),
            normalize_query("""
                SELECT employee_location.id, customer_location.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id 
                LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id 
                LEFT OUTER JOIN customer ON sale.customer_id = customer.id
                LEFT OUTER JOIN location AS customer_location ON customer.location_id = customer_location.id
            """),
        )

    def test_join_cte(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([
                    self.sales.tables['product_info_cte'].c.id.label('id'),
                ])
            ),
            normalize_query("""
                WITH product_info_cte AS (
                    SELECT product.id AS id, count(*) as count_1
                    FROM product
                )
                SELECT product_info_cte.id AS id
                FROM sale 
                LEFT OUTER JOIN product ON sale.product_id = product.id
                LEFT OUTER JOIN product_info_cte ON product.id = product_info_cte.id
            """),
        )

    def test_join_sub_select(self):
        self.assertEqual(
            normalize_query(
                self.sales.select([
                    self.sales.tables['product_info_sub'].c.id.label('id'),
                ])
            ),
            normalize_query("""
                SELECT product_info_sub.id AS id
                FROM sale 
                LEFT OUTER JOIN product ON sale.product_id = product.id
                LEFT OUTER JOIN (
                    SELECT product.id AS id, count(*) as count_1
                    FROM product
                ) AS product_info_sub ON product.id = product_info_sub.id
            """),
        )

    def test_union(self):
        self.assertEqual(
            normalize_query(
                sa.union(
                    self.sales.select([self.sales.tables['sale'].c.id]),
                    self.sales.select([self.sales.tables['customer'].c.id]),
                )
            ),
            normalize_query("""
                SELECT sale.id 
                FROM sale
                UNION
                SELECT customer.id
                FROM sale
                LEFT OUTER JOIN customer ON sale.customer_id = customer.id
            """),
        )