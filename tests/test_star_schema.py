from unittest import TestCase
import sqlalchemy as sa

from star_alchemy._star_alchemy import StarSchema, Join
from examples.sales import tables
from tests import tables
from tests.util import normalize_query


def fixture_sale():
    """
    :return: StarSchema that can be used for testing
    """
    product_info = sa.select([tables.product.c.id, sa.func.count()])
    product_info_sub = product_info.alias('product_info_sub')
    product_info_cte = product_info.cte('product_info_cte')

    return StarSchema.from_dicts({
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


class StarSchemaUnitTestCase(TestCase):
    """
    Unit style tests for verifying fundamental low level functionality
    of the StarSchema class.
    """
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.sales = fixture_sale()

    def test_path(self):
        star_schemas = {s.join.table.name: s for s in self.sales}
        sub_tests = [
            ("sale", "sale"),
            ("product", "sale/product"),
            ("employee", "sale/employee"),
            ("customer", "sale/customer"),
            ("category", "sale/product/category"),
            ("product_info_sub", "sale/product/product_info_sub"),
            ("product_info_cte", "sale/product/product_info_cte"),
            ("department", "sale/employee/department"),
            ("employee_location", "sale/employee/employee_location"),
            ("customer_location", "sale/customer/customer_location"),
        ]
        for table, expected in sub_tests:
            with self.subTest(table):
                actual = "/".join(x.join.table.name for x in star_schemas[table].path)
                self.assertEqual(actual, expected)


class StarSchemaQueryTestCase(TestCase):
    """
    Generate queries from the sale fixture and check that the expected
    SQL is generated.
    """
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.sales = fixture_sale()

    def test_no_table(self):
        self.assertEqual(
            normalize_query("""
                SELECT count(*) AS count_1 
                FROM sale
            """),
            normalize_query(
                self.sales.select([sa.func.count()])
            ),
        )

    def test_no_join(self):
        self.assertEqual(
            normalize_query("""
                SELECT sale.id 
                FROM sale
            """),
            normalize_query(
                self.sales.select([self.sales.tables['sale'].c.id]),
            ),
        )

    def test_join_internal_node(self):
        self.assertEqual(
            normalize_query("""
                SELECT employee.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id
            """),
            normalize_query(
                self.sales.select([self.sales.tables['employee'].c.id])
            ),
        )

    def test_join_leaf_node(self):
        self.assertEqual(
            normalize_query("""
                SELECT category.id 
                FROM sale 
                LEFT OUTER JOIN product ON sale.product_id = product.id 
                LEFT OUTER JOIN category ON product.category_id = category.id
            """),
            normalize_query(
                self.sales.select([self.sales.tables['category'].c.id]),
            ),
        )

    def test_join_leaf_node_alias(self):
        self.assertEqual(
            normalize_query("""
                SELECT employee_location.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id 
                LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
            """),
            normalize_query(
                self.sales.select([self.sales.tables['employee_location'].c.id]),
            ),
        )

    def test_join_branching(self):
        self.assertEqual(
            normalize_query("""
                SELECT employee_location.id, category.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id
                LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
                LEFT OUTER JOIN product ON sale.product_id = product.id
                LEFT OUTER JOIN category ON product.category_id = category.id
            """),
            normalize_query(
                self.sales.select([
                    self.sales.tables['employee_location'].c.id,
                    self.sales.tables['category'].c.id,
                ])
            ),
        )

    def test_branching_join_duplicate_underlying_table(self):
        self.assertEqual(
            normalize_query("""
                SELECT employee_location.id, customer_location.id 
                FROM sale 
                LEFT OUTER JOIN employee ON sale.employee_id = employee.id 
                LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id 
                LEFT OUTER JOIN customer ON sale.customer_id = customer.id
                LEFT OUTER JOIN location AS customer_location ON customer.location_id = customer_location.id
            """),
            normalize_query(
                self.sales.select([
                    self.sales.tables['employee_location'].c.id,
                    self.sales.tables['customer_location'].c.id,
                ])
            ),
        )

    def test_join_cte(self):
        self.assertEqual(
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
            normalize_query(
                self.sales.select([
                    self.sales.tables['product_info_cte'].c.id.label('id'),
                ])
            ),
        )

    def test_join_sub_select(self):
        self.assertEqual(
            normalize_query("""
                SELECT product_info_sub.id AS id
                FROM sale 
                LEFT OUTER JOIN product ON sale.product_id = product.id
                LEFT OUTER JOIN (
                    SELECT product.id AS id, count(*) as count_1
                    FROM product
                ) AS product_info_sub ON product.id = product_info_sub.id
            """),
            normalize_query(
                self.sales.select([
                    self.sales.tables['product_info_sub'].c.id.label('id'),
                ])
            ),
        )

    def test_union(self):
        self.assertEqual(
            normalize_query("""
                SELECT sale.id 
                FROM sale
                UNION
                SELECT customer.id
                FROM sale
                LEFT OUTER JOIN customer ON sale.customer_id = customer.id
            """),
            normalize_query(
                sa.union(
                    self.sales.select([self.sales.tables['sale'].c.id]),
                    self.sales.select([self.sales.tables['customer'].c.id]),
                )
            ),
        )

    def test_detach(self):
        product = self.sales['product']

        self.assertEqual(
            normalize_query("""
                SELECT category.id 
                FROM product
                LEFT OUTER JOIN category ON product.category_id = category.id
            """),
            normalize_query(
                product.select([product.tables['category'].c.id]),
            ),
        )
