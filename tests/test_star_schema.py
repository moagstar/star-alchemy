from unittest import TestCase

import sqlalchemy as sa

from star_alchemy import _star_schema
from star_alchemy._star_schema import Join
from tests import tables
from tests.util import AssertQueryEqualMixin, DocTestMixin, query_test


def fixture_sale():
    """
    :return: Schema that can be used for testing
    """
    product_info = sa.select(tables.product.c.id, sa.func.count())
    product_info_sub = product_info.alias("product_info_sub")
    product_info_cte = product_info.cte("product_info_cte")
    employee_location = tables.location.alias("employee_location")
    customer_location = tables.location.alias("customer_location")
    location_false_false = tables.location.alias("location_false_false")
    location_true_false = tables.location.alias("location_true_false")
    location_true_true = tables.location.alias("location_true_true")
    location_none_false_false = tables.location.alias("location_none_false_false")
    location_none_true_false = tables.location.alias("location_none_true_false")
    location_none_true_true = tables.location.alias("location_none_true_true")
    dynamic_table = sa.text("select 1 as id").columns(id=sa.INT).cte("dynamic")

    definition = {
        tables.sale: {
            dynamic_table: {},
            tables.product: {
                tables.category: {},
                product_info_sub: {},
                product_info_cte: {},
            },
            tables.employee: {
                tables.department: {},
                employee_location: {},
            },
            tables.customer: {
                customer_location: {},
                location_false_false: {},
                location_true_false: {},
                location_true_true: {},
                location_none_false_false: {},
                location_none_true_false: {},
                location_none_true_true: {},
            },
        },
    }

    joins = {
        (tables.product, product_info_sub): lambda l, r: l.c.id == r.c.id,
        (tables.product, product_info_cte): lambda l, r: l.c.id == r.c.id,
        (tables.employee, employee_location): lambda l, r: l.c.location_id == r.c.id,
        (tables.customer, customer_location): lambda l, r: l.c.location_id == r.c.id,
        (tables.sale, dynamic_table): lambda l, r: l.c.dynamic_id == r.c.id,
        (tables.customer, location_false_false): Join(
            lambda l, r: l.c.location_id == r.c.id, False, False
        ),
        (tables.customer, location_true_false): Join(
            lambda l, r: l.c.location_id == r.c.id, True, False
        ),
        (tables.customer, location_true_true): Join(
            lambda l, r: l.c.location_id == r.c.id, True, True
        ),
        (tables.customer, location_none_false_false): Join(isouter=False, full=False),
        (tables.customer, location_none_true_false): Join(isouter=True, full=False),
        (tables.customer, location_none_true_true): Join(isouter=True, full=True),
    }

    return _star_schema.Schema(definition, joins)


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
        sub_tests = [
            (self.sales.tables.sale, ("sale",)),
            (self.sales.tables.product, ("sale", "product")),
            (self.sales.tables.employee, ("sale", "employee")),
            (self.sales.tables.customer, ("sale", "customer")),
            (self.sales.tables.category, ("sale", "product", "category")),
            (
                self.sales.tables.product_info_sub,
                ("sale", "product", "product_info_sub"),
            ),
            (
                self.sales.tables.product_info_cte,
                ("sale", "product", "product_info_cte"),
            ),
            (self.sales.tables.department, ("sale", "employee", "department")),
            (
                self.sales.tables.employee_location,
                ("sale", "employee", "employee_location"),
            ),
            (
                self.sales.tables.customer_location,
                ("sale", "customer", "customer_location"),
            ),
        ]
        table_to_path = dict(self.sales.table_paths)
        for table, expected in sub_tests:
            with self.subTest(table.name):
                actual = table_to_path[table]
                self.assertEqual(actual, expected)


class StarSchemaQueryTestCase(TestCase, AssertQueryEqualMixin):
    """
    Generate queries from the sale fixture and check that the expected
    SQL is generated.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.sales = fixture_sale()

    @query_test(
        expected="""
        SELECT count(*) AS count_1
    """
    )
    def test_no_table(self):
        return self.sales.select(sa.func.count())

    @query_test(
        expected="""
        SELECT sale.id
        FROM sale
    """
    )
    def test_no_join(self):
        return self.sales.select(self.sales.tables.sale.c.id)

    @query_test(
        expected="""
        SELECT 1 AS anon_1
        FROM sale
        ORDER BY sale.product_id
    """
    )
    def test_no_join_order_by(self):
        return self.sales.select(sa.literal(1)).order_by(
            self.sales.tables.sale.c.product_id
        )

    @query_test(
        expected="""
        SELECT employee.id
        FROM sale
        LEFT OUTER JOIN employee ON sale.employee_id = employee.id
    """
    )
    def test_join_internal_node(self):
        return self.sales.select(self.sales.tables.employee.c.id)

    @query_test(
        expected="""
        SELECT category.id
        FROM sale
        LEFT OUTER JOIN product ON sale.product_id = product.id
        LEFT OUTER JOIN category ON product.category_id = category.id
    """
    )
    def test_join_leaf_node(self):
        return self.sales.select(self.sales.tables.category.c.id)

    @query_test(
        expected="""
        SELECT employee_location.id
        FROM sale
        LEFT OUTER JOIN employee ON sale.employee_id = employee.id
        LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
    """
    )
    def test_join_leaf_node_alias(self):
        return self.sales.select(self.sales.tables.employee_location.c.id)

    @query_test(
        expected="""
        SELECT employee_location.id as employee_location_id, category.id as category_id
        FROM sale
        LEFT OUTER JOIN product ON sale.product_id = product.id
        LEFT OUTER JOIN category ON product.category_id = category.id
        LEFT OUTER JOIN employee ON sale.employee_id = employee.id
        LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
    """
    )
    def test_join_branching(self):
        return self.sales.select(
            self.sales.tables.employee_location.c.id.label("employee_location_id"),
            self.sales.tables.category.c.id.label("category_id"),
        )

    @query_test(
        expected="""
        SELECT employee_location.id, customer_location.id AS id_1
        FROM sale
        LEFT OUTER JOIN employee ON sale.employee_id = employee.id
        LEFT OUTER JOIN location AS employee_location ON employee.location_id = employee_location.id
        LEFT OUTER JOIN customer ON sale.customer_id = customer.id
        LEFT OUTER JOIN location AS customer_location ON customer.location_id = customer_location.id
    """
    )
    def test_branching_join_duplicate_underlying_table(self):
        return self.sales.select(
            self.sales.tables.employee_location.c.id,
            self.sales.tables.customer_location.c.id,
        )

    @query_test(
        expected="""
        SELECT  location_false_false.id AS _1,
                location_true_false.id AS _2,
                location_true_true.id AS _3,
                location_none_false_false.id AS _4,
                location_none_true_false.id AS _5,
                location_none_true_true.id AS _6
        FROM sale
        LEFT OUTER JOIN customer ON sale.customer_id = customer.id
        JOIN location AS location_false_false ON customer.location_id = location_false_false.id
        LEFT OUTER JOIN location AS location_true_false ON customer.location_id = location_true_false.id
        FULL OUTER JOIN location AS location_true_true ON customer.location_id = location_true_true.id
        JOIN location AS location_none_false_false ON customer.location_id = location_none_false_false.id
        LEFT OUTER JOIN location AS location_none_true_false ON customer.location_id = location_none_true_false.id
        FULL OUTER JOIN location AS location_none_true_true ON customer.location_id = location_none_true_true.id
    """
    )
    def test_custom_joins(self):
        return self.sales.select(
            self.sales.tables.location_false_false.c.id.label("_1"),
            self.sales.tables.location_true_false.c.id.label("_2"),
            self.sales.tables.location_true_true.c.id.label("_3"),
            self.sales.tables.location_none_false_false.c.id.label("_4"),
            self.sales.tables.location_none_true_false.c.id.label("_5"),
            self.sales.tables.location_none_true_true.c.id.label("_6"),
        )

    def test_custom_join_with__isouter_false_and_full_true_should_error(self):
        with self.assertRaises(ValueError):
            Join(lambda l, r: l.c.id == r.c.id, isouter=False, full=True)

    @query_test(
        expected="""
        WITH product_info_cte AS (
            SELECT product.id AS id, count(*) as count_1
            FROM product
        )
        SELECT product_info_cte.id AS id
        FROM sale
        LEFT OUTER JOIN product ON sale.product_id = product.id
        LEFT OUTER JOIN product_info_cte ON product.id = product_info_cte.id
    """
    )
    def test_join_cte(self):
        return self.sales.select(
            self.sales.tables.product_info_cte.c.id.label("id"),
        )

    @query_test(
        expected="""
        SELECT product_info_sub.id AS id
        FROM sale
        LEFT OUTER JOIN product ON sale.product_id = product.id
        LEFT OUTER JOIN (
            SELECT product.id AS id, count(*) as count_1
            FROM product
        ) AS product_info_sub ON product.id = product_info_sub.id
    """
    )
    def test_join_sub_select(self):
        return self.sales.select(
            self.sales.tables.product_info_sub.c.id.label("id"),
        )

    @query_test(
        expected="""
        SELECT sale.id
        FROM sale
        UNION
        SELECT customer.id
        FROM sale
        LEFT OUTER JOIN customer ON sale.customer_id = customer.id
    """
    )
    def test_union(self):
        return sa.union(
            self.sales.select(self.sales.tables.sale.c.id),
            self.sales.select(self.sales.tables.customer.c.id),
        )

    @query_test(
        expected="""
        SELECT department.id
        FROM department
    """
    )
    def test_select_from_override(self):
        return self.sales.select(self.sales.tables.department.c.id).select_from(
            self.sales.tables.department
        )

    @query_test(
        expected="""
            WITH dynamic AS (
                SELECT 1 AS id
            )
            SELECT dynamic.id
            FROM sale
            LEFT OUTER JOIN dynamic ON sale.dynamic_id = dynamic.id
    """
    )
    def test_select_from_dynamic(self):
        return self.sales.select(self.sales.tables.dynamic.c.id)

    def test_to_str(self):
        actual = str(self.sales) + "\n"
        expected = (
            "  └─ sale\n"
            "    └─ dynamic\n"
            "    └─ product\n"
            "      └─ category\n"
            "      └─ product_info_sub\n"
            "      └─ product_info_cte\n"
            "    └─ employee\n"
            "      └─ department\n"
            "      └─ employee_location\n"
            "    └─ customer\n"
            "      └─ customer_location\n"
            "      └─ location_false_false\n"
            "      └─ location_true_false\n"
            "      └─ location_true_true\n"
            "      └─ location_none_false_false\n"
            "      └─ location_none_true_false\n"
            "      └─ location_none_true_true\n"
        )
        self.assertEqual(actual, expected)


class DocStringTestCase(TestCase, DocTestMixin(_star_schema)):
    """verify docstring examples"""
