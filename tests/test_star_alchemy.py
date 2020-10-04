import sqlalchemy as sa
import pytest
from sqlparse import format
from star_alchemy import StarSchema, Join
from tests.util import assert_query_equals


@pytest.fixture()
def fixture_star_schema():
    meta = sa.MetaData()

    tb1 = sa.Table('tb1', meta,
       sa.Column('id'),
       sa.Column('tb2_id', sa.Integer, sa.ForeignKey("tb2.id")),
    )
    tb2 = sa.Table('tb2', meta,
       sa.Column('id'),
       sa.Column('tb3_id', sa.Integer, sa.ForeignKey("tb3.id")),
    )
    tb3 = sa.Table(
        'tb3', meta,
        sa.Column('id'),
    )
    tb4 = sa.Table(
        'tb4', meta,
        sa.Column('id'),
    )
    tb6 = sa.Table(
        'tb6', meta,
        sa.Column('id')
    )

    cte = sa.select(tb1.c).cte('cte')

    return StarSchema(tb1, children=[
        StarSchema(cte, join=Join(lambda tb1, cte: tb1.id == cte.id)),
        StarSchema(cte.alias('cte_alias'), join=Join(lambda cte, cte_alias: cte.id == cte_alias.id)),
        StarSchema(tb2, children=[
            StarSchema(tb3, children=[
                StarSchema(tb4.alias('tb5'), join=Join(lambda tb3, tb5: tb3.id == tb5.id)),
            ]),
            StarSchema(tb4, join=Join(lambda tb3, tb4: tb3.id == tb4.id)),
        ]),
        StarSchema(tb6, join=Join(lambda tb1, tb6: tb1.id == tb6.id, isouter=False)),
    ])


def test_prevent_explicit_call_to_select_from(fixture_star_schema):
    with pytest.raises(Exception):
        fixture_star_schema.select([]).select_from(fixture_star_schema.tb1)


def test_no_joins(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.tb1.c)
        ),
        expected="""
            SELECT tb1.id, tb1.tb2_id FROM tb1
        """,
    )


def test_no_table(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select([sa.func.count()])
        ),
        expected="""
            SELECT count(*) AS count_1 
            FROM tb1
        """,
    )


def test_join_internal_node(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.tb2.c)
        ),
        expected="""
            SELECT tb2.id, tb2.tb3_id 
            FROM tb1 
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
        """
    )


def test_join_cte(fixture_star_schema):

    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.cte.c)
        ),
        expected="""
            WITH cte AS (
                SELECT tb1.id AS id,
                       tb1.tb2_id AS tb2_id
                FROM tb1
            )
            SELECT cte.id, cte.tb2_id
            FROM tb1 
            LEFT OUTER JOIN cte ON tb1.id = cte.id
        """,
    )


def test_join_aliased_cte(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.cte_alias.c)
        ),
        expected="""
            WITH cte AS (
                SELECT tb1.id AS id,
                       tb1.tb2_id AS tb2_id
                FROM tb1
            )
            SELECT cte_alias.id, cte_alias.tb2_id
            FROM tb1 
            LEFT OUTER JOIN cte AS cte_alias ON tb1.id = cte_alias.id
        """,
    )


def test_join_leaf_node(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.tb5.c)
        ),
        expected="""
            SELECT tb5.id
            FROM tb1
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
            LEFT OUTER JOIN tb3 ON tb3.id = tb2.tb3_id
            LEFT OUTER JOIN tb4 AS tb5 ON tb3.id = tb5.id
        """
    )


def test_custom_join(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.tb4.c)
        ),
        expected="""
            SELECT tb4.id
            FROM tb1
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
            LEFT OUTER JOIN tb4 ON tb2.id = tb4.id
        """
    )


def test_inner_join(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select(fixture_star_schema.tb6.c)
        ),
        expected="""
            SELECT tb6.id
            FROM tb1
            JOIN tb6 ON tb1.id = tb6.id
        """
    )


def test_simple_union(fixture_star_schema):
    assert_query_equals(
        actual=(
            sa.union(
                fixture_star_schema.select([fixture_star_schema.tb1.c.id]),
                fixture_star_schema.select([fixture_star_schema.tb1.c.id]),
            )
        ),
        expected="""
            SELECT tb1.id
            FROM tb1
            UNION
            SELECT tb1.id
            FROM tb1
        """
    )


def test_simple_union_with_complex_joins(fixture_star_schema):
    assert_query_equals(
        actual=(
            sa.union(
                fixture_star_schema.select([fixture_star_schema.tb1.c.id]),
                fixture_star_schema.select([fixture_star_schema.cte_alias.c.id])
            )
        ),
        expected="""
            WITH cte AS (
                SELECT tb1.id AS id,
                       tb1.tb2_id AS tb2_id
                FROM tb1
            )
            SELECT tb1.id
            FROM tb1
            UNION
            SELECT cte_alias.id
            FROM tb1 
            LEFT OUTER JOIN cte AS cte_alias ON tb1.id = cte_alias.id
        """
    )


def test_subquery(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select([fixture_star_schema.tb1.c.id])
            .where(fixture_star_schema.tb1.c.id.in_(fixture_star_schema.select([fixture_star_schema.cte_alias.c.id])))
        ),
        expected="""
            WITH cte AS (
                SELECT tb1.id AS id,
                       tb1.tb2_id AS tb2_id
                FROM tb1
            )
            SELECT tb1.id
            FROM   tb1
            WHERE  tb1.id IN (
                SELECT cte_alias.id
                FROM tb1
                LEFT OUTER JOIN cte AS cte_alias ON tb1.id = cte_alias.id
            )
        """
    )


def test_order_by(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select([fixture_star_schema.tb1.c.id]).order_by(fixture_star_schema.tb1.c.id)
        ),
        expected="""
            SELECT tb1.id FROM tb1 ORDER BY tb1.id
        """
    )


def test_order_by_joined(fixture_star_schema):
    assert_query_equals(
        actual=(
            fixture_star_schema.select([fixture_star_schema.tb1.c.id]).order_by(fixture_star_schema.tb2.c.id)
        ),
        expected="""
            SELECT tb1.id 
            FROM tb1 
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
            ORDER BY tb2.id
        """
    )


def test_cte_from_star_select(fixture_star_schema):
    cte = fixture_star_schema.select([fixture_star_schema.tb1.c.id]).cte('cte')
    assert_query_equals(
        actual=sa.select([cte.c.id]),
        expected="""
            WITH cte AS (SELECT tb1.id AS id FROM tb1) SELECT cte.id FROM cte
        """,
    )