import sqlalchemy as sa
import pytest
from sqlparse import format
from galaxy_schema import GalaxySchema, Join


@pytest.fixture()
def fixture_galaxy_schema():
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

    return GalaxySchema(tb1, children=[
        GalaxySchema(cte, join=Join(lambda tb1, cte: tb1.id == cte.id)),
        GalaxySchema(cte.alias('cte_alias'), join=Join(lambda cte, cte_alias: cte.id == cte_alias.id)),
        GalaxySchema(tb2, children=[
            GalaxySchema(tb3, children=[
                GalaxySchema(tb4.alias('tb5'), join=Join(lambda tb3, tb5: tb3.id == tb5.id)),
            ]),
            GalaxySchema(tb4, join=Join(lambda tb3, tb4: tb3.id == tb4.id)),
        ]),
        GalaxySchema(tb6, join=Join(lambda tb1, tb6: tb1.id == tb6.id, isouter=False)),
    ])


def query_str(query):
    """

    :param query:

    :return:
    """
    from sqlalchemy.dialects import postgresql
    return str(query.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True},
    ))


def assert_query(actual, expected):
    actual_formatted_str = format(query_str(actual).strip(), reindent=True)
    expected_formatted_str = format(expected.strip(), reindent=True)
    if actual_formatted_str.lower() != expected_formatted_str.lower():
        assert actual_formatted_str == expected_formatted_str


def test_prevent_explicit_call_to_select_from(fixture_galaxy_schema):
    with pytest.raises(Exception):
        fixture_galaxy_schema.select([]).select_from(fixture_galaxy_schema.tb1)


def test_no_joins(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.tb1.c)
        ),
        expected="""
            SELECT tb1.id, tb1.tb2_id FROM tb1
        """,
    )


def test_no_table(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select([sa.func.count()])
        ),
        expected="""
            SELECT count(*) AS count_1 
            FROM tb1
        """,
    )


def test_join_internal_node(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.tb2.c)
        ),
        expected="""
            SELECT tb2.id, tb2.tb3_id 
            FROM tb1 
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
        """
    )


def test_join_cte(fixture_galaxy_schema):

    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.cte.c)
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


def test_join_aliased_cte(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.cte_alias.c)
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


def test_join_leaf_node(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.tb5.c)
        ),
        expected="""
            SELECT tb5.id
            FROM tb1
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
            LEFT OUTER JOIN tb3 ON tb3.id = tb2.tb3_id
            LEFT OUTER JOIN tb4 AS tb5 ON tb3.id = tb5.id
        """
    )


def test_custom_join(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.tb4.c)
        ),
        expected="""
            SELECT tb4.id
            FROM tb1
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
            LEFT OUTER JOIN tb4 ON tb2.id = tb4.id
        """
    )


def test_inner_join(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select(fixture_galaxy_schema.tb6.c)
        ),
        expected="""
            SELECT tb6.id
            FROM tb1
            JOIN tb6 ON tb1.id = tb6.id
        """
    )


def test_simple_union(fixture_galaxy_schema):
    assert_query(
        actual=(
            sa.union(
                fixture_galaxy_schema.select([fixture_galaxy_schema.tb1.c.id]),
                fixture_galaxy_schema.select([fixture_galaxy_schema.tb1.c.id]),
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


def test_simple_union_with_complex_joins(fixture_galaxy_schema):
    assert_query(
        actual=(
            sa.union(
                fixture_galaxy_schema.select([fixture_galaxy_schema.tb1.c.id]),
                fixture_galaxy_schema.select([fixture_galaxy_schema.cte_alias.c.id])
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


def test_subquery(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select([fixture_galaxy_schema.tb1.c.id])
            .where(fixture_galaxy_schema.tb1.c.id.in_(fixture_galaxy_schema.select([fixture_galaxy_schema.cte_alias.c.id])))
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


def test_order_by(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select([fixture_galaxy_schema.tb1.c.id]).order_by(fixture_galaxy_schema.tb1.c.id)
        ),
        expected="""
            SELECT tb1.id FROM tb1 ORDER BY tb1.id
        """
    )


def test_order_by_joined(fixture_galaxy_schema):
    assert_query(
        actual=(
            fixture_galaxy_schema.select([fixture_galaxy_schema.tb1.c.id]).order_by(fixture_galaxy_schema.tb2.c.id)
        ),
        expected="""
            SELECT tb1.id 
            FROM tb1 
            LEFT OUTER JOIN tb2 ON tb2.id = tb1.tb2_id
            ORDER BY tb2.id
        """
    )