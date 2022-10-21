import dataclasses
from functools import cached_property, partial

from sqlalchemy import Column
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import Select, visitors, Selectable, FromClause, ClauseElement
from toolz import sliding_window, first, unique


@dataclasses.dataclass(frozen=True)
class Schema:
    """
    Defines a star-alchemy schema. This defines how tables are attached
    together and is used to automatically detect the joins that are
    needed for a particular query. There are 3 main parts to the API...

    1. Generation using nested dictionaries to describe the topology:

    >>> from examples.sales import tables
    >>> from star_alchemy import Schema
    >>> schema = Schema({
    ...     tables.sale: {
    ...         tables.product: {}
    ...     }
    ... })

    2. The tables can be inspected `Schema.tables`
    3. Queries can be generated using `Schema.select`
    """

    definition: dict
    on_clauses: dict = dataclasses.field(default_factory=dict)

    @cached_property
    def tables(self):
        """
        Get a frozen dataclass containing all the tables in this schema.
        The tables can then be queried as attributes using their name
        (or alias)...

        >>> from examples.sales.schema import schema
        >>> schema.tables.product.name
        'product'
        """
        tables = {path[-1]: table for table, path in self.table_paths}
        return dataclasses.make_dataclass("Tables", tables, frozen=True)(**tables)

    def select(self, *args, **kwargs) -> "_Select":
        """
        Generate a SQLAlchemy query. Works just like any other
        SQLALchemy query. In addition, a custom compilation step to
        automatically generate the `select_from` part of the query
        based on the used expressions and the defined schema.

        >>> from examples.sales.schema import schema
        >>> from tests.util import assert_query_equal
        >>> assert_query_equal(
        ...      schema.select([schema.tables.product.c.id]),
        ...      '''
        ...        SELECT product.id
        ...        FROM sale
        ...        LEFT OUTER JOIN product ON sale.product_id = product.id
        ...      '''
        ... )
        """
        return self._Select(self, *args, **kwargs)

    @cached_property
    def table_paths(self) -> list[tuple[str]]:
        """
        Get a list of tuples containing the paths to the root of the schema,
        or in other words a description of how a table should be joined.
        """

        def _paths(obj, path=()):
            for table, value in (obj if isinstance(obj, dict) else {}).items():
                yield table, (table_path := tuple([*path, table.name]))
                yield from _paths(value, table_path)

        return list(_paths(self.definition))

    def on_clause(self, left: Selectable, right: Selectable) -> ClauseElement:
        """
        Generate the on_clause for joining two tables. If there is a
        simple foreign key relationship between the tables then it is
        possible to automatically generate this clause, otherwise an
        explicit join needs to be specified in the constructor, for
        example...

        :param left: Left selectable.
        :param right: Right selectable.

        :return: Expression which is used to join the two tables.
        """
        on_clause_func = self.on_clauses.get((left, right), self._default_on_clause)
        on_clause = on_clause_func(left, right)
        return on_clause

    class _Select(Select):
        """
        Special sqlalchemy select that stores the `Schema` which
        generated it. This can then be used to automatically generate
        the `select_from` for the query.
        """

        def __init__(self, schema: "Schema", *args, **kwargs):
            self._schema = schema
            super().__init__(*args, **kwargs)

    @staticmethod
    def _default_on_clause(left: Selectable, right: Selectable) -> ClauseElement:
        """
        Generate an on clause using foreign key relations to determine
        how the join should be made. (see on_clause).
        """
        error_msg = f"explicit on_clause required for {(left, right)}"
        assert isinstance(left, FromClause) and isinstance(right, FromClause), error_msg

        if len(column_list := list(right.primary_key)) != 1:
            raise ValueError(error_msg)

        for foreign_key in left.foreign_keys:
            if first(column_list[0].base_columns) is foreign_key.column:
                return foreign_key.parent == column_list[0]

        raise ValueError(error_msg)

    def __str__(self):
        return "\n".join(
            f"{'  ' * len(path)}└─ {path[-1]}" for _, path in self.table_paths
        )


@compiles(Schema._Select)
def _compile_schema_select(select: Schema._Select, compiler, **kw):
    """
    Compile a special SQLAlchemy Schema._Select object, automatically
    detecting the joins that are required based on the expressions
    in the query and the schema which defines how tables should
    be joined.
    """
    # get the columns (and thus the tables) from the sub-expressions involved in this query
    tables = {x.table for x in visitors.iterate(select, {}) if isinstance(x, Column)}

    # TODO: Perhaps only join to lowest common root rather than root?
    # use paths to generate the joins based on the tables present in this query
    joins = unique(
        join
        for table, path in select._schema.table_paths
        for join in sliding_window(2, path)
        if table in tables
    )

    select_from = None
    for join in joins:
        get_table = partial(getattr, select._schema.tables)
        left_table, right_table = map(get_table, join)
        on_clause = select._schema.on_clause(left_table, right_table)
        select_from = left_table if select_from is None else select_from
        select_from = select_from.join(right_table, on_clause, isouter=True)

    # Add the select_from to the select and let sqlalchemy do the rest
    if select_from is not None:
        select = super(Schema._Select, select).select_from(select_from)

    compiled = compiler.process(super(Schema._Select, select), **kw)
    return compiled