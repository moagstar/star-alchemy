# std
import typing
from itertools import chain
# 3rd party
import sqlalchemy as sa
from funcoperators import mapwith, to
from sqlalchemy.ext.compiler import compiles
from toolz import unique


class Join(typing.NamedTuple):
    """
    Describes how two sqlalchemy tables are joined together, these
    values are used as arguments to the sqlalchemy join function.
    """
    onclause: typing.Union[typing.Callable, None]
    isouter: bool
    full: bool


# in compile_star_schema_select we will use the sqlalchemy join
# function to join tables, so the values here mimic the default values
# required by that function.
Join.__new__.__defaults__ = (lambda *_: None, True, False)


SqlAlchemyTable = typing.Union[sa.sql.expression.Alias, sa.sql.expression.TableClause]


class StarSchema:
    """
    Recursive container of SqlAlchemy selectables which models a
    star schema. The schema is a tree, where the nodes in the
    tree are SQLAlchemy tables and the edges describe the way these
    tables should be joined. By modelling the data schema in this
    fashion we can construct queries using only expressions, and
    letting the topology of the tree determine the appropriate joins.
    """
    StarSchemas = typing.Iterable['StarSchema']

    def __init__(
        self,
        center: SqlAlchemyTable,
        children: typing.Optional[StarSchemas] = None,
        *,
        join: typing.Optional[Join] = None,
    ):
        """
        :param center:   The center table of this topology, when
                         auto-generating the select_from for queries
                         joins will try to be made to this table.
        :param children: The children on this topology.
        :param join:     The manner in which this center table is
                         joined to it's parent.
        """
        self.center = center
        self.join = Join() if join is None else join
        self._children = children or []

        self._stars_schemas = {}
        self.parent = None

        def init_tree(star_schema: StarSchema):
            if star_schema.center.name in self._stars_schemas:
                raise ValueError(
                    f'{star_schema.center.name} already exists in'
                    f' this star schema'
                )
            self._stars_schemas[star_schema.center.name] = star_schema
            for child_star_schema in star_schema._children:
                child_star_schema.parent = star_schema
                init_tree(child_star_schema)

        init_tree(self)

    @classmethod
    def from_dicts(cls, dicts):
        """
        Create a star schema from recursive dictionaries, the key of
        each dictionary is the center table, the value being the child
        schemas:

        >>> meta = sa.MetaData()
        >>> sale = sa.Table('sale', meta,
        ...     sa.Column('id'),
        ...     sa.Column('customer_id', sa.Integer, sa.ForeignKey("customer.id")),
        ...     sa.Column('product_id', sa.Integer, sa.ForeignKey("product.id")),
        ... )
        >>> product = sa.Table('product', meta, sa.Column('id'))
        >>> customer = sa.Table('customer', meta, sa.Column('id'))
        >>> StarSchema.from_dicts({
        ...     sale: {
        ...         product: {},
        ...         customer: {},
        ...     }
        ... })

        This is offers some nice syntactic sugar for defining a schema,
        however we cannot customize the joins, and so this method only
        works with LEFT OUTER joins where there is a foreign key
        between the left and right tables.
        TODO: Design a nice way to remove this constraint

        :param dicts: Recursive dicts describing this schema

        :return: StarSchema instance created from ``dicts``.
        """
        if len(dicts) > 1:
            raise ValueError("Star schema should have 1 root node")

        def _make(d):
            return [StarSchema(c, children=_make(k)) for c, k in d.items()]

        return _make(dicts)[0]

    def select(self, *args, **kwargs):
        """
        Construct a SQLAlchemy select statement from the star schema.
        The main difference between this and ``sqlalchemy.select`` is
        that we can automatically determine which joins are needed based
        on the defined schema and the requested expressions.

        :param args: Position arguments (see sqlalchemy.select)
        :param kwargs: Keyword arguments (see sqlalchemy.select)

        :return: SQLAlchemy select statement.
        """
        return StarSchemaSelect(self, *args, **kwargs)

    def __getattr__(self, table_name: str):
        """
        Get a table that is part of this star schema.

        :param table_name: Name of the table to retrieve.

        :return: SQLAlchemy selectable.
        """
        return self._stars_schemas[table_name].center

    def __repr__(self):
        """
        :return: string representation of this schema
        """
        return self.center.name

    def path_to_center(self, table: SqlAlchemyTable) -> \
        typing.Iterable[typing.Tuple[SqlAlchemyTable, 'StarSchema']]:
        """
        Get the path from a particular table in this schema to the
        center of the schema.

        :param table: The table to get the path for.

        :return: Iterable of tuples where each tuple are the left and
                 right sides of a SQL join. The left side is a
                 SQLAlchemy table and the right side is a StarSchema
                 which describes how it should be joined to it's parent
                 StarSchema.
        """
        if table.name in self._stars_schemas:
            star_schema = self._stars_schemas[table.name]
            path = []
            while star_schema.parent != None:
                path.append((star_schema.parent, star_schema))
                star_schema = star_schema.parent
            return reversed(path)
        else:
            return []


class StarSchemaSelect(sa.sql.expression.Select):
    """
    Custom SQLAlchemy expression for automatically generating the
    select_from for a query based on the expressions that are present
    in the query. In addition to the actual query (defined by *args
    and **kwargs, we store the star that contains the tables for
    generating the appropriate select_from based on the expressions
    in the query).
    """
    def __init__(self, star_schema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.star_schema = star_schema

    def select_from(self, fromclause):
        """
        Client code should not call select_from explicitly, raise an
        error if they attempt to do this.
        """
        raise RuntimeError(
            'select_from is automatically generated based on the'
            ' expressions present in this query, do not call'
            ' select_from explicitly.'
        )


@compiles(StarSchemaSelect)
def compile_star_schema_select(element: StarSchemaSelect, compiler, **kw):
    """
    Compile a StarSchemaSelect element, this function turns the special
    StarSchemaSelect created by ``StarSchema.select`` into SQL code.
    We generate the appropriate ``select_from`` based on the
    expressions that occur in the query.
    """
    def get_children(expression):
        """
        Recursively traverse the given expression tree and yield all
        sub-expressions
        """
        # don't generate joins for scalar selects
        if not isinstance(expression, sa.sql.selectable.ScalarSelect):
            children = list(expression.get_children())
            # don't recurse table aliases
            if isinstance(expression, sa.sql.Alias):
                children.remove(expression.original)
            for child in children:
                if isinstance(child, (sa.sql.expression.Alias, sa.sql.expression.TableClause)):
                    yield child
                elif isinstance(child, sa.sql.expression.ClauseList):
                    for clause in child.clauses:
                        is_column = isinstance(clause, sa.sql.expression.ColumnClause)
                        if is_column and clause.table is not None:
                            yield clause.table
                yield from get_children(child)

    # find all the tables that need to be joined by recursively looking
    # at each expression in the query, and traversing the star topology
    # for each table found to the center. This will determine the joins
    # that are required
    joins = (
        get_children(element)
        | mapwith(element.star_schema.path_to_center)
        | to(chain.from_iterable)
        | to(unique)
        | to(list)
    )

    # generate the select_from using all the referenced tables and the
    # various joins required for these tables based on the traversal
    # we just did.
    select_from = element.star_schema.center
    for left, right in joins:
        select_from = select_from.join(
            right.center,
            onclause=right.join.onclause(left.center.c, right.center.c),
            isouter=right.join.isouter,
            full=right.join.full,
        )

    # now let SQLAlchemy complete the compilation of the actual query
    # using the generated select_from
    select = super(StarSchemaSelect, element).select_from(select_from)
    return compiler.process(super(StarSchemaSelect, select), **kw)
