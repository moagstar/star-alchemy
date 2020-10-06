# std
import typing
from itertools import chain
# 3rd party
import attr
import sqlalchemy as sa
from funcoperators import to, mapwith
from sqlalchemy.ext.compiler import compiles
from toolz import unique


@attr.s(frozen=True, auto_attribs=True)
class Join:
    """
    Describe how two sqlalchemy tables are joined together, will be
    used as arguments to the sqlalchemy join function. So the
    defaults mimic those found in join.
    """
    onclause: typing.Union[typing.Callable, None] = lambda *_: None
    isouter: bool = True
    full: bool = False


SqlAlchemyTable = typing.Union[sa.sql.expression.Alias, sa.sql.expression.TableClause]


@attr.s(auto_attribs=True)
class StarSchema:

    table: SqlAlchemyTable
    parent: typing.Optional['StarSchema'] = None
    join: Join = Join()
    _children: typing.Dict[str, 'StarSchema'] = attr.Factory(dict)

    def select(self, *args, **kwargs):
        return StarSchemaSelect(self, *args, **kwargs)

    @property
    def tables(self):
        return {s.table.name: s.table for s in self}

    def path(self, table):
        star_schema = {s.table: s for s in self}[table]
        path = []
        while star_schema.parent != None:
            path.append((star_schema.parent, star_schema))
            star_schema = star_schema.parent
        return reversed(path)

    def __attrs_post_init__(self):
        if self.parent is not None:
            self.parent._children[self.table.name] = self

    def __getitem__(self, item):
        # make a detached clone of the requested sub schema
        return attr.evolve(self._children[item], parent=None)

    def __iter__(self):
        def recurse(s: StarSchema):
            yield s
            for c in s._children.values():
                yield from recurse(c)
        return recurse(self)

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

        :param dicts: Recursive dicts describing this schema

        :return: StarSchema instance created from ``dicts``.
        """
        if len(dicts) > 1:
            raise ValueError("Star schema should have 1 root node")

        # TODO: Really not sure about this way of creating
        #  the schema
        def _make(d):
            d, j = d if isinstance(d, tuple) else d, Join()
            return [
                StarSchema(
                    c,
                    join=j,
                    children=dict(map(lambda child: (child.table.name, child), _make(k))),
                )
                for c, k in d.items()
            ]

        instance = _make(dicts)[0]

        def recurse(s):
            for child in s._children.values():
                child.parent = s
                recurse(child)
        recurse(instance)

        return instance


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
        # TODO: Maybe this function can be cleaned / simplified a bit
        # don't generate joins for scalar selects
        if not isinstance(expression, sa.sql.selectable.ScalarSelect):
            children = list(expression.get_children())
            # don't recurse table aliases
            if isinstance(expression, sa.sql.Alias):
                children.remove(expression.original)
            for child in children:
                # TODO: Didn't we remove aliases above - check this?
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
        | mapwith (element.star_schema.path)
        | to (chain.from_iterable)
        | to (list)
    )
    # TODO: Because StarSchema is mutable we can't use hash to test for
    #  unique. So we have to use this rather ugly method. Should come
    #  up with a nicer way to do this.
    joins = unique(joins, key=lambda lr: (lr[0].table, lr[1].table))

    # generate the select_from using all the referenced tables
    select_from = element.star_schema.table
    for left, right in joins:
        select_from = select_from.join(
            right.table,
            onclause=right.join.onclause(left.table.c, right.table.c),
            isouter=right.join.isouter,
            full=right.join.full,
        )

    # now let SQLAlchemy complete the compilation of the actual query
    # using the generated select_from
    select = super(StarSchemaSelect, element).select_from(select_from)
    return compiler.process(super(StarSchemaSelect, select), **kw)
