# std
import typing
# 3rd party
import attr
import sqlalchemy as sa
from funcoperators import to
from sqlalchemy import Column
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ColumnCollection, ClauseElement
from sqlalchemy.sql.visitors import iterate
from toolz import unique


SqlAlchemyTable = typing.Union[sa.sql.expression.Alias, sa.sql.expression.TableClause]


@attr.s(frozen=True, auto_attribs=True)
class Join:
    """
    Describe how two sqlalchemy tables are joined together, will be
    used as arguments to the sqlalchemy join function. So the
    defaults mimic those found in join.
    """
    OnClauseBuilder = typing.Callable[[ColumnCollection, ColumnCollection], ClauseElement]

    table: SqlAlchemyTable
    onclause: OnClauseBuilder = lambda *_: None
    isouter: bool = True
    full: bool = False


class StarSchemaSelect(sa.sql.expression.Select):
    """
    Special select which when compiled uses the stored star_schema
    object to automatically generate the ``select_from`` for the
    query based on the expressions present and the topology of the
    schema.
    """
    def __init__(self, star_schema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.star_schema = star_schema


@attr.s(auto_attribs=True, hash=False, order=False)
class StarSchema:
    """

    """
    join: Join
    parent: typing.Optional['StarSchema'] = None
    _children: typing.Dict[str, 'StarSchema'] = attr.Factory(dict)

    @property
    def tables(self) -> typing.Dict[str, SqlAlchemyTable]:
        """
        :return:
        """
        return {star_schema.join.table.name: star_schema.join.table for star_schema in self}

    # TODO: Not sure about this name
    @property
    def schemas(self) -> dict:
        """
        :return:
        """
        return {s.join.table.name: s for s in self}

    def select(self, *args, **kwargs) -> StarSchemaSelect:
        """

        :return:
        """
        return StarSchemaSelect(self, *args, **kwargs)

    @property
    def path(self) -> typing.Tuple['StarSchema']:
        """
        :return:
        """
        def make_path(star_schema):
            yield star_schema
            yield from () if star_schema.parent is None else make_path(star_schema.parent)
        return make_path(self) | to(tuple) | to(reversed) | to(tuple)

    @property
    def name(self) -> str:
        return self.join.table.name

    def __getitem__(self, table_name: str) -> 'StarSchema':
        """
        Get the sub-schema which references ``table_name`` and return
        detached clone of this schema. Normally joins will be generated
        to the root of the schema, in some cases you may not want that.
        Then you can use this function to 'detach' a sub-schema making
        that the root of a new tree.

        :param table_name: The name of the table to get the sub schema
                           for.

        :return: Detached sub-schema.
        """
        def clone(schema, parent):
            schema = attr.evolve(schema, parent=parent)
            schema._children = {
                table_name: clone(child, schema)
                for table_name, child in schema._children.items()
            }
            return schema
        return clone(self.schemas[table_name], None)

    def __iter__(self) -> typing.Iterable['StarSchema']:
        """

        :return:
        """
        def recurse(star_schema: StarSchema) -> typing.Iterable['StarSchema']:
            yield star_schema
            for child in star_schema._children.values():
                yield from recurse(child)
        return recurse(self)

    def __hash__(self) -> int:
        return hash(self.join) | hash(self.parent)

    @classmethod
    def from_dicts(cls, dicts: typing.Dict[str, typing.Any]) -> 'StarSchema':
        """
        Create a star schema from recursive dictionaries, the key of
        each dictionary is the center table, the value being the child
        schemas:

        :param dicts: Recursive dicts describing this schema

        :return: StarSchema instance created from ``dicts``.
        """
        # TODO: Still needs cleaning up a bit
        # TODO: Override for default_on_clause
        if len(dicts) > 1:
            raise ValueError("Star schema should have 1 root node")

        def _default_on_clause(left, right):
            try:
                right_name = right.element.name  # aliases
            except AttributeError:
                right_name = right.name          # tables
            return left.c[f'{right_name}_id'] == right.c['id']

        def _make_single_star_schema(table_or_join, children):
            if isinstance(table_or_join, Join):
                join = table_or_join
            else:
                join = Join(table_or_join, _default_on_clause)
            children = {child.join.table.name: child for child in _make_star_schema(children)}
            return StarSchema(join, children=children)

        def _make_star_schema(nodes):
            for node in nodes.items():
                star_schema = _make_single_star_schema(*node)
                for child in star_schema._children.values():
                    child.parent = star_schema
                yield star_schema

        return next(_make_star_schema(dicts))


@compiles(StarSchemaSelect)
def compile_star_schema_select(element: StarSchemaSelect, compiler, **kw):
    """
    Compile a StarSchemaSelect element, generate the appropriate
    ``select_from`` based on the expressions that occur in the query,
    and then let SQLAlchemy do it's magic on the rest.
    """
    # traverse the expression tree to get all subexpressions, then for
    # each subexpression get the path to the center of the schema, the
    # edges in these paths are the joins that need to be made
    schemas = element.star_schema.schemas
    joins = (
        star_schema

        for expression in iterate(element, {'column_collections': False})
        if isinstance(expression, Column) and not isinstance(expression.table, StarSchemaSelect)

        for star_schema in schemas[expression.table.name].path
        if star_schema.parent is not None  # don't need to create a join from root -> root
    )

    # generate the select_from using all the referenced tables
    select_from = element.star_schema.join.table
    for right in unique(joins):
        select_from = select_from.join(
            right.join.table,
            onclause=right.join.onclause(right.parent.join.table, right.join.table),
            isouter=right.join.isouter,
            full=right.join.full,
        )

    # now let SQLAlchemy complete the compilation of the actual query
    # using the generated select_from
    select = super(StarSchemaSelect, element).select_from(select_from)
    return compiler.process(super(StarSchemaSelect, select), **kw)
