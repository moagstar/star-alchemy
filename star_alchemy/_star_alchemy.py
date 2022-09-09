# std
import dataclasses
import typing

# 3rd party
import attr
import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.visitors import iterate

SqlAlchemyTable = typing.Union[sa.sql.expression.Alias, sa.sql.expression.TableClause]


@attr.s(frozen=True, auto_attribs=True)
class Join:
    """
    Describe how two sqlalchemy tables are joined together, will be
    used as arguments to the sqlalchemy join function. So the
    defaults mimic those found in join.
    """
    OnClauseBuilder = typing.Callable[
        [SqlAlchemyTable, SqlAlchemyTable],
        typing.Optional[ClauseElement],
    ]

    table: SqlAlchemyTable
    onclause: OnClauseBuilder = lambda l, r: None
    isouter: bool = True
    full: bool = False

    @property
    def name(self) -> str:
        """
        :return: The name of the right hand table in this join.
        """
        assert self.table.name is not None
        return self.table.name


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
    StarSchemaDict = typing.Dict[str, 'StarSchema']
    SqlAlchemyTableDict = typing.Dict[str, SqlAlchemyTable]

    join: Join
    parent: typing.Optional['StarSchema'] = None
    _children: StarSchemaDict = attr.Factory(dict)
    _schemas: typing.Optional[StarSchemaDict] = None
    _tables: typing.Optional[SqlAlchemyTableDict] = None

    @property
    def tables(self) -> SqlAlchemyTableDict:
        """
        :return: Dictionary mapping names to the SQLAlchemy tables
                 referenced by this schema.
        """
        if self._tables is None:
            self._tables = {s.name: s.table for s in self}
        return self._tables

    def select(self, *args, **kwargs) -> StarSchemaSelect:
        """
        :param args: star args to pass to select
        :param kwargs: double star args to pass to select

        For more information consult the SQLAlchemy documentation:

        https://docs.sqlalchemy.org/en/latest/core/selectable.html#sqlalchemy.sql.expression.select

        :return: StarSchemaSelect instance which will auto generate it's
                 select_from upon compilation.
        """
        return StarSchemaSelect(self, *args, **kwargs)

    @property
    def schemas(self) -> StarSchemaDict:
        """
        :return: Dictionary mapping table names to subschemas.
        """
        if self._schemas is None:
            self._schemas = {s.name: s for s in self}
        return self._schemas

    def __getitem__(self, name) -> 'StarSchema':
        """
        Traverse the tree and retrieve the sub schema with the given
        name.

        :param name: The name (this is the name of the SQLAlchemy table
                     the sub schema holds) of the sub schema to get.

        :return: Sub schema referenced by the requested name.
        """
        return self.schemas[name]

    @property
    def path(self) -> typing.List['StarSchema']:
        """
        :return: Path to the root of the schema.
        """
        def make_path(star_schema) -> typing.Iterator[StarSchema]:
            yield star_schema
            yield from () if star_schema.parent is None else make_path(star_schema.parent)
        return list(reversed(list(make_path(self))))

    @property
    def name(self) -> str:
        """
        :return: The name of the table this schema references.
        """
        return self.join.name

    @property
    def table(self) -> SqlAlchemyTable:
        """
        :return: The Sqlachemy table referenced by this schema.
        """
        return self.join.table

    def detach(self, table_name: str) -> 'StarSchema':
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
        return clone(self[table_name], None)

    def __iter__(self) -> typing.Iterator['StarSchema']:
        """
        :return: Iterator which traverses the tree in a depth first
                 fashion.
        """
        def recurse(star_schema: StarSchema) -> typing.Iterable['StarSchema']:
            yield star_schema
            for child in star_schema._children.values():
                yield from recurse(child)
        return iter(recurse(self))

    def __hash__(self) -> int:
        """
        :return: Hash value to uniquely identify this instance.
        """
        return hash(self.join) | hash(self.parent)

    @classmethod
    def from_dicts(cls, dicts: dict) -> 'StarSchema':
        """
        Create a star schema from recursive dictionaries, the key of
        each dictionary is the root table, the value being the child
        schemas:

        :param dicts: Recursive dicts describing this schema

        :return: StarSchema instance created from ``dicts``.
        """
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
            children = {child.name: child for child in _make_star_schema(children)}
            return StarSchema(join, children=children)

        def _make_star_schema(nodes):
            try:
                for node in nodes.items():
                    star_schema = _make_single_star_schema(*node)
                    for child in star_schema._children.values():
                        child.parent = star_schema
                    yield star_schema
            except Exception as e:
                # When the input dictionary is not of the correct form an exception is
                # raised here. One common reason is if you do not use an empty dict for
                # leaf nodes, e.g. the following is an error:
                #
                #   StarSchema.from_dicts({
                #       tables.sale: {
                #           tables.product   # <- error, should be `tables.product: {}`
                #       }
                #   })
                #
                raise ValueError(nodes) from e

        return next(_make_star_schema(dicts))


@compiles(StarSchemaSelect)
def compile_star_schema_select(element: StarSchemaSelect, compiler, **kw):
    """
    Compile a StarSchemaSelect element, generate the appropriate
    ``select_from`` based on the expressions that occur in the query,
    and then let SQLAlchemy do it's magic on the rest.
    """
    # traverse the expression tree to get all subexpressions, then for
    # each subexpression get the path to the root of the schema, the
    # edges in these paths are the joins that need to be made
    joins = (
        star_schema
        for expression in iterate(element, {'column_collections': False})
        if isinstance(expression, Column)                       # only interested in columns
        if not isinstance(expression.table, StarSchemaSelect)   # but not the select itself
        for star_schema in element.star_schema[expression.table.name].path[1:]
    )

    # generate the select_from using all the referenced tables
    select_from = element.star_schema.table
    for right in dict.fromkeys(joins).keys():
        select_from = select_from.join(
            right.table,
            onclause=right.join.onclause(right.parent.table, right.table),
            isouter=right.join.isouter,
            full=right.join.full,
        )

    # now let SQLAlchemy complete the compilation of the actual query
    # using the generated select_from
    select = super(StarSchemaSelect, element).select_from(select_from)
    return compiler.process(super(StarSchemaSelect, select), **kw)
