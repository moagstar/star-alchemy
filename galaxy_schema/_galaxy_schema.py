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


# in compile_galaxy_schema_select we will use the sqlalchemy join
# function to join tables, so the values here mimic the default values
# required by that function.
Join.__new__.__defaults__ = (lambda *_: None, True, False)


SqlAlchemyTable = typing.Union[sa.sql.expression.Alias, sa.sql.expression.TableClause]


class GalaxySchema:
    """
    Recursive container of SqlAlchemy selectables which models a
    galaxy schema. The schema is a tree, where the nodes in the
    tree are SQLAlchemy tables and the edges describe the way these
    tables should be joined. By modelling the data schema in this
    fashion we can construct queries using only expressions, and
    letting the topology of the tree determine the appropriate joins.

    A GalaxySchema can be constructed like so:

        >>> galaxy_schema = GalaxySchema(
        ...
        ... )

    Each node in the schema is itself a GalaxySchema, and in this
    fashion one can compose schemas easily using ``clone`` and ``extend``:

        >>> galaxy_schema.clone()

    The schema can also be seen as a container of sqlalchemy tables,
    these tables can be retrieved from the collection so:

        >>> galaxy_schema.table

    Each table must have a unique name, so if the same sqlalchemy table
    is referenced multiple times in the schema it must be aliased:

        >>> GalaxySchema()

    Once a topology is defined we can select from it. It can be used to
    generate queries using regular sqlalchemy, GalaxySchema.select
    generates a sqlalchemy query...

        >>> galaxy_schema.select(galaxy_schema.some_table.c)

    Notice that the ``select_from`` is not needed, this is
    automatically generated based on the expressions in the query upon
    compilation.
    """
    GalaxySchemas = typing.Iterable['GalaxySchema']

    def __init__(
        self,
        center: SqlAlchemyTable,
        children: typing.Optional[GalaxySchemas] = None,
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

        self._galaxys_schemas = {}
        self.parent = None

        def init_tree(galaxy_schema: GalaxySchema):
            if galaxy_schema.center.name in self._galaxys_schemas:
                raise ValueError(
                    f'{galaxy_schema.center.name} already exists in'
                    f' this galaxy schema'
                )
            self._galaxys_schemas[galaxy_schema.center.name] = galaxy_schema
            for child_galaxy_schema in galaxy_schema._children:
                child_galaxy_schema.parent = galaxy_schema
                init_tree(child_galaxy_schema)

        init_tree(self)

    def select(self, *args, **kwargs):
        """
        Construct a SQLAlchemy select statement from the galaxy schema.
        The main difference between this and ``sqlalchemy.select`` is
        that we can automatically determine which joins are needed based
        on the defined schema and the requested expressions.

        :param args: Position arguments (see sqlalchemy.select)
        :param kwargs: Keyword arguments (see sqlalchemy.select)

        :return: SQLAlchemy select statement.
        """
        return GalaxySchemaSelect(self, *args, **kwargs)

    def __getattr__(self, table_name: str):
        """

        :param table_name:

        :return:
        """
        return self._galaxys_schemas[table_name].center

    def __repr__(self):
        """
        :return:
        """
        return self.center.name

    def path_to_center(self, table):
        """

        :param table:

        :return:
        """
        if table.name in self._galaxys_schemas:
            galaxy_schema = self._galaxys_schemas[table.name]
            path = []
            while galaxy_schema.parent != None:
                path.append((galaxy_schema.parent, galaxy_schema))
                galaxy_schema = galaxy_schema.parent
            return reversed(path)
        else:
            return []


class GalaxySchemaSelect(sa.sql.expression.Select):
    """
    Custom SQLAlchemy expression for automatically generating the
    select_from for a query based on the expressions that are present
    in the query. In addition to the actual query (defined by *args
    and **kwargs, we store the galaxy that contains the tables for
    generating the appropriate select_from based on the expressions
    in the query).
    """
    def __init__(self, galaxy_schema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.galaxy_schema = galaxy_schema

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


@compiles(GalaxySchemaSelect)
def compile_galaxy_schema_select(element, compiler, **kw):
    """

    :param element:
    :param compiler:
    :param kw:

    :return:
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
    # at each expression in the query, and traversing the galaxy topology
    # for each table found to the center. This will determine the joins
    # that are required
    joins = (
        get_children(element)
        | mapwith(element.galaxy_schema.path_to_center)
        | to(chain.from_iterable)
        | to(unique)
        | to(list)
    )

    # generate the select_from using all the referenced tables and the
    # various joins required for these tables based on the traversal
    # we just did.
    select_from = element.galaxy_schema.center
    for left, right in joins:
        select_from = select_from.join(
            right.center,
            onclause=right.join.onclause(left.center.c, right.center.c),
            isouter=right.join.isouter,
            full=right.join.full,
        )

    # now let SQLAlchemy complete the compilation of the actual query
    # using the generated select_from
    select = super(GalaxySchemaSelect, element).select_from(select_from)
    return compiler.process(super(GalaxySchemaSelect, select), **kw)
