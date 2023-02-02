import sqlalchemy as sa


def select(schema, measures, dimensions, filters):
    return (
        schema.select(*dimensions, *measures)
        .where(sa.and_(*filters))
        .group_by(sa.func.cube(*dimensions))
    )
