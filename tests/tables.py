import pytest
import sqlalchemy as sa
from star_alchemy import StarSchema

meta = sa.MetaData()

sale = sa.Table('sale', meta,
    sa.Column('id', sa.Integer),
    sa.Column('product_id', sa.Integer, sa.ForeignKey('product.id')),
    sa.Column('employee_id', sa.Integer, sa.ForeignKey('employee.id')),
    sa.Column('customer_id', sa.Integer, sa.ForeignKey('customer.id')),
    sa.Column('total', sa.Integer),
    sa.Column('quantity', sa.Integer),
    sa.Column('discount', sa.Integer),
)

product = sa.Table('product', meta,
    sa.Column('id', sa.Integer),
    sa.Column('unit_price', sa.Numeric),
    sa.Column('name', sa.Text),
    sa.Column('category_id', sa.Integer, sa.ForeignKey('category.id')),
)

category = sa.Table('category', meta,
    sa.Column('id', sa.Integer),
    sa.Column('name', sa.Text),
)

department = sa.Table('department', meta,
    sa.Column('id', sa.Integer),
    sa.Column('department_level_0', sa.Text),
    sa.Column('department_level_1', sa.Text),
)

location = sa.Table('location', meta,
    sa.Column('id', sa.Integer),
    sa.Column('country', sa.Text),
    sa.Column('state', sa.Text),
    sa.Column('city', sa.Text),
    sa.Column('postal code', sa.Text),
)

customer = sa.Table('customer', meta,
    sa.Column('id', sa.Integer),
    sa.Column('name', sa.Text),
    sa.Column('location_id', sa.Integer, sa.ForeignKey('location.id')),
)

employee = sa.Table('employee', meta,
    sa.Column('id'),
    sa.Column('location_id', sa.Integer, sa.ForeignKey('location.id')),
    sa.Column('department_id', sa.Integer, sa.ForeignKey('department.id')),
)


