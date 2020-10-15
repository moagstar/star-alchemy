from examples.sales import tables
from star_alchemy import StarSchema


schema = StarSchema.from_dicts({
    tables.sale: {
        tables.product: {
            tables.category: {},
        },
        tables.employee: {
            tables.department: {},
            tables.location.alias('employee_location'): {},
        },
        tables.customer: {
            tables.location.alias('customer_location'): {},
        },
    },
})
