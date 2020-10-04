import sqlparse


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


def assert_query_equals(actual, expected):
    actual_formatted_str = sqlparse.format(query_str(actual).strip(), reindent=True)
    expected_formatted_str = sqlparse.format(expected.strip(), reindent=True)
    if actual_formatted_str.lower() != expected_formatted_str.lower():
        assert actual_formatted_str == expected_formatted_str