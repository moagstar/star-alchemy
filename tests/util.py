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
    if not isinstance(actual, str):
        actual = query_str(actual)
    actual_formatted_str = sqlparse.format(actual.strip(), reindent=True)
    expected_formatted_str = sqlparse.format(expected.strip(), reindent=True)
    if actual_formatted_str.lower() != expected_formatted_str.lower():
        assert actual_formatted_str == expected_formatted_str


def normalize_query(q):
    if not isinstance(q, str):
        q = query_str(q)
    return sqlparse.format(
        q.strip(),
        reindent=True,
        reindent_aligned=True,
        keyword_case='upper',
        use_space_around_operators=True,
        strip_whitespace=True,
    )


class AssertQueryEqualMixin:
    def assertQueryEqual(self, expected, actual):
        self.assertEqual(normalize_query(expected), normalize_query(actual))


def query_test(expected):
    def decorator(fn):
        def inner(self):
            return self.assertQueryEqual(expected, fn(self))
        return inner
    return decorator

