import difflib
import doctest

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


def assert_query_equal(actual, expected):
    if not isinstance(actual, str):
        actual = query_str(actual)
    actual_formatted_str = sqlparse.format(actual.strip(), reindent=True)
    expected_formatted_str = sqlparse.format(expected.strip(), reindent=True)
    if actual_formatted_str.lower() != expected_formatted_str.lower():
        if actual_formatted_str != expected_formatted_str:
            diff = difflib.unified_diff(
                actual_formatted_str.split('\n'),
                expected_formatted_str.split('\n'),
            )
            raise AssertionError('\n'.join(diff))


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


def DocTestMixin(*run_doctests_for_these_objects):
    """
    Create doctests.
    """

    class DocTestMixin:
        def test_doctest(self):
            for doc_test_object in run_doctests_for_these_objects:
                for test in doctest.DocTestFinder().find(doc_test_object):
                    with self.subTest(test.name):
                        report = []
                        result = doctest.DocTestRunner(verbose=True).run(test, out=report.append)
                        if result.failed:
                            self.fail('\n'.join(report))

    return DocTestMixin
