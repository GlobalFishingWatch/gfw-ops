from python_app_template import version


def test_version():
    assert isinstance(version.__version__, str)
