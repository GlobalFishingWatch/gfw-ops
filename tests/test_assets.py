import json
from importlib import resources

from python_app_template import assets


def test_data():
    with open(resources.files(assets) / "data.json") as file:
        data = json.load(file)

    assert data == {"value": 1234}
