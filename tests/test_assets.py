import json
from importlib import resources

from gfw_ops import assets


def test_data():
    with open(resources.files(assets) / "data.json") as file:
        data = json.load(file)

    assert data == {"value": 1234}
