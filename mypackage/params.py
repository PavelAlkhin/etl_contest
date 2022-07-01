import pathlib
import json

here = pathlib.Path(__file__).resolve()
config_path = here.parents[1] / "dbconf.json"


def load_params():
    with open(config_path, "r") as f:
        return json.load(f)
