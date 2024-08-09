import os
import orjson

with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modoptions.json'), 'r'
) as f:
    modoptions = orjson.loads(f.read())
