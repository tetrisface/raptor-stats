import os
import re
import lupa
import lupa.lua54
import orjson
import requests

from bpdb import set_trace as s

# fetch modoptions.lua
# curl -L "https://raw.githubusercontent.com/beyond-all-reason/Beyond-All-Reason/master/modoptions.lua" -o "python/common/modoptions.lua"

lua_string = requests.get(
    'https://raw.githubusercontent.com/beyond-all-reason/Beyond-All-Reason/master/modoptions.lua'
).content.decode('utf-8')

lua = lupa.LuaRuntime(unpack_returned_tuples=True)

lua_string = re.sub(r'^.*?local options = ', '', lua_string, flags=re.DOTALL)
lua_string = re.sub(r'End Options.*$', '', lua_string, flags=re.DOTALL)
lua_string = re.sub(r'for \w+ = .*$', '', lua_string, flags=re.DOTALL)

modoptions = lua.eval(lua_string)


def lua_table_to_dict(lua_table):
    if 'LuaTable' in str(type(lua_table)):
        return {
            str(
                value['key']
                if not isinstance(value, str | int | float) and 'key' in value
                else key
            ): lua_table_to_dict(value)
            for key, value in lua_table.items()
        }
    elif isinstance(lua_table, list):
        return [lua_table_to_dict(item) for item in lua_table]
    else:
        return lua_table


modoptions_py = lua_table_to_dict(modoptions)

if 'options_main' in modoptions_py:
    del modoptions_py['options_main']

if 'sub_header' in modoptions_py:
    del modoptions_py['sub_header']

with open(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', 'common', 'modoptions.json'
    ),
    'w',
) as f:
    f.write(orjson.dumps(modoptions_py, option=orjson.OPT_INDENT_2).decode('utf-8'))
