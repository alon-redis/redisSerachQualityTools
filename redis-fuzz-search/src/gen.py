import json
from pathlib import Path

class Command:
    def __init__(self, name, arity, func, args):
        self.name = name
        self.arity = arity
        self.func = func
        self.args = args
    def __str__(self):
        return str(f"{self.name}: {self.func}({self.arity}) <- {self.args}")

data = []
folder = Path("redis/src/commands")
for p in folder.glob("*.json"):
    try:
        with p.open("r") as f:
            data.append(json.load(f))
    except:
        pass

cmds = []
for i in range(len(data)):
    try:
        name = next(iter(data[i]))
        if "container" in data[i][name]:
            continue
        cmds.append(Command(
            name,
            data[i][name]["arity"],
            data[i][name]["function"],
            data[i][name]["arguments"],
        ))
    except:
        pass
for c in cmds:
    if c.name == "ZINTER":
        print(c)
