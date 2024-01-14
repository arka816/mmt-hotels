import json

d = json.load(open('test.json'))

json.dump(d, open('test.json', 'w'), indent=4)