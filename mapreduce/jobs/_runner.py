#!/usr/bin/python
import pickle
import sys
import base64

job = pickle.loads(base64.b64decode(sys.argv[1]))
task = sys.argv[2]
if task == "map":
    job.map()
elif task == "reduce":
    job.reduce()
else:
    raise BaseException("_runner param must be 'map' or 'reduce'")