import os

try:
    import requests
except ImportError:
    print("Trying to Install required module: requests\n")
    os.system('python -m pip install requests')

try:
    import enum
except ImportError:
    print("Trying to Install required module: enum\n")
    os.system('python -m pip install enum34')
# try:
# 	import pathlib
# except ImportError:
# 	print ("Trying to Install required module: pathlib\n")
# 	os.system('python -m pip install pathlib')

from flythings.client import \
    ServerClient, ActionDataTypes, SamplingFeatureType

from flythings.config import ServerConfig

import flythings.paths
