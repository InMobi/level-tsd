# level-tsd

leveldb based backend for graphite.


## Installion instructions

### Assumption
You are familiar on how to install and operate graphite in general. If not, have a look at [these instructions](https://graphite.readthedocs.org/en/latest/install.html). The _install from source instructions_ [(this one)](https://graphite.readthedocs.org/en/latest/install-source.html) is what the rest of the document specifically based on.

### Getting leveldb
There is a nice script that provides leveldb extensions for python. It can be installed by using this [package](http://code.google.com/p/py-leveldb/)

### Plugin
Install the plugin using pip
```
pip install -git+https://github.com/InMobi/level-tsd.git#egg=pyleveltsd

```

### Carbon
We require the megacarbon branch of carbon daemon. This is becase no other version permits storage plugins. There is still a small level of change that needs to be done as the plugin autodiscovery mechanism is broken. Hence, you need to use this fork:
```
pip install -git+https://github.com/InMobi/carbon.git@leveltsd#egg=carbon

```

Set the storage engine as follows by editing your _db.conf_
```
DATABASE = level-tsd
```

### Graphite-web
__Assumption :__ The graphite version is 0.10.0 or above. Specifically, it needs to support the STORAGE_FINDERS directive

Add the following to your local_settings.py

```py
STORAGE_FINDERS = ( 
    'pyleveltsd.gateway.LevelRpcFinder',
)

LEVEL_RPC_PATH="http://localhost:2005"
```

Note that you may need to modify the _STORAGE_FINDERS_ section if you have other storage mechanisms that need to be used. Most users would be interested in also adding _graphite.finders.standard.StandardFinder_ to the list.

## TODO
(All of these should be turned into issues)

1. Make the json RPC port configurable
1. Make the binding network interface of the json RPC configurable
1. Leveldb tunables (block size, buffer size, cache size, etc. etc.) as configurations
1. Flush the batched metrics after a fixed time interval
1. Configurable size for write batch
