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
We require the [megacarbon](https://github.com/graphite-project/carbon/tree/megacarbon) branch of carbon daemon. This is becase no other version permits storage plugins. There is still a small level of change that needs to be done as the plugin autodiscovery mechanism is broken. Hence, you need to use this fork:
```
pip install -git+https://github.com/InMobi/carbon.git@leveltsd#egg=carbon

```

Set the storage engine as follows by editing your _db.conf_
```
DATABASE = level-tsd
```

### Graphite-web
A new graphite web connector is needed to access this new backend. The carbon daemon and the graphite-web deployment need not be on the same host. Hence the finder is a different project

See InMobi/level-tsd-finder for getting the graphite web connector.

## TODO
(All of these should be turned into issues)

1. Make the json RPC port configurable
1. Make the binding network interface of the json RPC configurable
1. Leveldb tunables (block size, buffer size, cache size, etc. etc.) as configurations
1. Flush the batched metrics after a fixed time interval
1. Configurable size for write batch
