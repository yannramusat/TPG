# vPGt

## Setup 

We connect to a local Neo4j Community Edition instance. 

This instance should be configured with the default settings from `$NEO4J_CONF/neo4j.conf`, 
but to be able to access any CSV from the local file system, we need to make sure Neo4j is configured this way:
```
#server.directories.import=import
dbms.security.auth_enabled=false
#dbms.security.allow_csv_import_from_file_urls=true
```

Hence, no credentials are needed and we can `LOAD CSV` from files located anywhere in the filesystem.

Then, start the server by typing: `sudo /opt/neo4j/bin/neo4j start`.
Do this instead of `sudo -u neo4j /opt/neo4j/bin/neo4j start` in order to avoid permission errors.

This software connects to `localhost` with the default port and with no authentication.

## Specs

We runned the experiments on the following system: 
* HP EliteBook 840 G3 (L3C67AV)
* Intel(R) Core(TM) i7-6600U CPU @ 2.60GHz
* 32GiB system memory (2133 MHz)

And used the following toolchain:
* java-openJDK-17
* Neo4j Community Edition 5.9.0
* Neo4j Python Driver 5.9.0




