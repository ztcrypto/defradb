## defradb client p2p replicator delete

Delete a replicator. It will stop synchronizing

### Synopsis

Delete a replicator. It will stop synchronizing.

```
defradb client p2p replicator delete <peer> [flags]
```

### Options

```
  -h, --help   help for delete
```

### Options inherited from parent commands

```
      --logformat string     Log format to use. Options are csv, json (default "csv")
      --logger stringArray   Override logger parameters. Usage: --logger <name>,level=<level>,output=<output>,...
      --loglevel string      Log level to use. Options are debug, info, error, fatal (default "info")
      --lognocolor           Disable colored log output
      --logoutput string     Log output path (default "stderr")
      --logtrace             Include stacktrace in error and fatal logs
      --rootdir string       Directory for data and configuration to use (default: $HOME/.defradb)
      --tx uint              Transaction ID
      --url string           URL of HTTP endpoint to listen on or connect to (default "localhost:9181")
```

### SEE ALSO

* [defradb client p2p replicator](defradb_client_p2p_replicator.md)	 - Configure the replicator system

