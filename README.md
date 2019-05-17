This is a tool for benchmarking postgresql with the access pattern that Sensu
events would use, if Sensu events used postgresql.

The number of reader/writer goroutines is tunable, as is the total number of
events.
