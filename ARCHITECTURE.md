CLI commands / JSONL serve
          |
          v
        DB API
          |
       DB.mu (RWMutex) ----------------------> GET returns a copy
          |
          v
  committed map[string][]byte
          ^
          | update only after durable storage commit
          |
  maxBytes complete-transaction preflight
          |
          v
  storage.mu + append-only file engine
          |
  marshal diskRecord -> uint32 length prefix
          |
  operation records -> commit record -> File.Sync()
          |
          +---------------- success ----------------> map update -> success / ACK
          |
          +-- append or sync error
                    |
                    v
          rollback: truncate -> seek -> Sync
                    |
          +-- rollback succeeds -> return write error; map unchanged
          |
          +-- rollback/sync uncertain -> poison storage
                                      -> ErrRecoveryRequired
                                      -> JSONL ERROR / UNKNOWN; stop serving

Open -> candidate validation -> recovery -> DB ready
Compact -> DB write lock -> live snapshot -> KVCPv003 .compact-tmp
        -> Sync / close / independent validation
        -> platform replacement + .compact-old -> reopen -> cleanup