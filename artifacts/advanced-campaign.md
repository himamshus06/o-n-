# Advanced campaign

Categories:
- concurrency-library-stress: 1/1 passed, 0 failed
- real-binary-seeded-workloads: 0/0 passed, 0 failed

Environment blocked:
- race detector unavailable: go: -race requires cgo; enable cgo by setting CGO_ENABLED=1
- Program 'kvdb-fault.exe' failed to run: An error occurred trying to start process 'C:\Users\ANUJNA~1\AppData\Local\Temp\kvdb-advanced-00c40de46eb54cb6b9b9518d054c36e1\kvdb-fault.exe' with working directory 'D:\Srinivas'. An Application Control policy has blocked this file.At D:\Srinivas\torture\advanced-campaign.ps1:36 char:80
+ …  { $_ | ConvertTo-Json -Compress } | & $exe serve $db 2>$null); $code …
+                                        ~~~~~~~~~~~~~~~~~~~~~~~~.

Simulated only:
- src/kvdb/storage_test.go memoryFile short-write, rollback, sync-failure tests
- torture/simulator.py fault controls
