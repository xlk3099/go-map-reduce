# gomapreduce

This map-reduce implementation is based on MIT Lab: https://pdos.csail.mit.edu/6.824/labs/lab-1.html

Can be run in 3 ways:
1. Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
2. Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
3. Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
