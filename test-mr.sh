#!/bin/bash
# sudo sysctl -w kern.maxfiles=100000

echo ""
echo "==> Part I"
go test -run Sequential github.com/xlk3099/gomapreduce/mapreduce/
echo ""
echo "==> Part II"
sh ./test-wc.sh > /dev/null
echo ""
echo "==> Part III"
go test -run TestBasic github.com/xlk3099/gomapreduce/mapreduce/
echo ""
echo "==> Part IV"
go test -run Failure github.com/xlk3099/gomapreduce/mapreduce/
echo ""
echo "==> Part V (challenge)"
sh ./test-ii.sh > /dev/null

rm mrtmp.* diff.out
