#!/usr/bin/env sh
######################################################################
# Starting local dynamodb server.
######################################################################

docker run -d -i -t -p 2700:2700 tray/dynamodb-local -inMemory -port 2700
sleep 10