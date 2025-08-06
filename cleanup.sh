#!/bin/bash

PORTS=(9001 9002 7000 7001 7010 7011 8090 8080)

for PORT in "${PORTS[@]}"
do
  echo "Checking for processes using port $PORT..."
  PID=$(lsof -ti tcp:$PORT)
  if [ ! -z "$PID" ]; then
    echo "Found process on port $PORT with PID $PID. Attempting to kill..."
    kill -9 $PID
    if [ $? -eq 0 ]; then
      echo "Successfully killed process on port $PORT."
    else
      echo "Failed to kill process on port $PORT. May require sudo."
    fi
  else
    echo "No process found using port $PORT."
  fi
done
