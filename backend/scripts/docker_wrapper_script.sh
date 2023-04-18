#!/bin/bash

# Start the first process
/bin/bash /backend/scripts/fastapi_process.sh &
  
# Start the second process
/bin/bash /backend/scripts/python_process.sh &

/bin/bash /backend/scripts/python_process_btc_eth.sh &

# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?