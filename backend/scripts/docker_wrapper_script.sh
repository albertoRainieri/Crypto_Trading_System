#!/bin/bash

# Start the first process
/bin/bash /backend/scripts/fastapi_process.sh &
  
# Start the second process
/bin/bash /backend/scripts/python_process.sh &

/bin/bash /backend/scripts/python_process_btc_eth.sh &

/bin/bash /backend/scripts/wss_python_process.sh &

# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?