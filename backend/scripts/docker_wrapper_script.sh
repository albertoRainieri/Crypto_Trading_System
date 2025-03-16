#!/bin/bash

# Start the first process
/bin/bash /backend/scripts/fastapi_process.sh &
echo 'ANALYSIS: ' $ANALYSIS
  
# Start the second process
if [ $ANALYSIS = 1 ]
then
    echo 'Analysis Mode On'
    /bin/bash /backend/scripts/analysis_mode.sh &
else
    for i in $(seq 1 "${SETS_WSS_BACKEND}"); do  # Double quotes are important!
        /bin/bash /backend/scripts/wss_python_process.sh list${i} &
        sleep 2
    done

fi


# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?