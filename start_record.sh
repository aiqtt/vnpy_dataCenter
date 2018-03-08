###stop ####

process_pid=`ps -ef|grep runDataRecording.py|grep -v grep`
echo "process_pid  "$process_pid
if [   -n "$process_pid" ]; then

    kill -9 `ps -ef| grep "runDataRecording.py" | grep -v "grep"  | awk '{print $2}'`
    echo "stop process ok!!!!"
fi

#### start  ###
cd ./dataTools/record/
echo `pwd`

nohup  python runDataRecording.py &

echo "start  ok!!!"