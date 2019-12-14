#!/bin/bash

startProgramAndCalculateTime() {
    (cat $initFile $workFile | ./final > $outputFile )&
    
    while read path action file
    do
        # echo "The file '$file' appeared/changed in directory '$path' via '$action'"
        if [[ $file == "read_arrays_end" ]]; then
            
            startTimestamp=$(date +%s%N)
            # cat $workFile > P
            exec 3>&-
        elif [[ $file == $outputFile ]] && [[ $action == "MODIFY" ]]; then
            # echo "$(wc -l < $outputFile)"
            # echo "results: $resultsLinesNum"
            # echo "The file '$file' appeared in directory '$path' via '$action'"
            if [[ $resultsLinesNum == $(wc -l < $outputFile) ]]; then
                endTimestamp=$(date +%s%N)
                elapsedTimeNanoseconds=$((endTimestamp-startTimestamp))
                elapsedMinutes=$(($elapsedTimeNanoseconds/60000000000))
                remainingNanoseconds=$(($elapsedTimeNanoseconds%60000000000))
                elapsedSeconds=$(($remainingNanoseconds/1000000000))
                remainingNanoseconds=$(($remainingNanoseconds%1000000000))
                elapsedMilliseconds=$(($remainingNanoseconds/1000000))
                remainingNanoseconds=$(($remainingNanoseconds%1000000))

                echo -e "Queries execution time -> $elapsedMinutes:$elapsedSeconds:$elapsedMilliseconds:$remainingNanoseconds\t(Minutes:Seconds:Milliseconds:Nanoseconds)"
                
                kill -SIGINT -$$
            fi
        fi
    done < <(inotifywait -qm . -e create,modify -e moved_to)
}

while [[ $inputChar != 'y' ]] && [[ $inputChar != 'n' ]]
do
    read -p "Compile program with \"-Ο3\" flag? (y/n) " -n1 inputChar
    echo
    case $inputChar in
        y)
            echo -e "\nCompiling program with \"-Ο3\" flag..."
            make finalo
            ;;
        n)
            echo -e "\nCompiling program normally..."
            make
            ;;
        *)
            echo -e "\nInvalid character.\n"
            ;;
    esac
done

echo

workloadsPath=""
while [[ $workloadsPath == "" ]]
do
    read -p "Workloads path: " workloadsPath
    if [[ $workloadsPath == "" ]]; then
        echo -e "\nEmpty workload path, please try again\n"
    elif [[ ! -d $workloadsPath ]]; then
        echo -e "\nDirectory \"$workloadsPath\" does not exist, please try again\n"
        workloadsPath=""
    fi
done

inputChar=""
while [[ $inputChar != 's' ]] && [[ $inputChar != 'm' ]]
do
    echo "Press 's' to run program with \"small\" workload or 'm' to run it with \"medium\" workload" 
    read -n1 inputChar
    echo
    case $inputChar in
        s)
            echo "Running program with \"small\" workload..."
            #cat $workloadsPath/small/small.init $workloadsPath/small/small.work | ./final > out.txt
            initFile="$workloadsPath/small/small.init"
            resultsFile="$workloadsPath/small/small.result"
            workFile="$workloadsPath/small/small.work"
            outputFile="small_ouput.txt"
            resultsLinesNum=$(wc -l < $resultsFile)
            # mkfifo mfifo
            # cat > mfifo &
            # mpid=$!
            # ./final < mfifo
            # input=`cat ./input/inputFileNames.txt`
            # echo "ok1"
            # echo "$input" > mfifo
            # #cat ./input/inputFileNames.txt > mfifo 
            # echo "ok2"
            # #start timer
            # input=`cat ./input/inputQueries.txt` 
            # echo "$input" > mfifo 
            # echo EOF > mfifo
            # #cat ./input/inputQueries.txt > mfifo
            # kill $mypid
            # rm mfifo

            # mkfifo P
            # ./final < P > $outputFile &
            # exec 3>P # open file descriptor 3 writing to the pipe

            # < P tail -n +1 -f | program
            # cat $initFile > P
            # cat more_stuff.txt > P
            # cat ../workloads/small/small.init - | ./final
                    # cat $workFile > P
                    # exec 3>&-
            startProgramAndCalculateTime
            ;;
        m)
            echo "Running program with \"medium\" workload..."
            
            initFile="$workloadsPath/medium/medium.init"
            resultsFile="$workloadsPath/medium/medium.result"
            workFile="$workloadsPath/medium/medium.work"
            outputFile="medium_ouput.txt"
            resultsLinesNum=$(wc -l < $resultsFile)

            startProgramAndCalculateTime

            # cat $workloadsPath/medium/medium.init $workloadsPath/medium/medium.work | ./final > out.txt
            ;;
        *)
            echo -e "\nInvalid character, please try again\n"
            ;;
    esac
done