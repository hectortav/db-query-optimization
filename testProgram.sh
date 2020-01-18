#!/bin/bash

startProgramAndCalculateTime() {
    initS=$(sed -e "$ ! s#^#$pathPrefix/#" "$initFile")
    initS="$initS\n"
    workS=$(cat $workFile)
    (echo -e "${initS}${workS}" | ./final $args > $outputFile )&
    
    while read path action file
    do
        if [[ $file == "read_arrays_end" ]]; then
            startTimestamp=$(date +%s%N)
        elif [[ $file == $outputFile ]] && [[ $action == "MODIFY" ]]; then
            if grep -q "Wrong arguments" "$outputFile"; then
                cat $outputFile
                kill -SIGINT -$$
            fi
            if [[ $(wc -l < $resultsFile) == $(wc -l < $outputFile) ]]; then
                endTimestamp=$(date +%s%N)
                elapsedTimeNanoseconds=$((endTimestamp-startTimestamp))
                elapsedMinutes=$(($elapsedTimeNanoseconds/60000000000))
                remainingNanoseconds=$(($elapsedTimeNanoseconds%60000000000))
                elapsedSeconds=$(($remainingNanoseconds/1000000000))
                remainingNanoseconds=$(($remainingNanoseconds%1000000000))
                elapsedMilliseconds=$(($remainingNanoseconds/1000000))
                remainingNanoseconds=$(($remainingNanoseconds%1000000))

                echo -e "Queries execution time -> $elapsedMinutes:$elapsedSeconds:$elapsedMilliseconds:$remainingNanoseconds\t(Minutes:Seconds:Milliseconds:Nanoseconds)"
                
                diffOutput=$(diff "$outputFile" "$resultsFile")
                if [[ $diffOutput == "" ]]; then
                    echo "Result: Correct"
                else
                    echo "Result: Wrong"
                    echo "diff output:"
                    echo $diffOutput
                fi

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
            make -B
            ;;
        *)
            echo -e "\nInvalid character.\n"
            ;;
    esac
done

echo

read -p "Provide arguments for program run according to the following guide: 
-qr                                 (QueRy) Run in queries of every batch in parallel
-ro                                 (ReOrder) Run bucket reorder (radix-sort) in parallel
-pb                                 (Reorder -> New job Per Bucket) (\"-ro\" should be provided) Create a new parallel job for each new bucket
-qs                                 (QuickSort) Run quicksorts independently
-jn                                 (JoiN) Run joins in parallel (split arrays)
-jnthreads                          (JoiN THREADS) Extra flag when parallel join is running. It will split every join array in <num of threds> parts
                                    *Default is: split join array by prefix

-ft                                 (FilTer) Runs filters in parallel
-pj                                 (ProJection) Runs Projection checksums in parallel
-all                                (ALL) Everything runs in parallel
-n <threads>                        Specify number of threads to run (if 1 is provided, then program will run in serial mode)
-optimize                           Optimize the Predicates Given

[no argument or any other argument] Everything runs serial
-> " args

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
            initFile="$workloadsPath/small/small.init"
            resultsFile="$workloadsPath/small/small.result"
            workFile="$workloadsPath/small/small.work"
            pathPrefix="$workloadsPath/small"
            outputFile="small_ouput.txt"
            startProgramAndCalculateTime
            ;;
        m)
            echo "Running program with \"medium\" workload..."
            
            initFile="$workloadsPath/medium/medium.init"
            resultsFile="$workloadsPath/medium/medium.result"
            workFile="$workloadsPath/medium/medium.work"
            pathPrefix="$workloadsPath/medium"
            outputFile="medium_ouput.txt"
            startProgramAndCalculateTime
            ;;
        *)
            echo -e "\nInvalid character, please try again\n"
            ;;
    esac
done