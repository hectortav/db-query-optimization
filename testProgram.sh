#!/bin/bash

echo "Compiling program..."
make finalo

echo
read -p "Workloads path: " workloadsPath

while [[ $inputChar != 's' ]] && [[ $inputChar != 'm' ]]
do
    echo "Press 's' to run program with \"small\" workload or 'm' to run it with \"medium\" workload" 
    read -n1 inputChar
    echo
    case $inputChar in
        s)
            echo "Running program with \"small\" workload..."
            #cat $workloadsPath/small/small.init $workloadsPath/small/small.work | ./final > out.txt
            mkfifo mfifo
            cat > mfifo &
            mpid=$!
            ./final < mfifo
            input=`cat ./input/inputFileNames.txt`
            echo "ok1"
            echo "$input" > mfifo
            #cat ./input/inputFileNames.txt > mfifo 
            echo "ok2"
            #start timer
            input=`cat ./input/inputQueries.txt` 
            echo "$input" > mfifo 
            echo EOF > mfifo
            #cat ./input/inputQueries.txt > mfifo
            kill $mypid
            rm mfifo
            
            ;;
        m)
            echo "Running program with \"medium\" workload..."
            cat $workloadsPath/medium/medium.init $workloadsPath/medium/medium.work | ./final > out.txt
            ;;
        *)
            echo -e "\nInvalid character\n"
            ;;
    esac
done