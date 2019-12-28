#include "functions.h"

//using namespace std;
//tuple is in std :/

int main(int argc,char** argv)
{

    params(argv,argc);
    // std::cout<<"Running in: "<<std::endl;
    // std::cout<<"  quicksort mode: "<<quickSortMode<<std::endl;
    // std::cout<<"  reorder mode: "<<reorderMode<<std::endl;
    // std::cout<<"  join mode: "<<joinMode<<std::endl;
    // std::cout<<"  query mode: "<<queryMode<<std::endl;
    InputArray** inputArrays = readArrays();

    FILE * fp = fopen("read_arrays_end", "w");

    if(fp == NULL) {
        std::cout<<"Unable to create file"<<std::endl;
        exit(1);
    }

    fclose(fp);

    int lines;
    // scheduler = new JobScheduler(16, 1000000000);
    pthread_mutex_init(&queryJobDoneMutex,NULL);
    pthread_cond_init(&queryJobDoneCond,NULL);
    while(1)
    {
        lines=0;
        char** arr=readbatch(lines);
        if(arr==NULL)
            break;
       // std::cout<<arr<<std::endl;
        // std::cout<<std::endl;
        //std::cout<<lines<<std::endl;
        JoinQueued=new int[lines];
        jobsCounterMutexes = new pthread_mutex_t[lines];
        jobsCounterConds = new pthread_cond_t[lines];
        predicateJobsDoneMutexes = new pthread_mutex_t[lines];
        predicateJobsDoneConds = new pthread_cond_t[lines];
        lastJobDoneArrays = new bool*[lines];
        // queryJobDoneMutexes = new pthread_mutex_t[lines];
        // queryJobDoneConds = new pthread_cond_t[lines];
        // queryJobDoneArray = new bool[lines];
        QueryResult=new char*[lines];
        jobsCounter = new int64_t[lines];
        queryJobDone=lines;
        // std::cout<<"total queries: "<<lines<<std::endl;
        for (int i = 0; i < lines; i++) {
            pthread_mutex_init(&jobsCounterMutexes[i], NULL);
            pthread_cond_init(&jobsCounterConds[i], NULL);
            jobsCounter[i] = 0;
            QueryResult[i]=new char[100];
            pthread_mutex_init(&predicateJobsDoneMutexes[i], NULL);
            pthread_cond_init(&predicateJobsDoneConds[i], NULL);
            // queryJobDoneMutexes[i] = PTHREAD_MUTEX_INITIALIZER;
            // queryJobDoneConds[i] = PTHREAD_COND_INITIALIZER;
            // queryJobDoneArray[i] = false;

            lastJobDoneArrays[i] = new bool[2];
            lastJobDoneArrays[i][0] = false;
            lastJobDoneArrays[i][1] = false;

            if (queryMode == serial) {
                handlequery(makeparts(arr[i]), (const InputArray**)inputArrays, i);
            } else if (queryMode == parallel) {
                scheduler->schedule(new queryJob(makeparts(arr[i]), (const InputArray**)inputArrays, i), -1);
            }
        }

        // for(int i=0;i<lines;i++)
        // {
        //     // std::cout<<"query index: "<<i<<std::endl;
        //     //std::cout<<arr[i]<<std::endl;
        //     // handlequery(makeparts(arr[i]), (const InputArray**)inputArrays, i);
        //     // std::cout<<std::endl;
        // }
        // std::cout<<"-------------MAIN THREAD: will wait for queries to finish"<<std::endl;

        if (queryMode == parallel) {
            pthread_mutex_lock(&queryJobDoneMutex);
            while (queryJobDone >0){

                pthread_cond_wait(&queryJobDoneCond, &queryJobDoneMutex);
                // struct timespec timeout;
                // clock_gettime(CLOCK_REALTIME, &timeout);
                // timeout.tv_sec += 1;
                // pthread_cond_timedwait(&queryJobDoneCond, &queryJobDoneMutex, &timeout);
                
            }
                // std::cout<<"-------------MAIN THREAD: finished query with index -> "<<i<<std::endl;

            pthread_mutex_unlock(&queryJobDoneMutex);
            // queryJobDoneArray[i] == false;

        
        }
        for(int i=0;i<lines;i++)
        {
            delete[] lastJobDoneArrays[i];
            std::cout<<QueryResult[i]<<std::endl;
            delete[] QueryResult[i];
        }
        delete[] QueryResult;

        // delete[] arr;
        delete[] predicateJobsDoneMutexes;
        delete[] predicateJobsDoneConds;
        delete[] lastJobDoneArrays;
        delete[] jobsCounter;
        delete[] jobsCounterMutexes;
        delete[] JoinQueued;

        for(int i=0;i<lines;i++)
            delete[] arr[i];
        delete[] arr;
        arr=NULL;
    }
    // std::cout<<"-----------------------------------------------------------------------------OUT OF LOOP"<<std::endl;
    delete scheduler;

    for(int i=0;i<MAX_INPUT_ARRAYS_NUM;i++)
    {
        if (inputArrays[i] != NULL) {
            delete inputArrays[i];
        }
    }
    delete[] inputArrays;
    remove("read_arrays_end");
    return 0;
}
