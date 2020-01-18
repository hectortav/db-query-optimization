#include "functions.h"

int main(int argc,char** argv)
{
    params(argv,argc);
    InputArray** inputArrays = readArrays();

    FILE * fp = fopen("read_arrays_end", "w");

    if(fp == NULL) {
        std::cout<<"Unable to create file"<<std::endl;
        exit(1);
    }

    fclose(fp);
    int lines;
    pthread_mutex_init(&queryJobDoneMutex,NULL);
    pthread_cond_init(&queryJobDoneCond,NULL);
    if(queryMode==parallel)
        available_threads=scheduler->getThreadsNum()-1;
    while(1)
    {
        lines=0;
        char** arr=readbatch(lines);
        if(arr==NULL)
            break;
        jobsCounterMutexes = new pthread_mutex_t[lines];
        jobsCounterConds = new pthread_cond_t[lines];
        predicateJobsDoneMutexes = new pthread_mutex_t[lines];
        predicateJobsDoneConds = new pthread_cond_t[lines];
        lastJobDoneArrays = new bool*[lines];
        QueryResult=new char*[lines];
        jobsCounter = new int64_t[lines];
        queryJobDone=lines;
        int Queries_Run=0;
        for (int i = 0; i < lines; i++) {
            pthread_mutex_init(&jobsCounterMutexes[i], NULL);
            pthread_cond_init(&jobsCounterConds[i], NULL);
            jobsCounter[i] = 0;
            QueryResult[i]=new char[100];
            pthread_mutex_init(&predicateJobsDoneMutexes[i], NULL);
            pthread_cond_init(&predicateJobsDoneConds[i], NULL);

            lastJobDoneArrays[i] = new bool[2];
            lastJobDoneArrays[i][0] = false;
            lastJobDoneArrays[i][1] = false;

            if (queryMode == serial) {
                handlequery(makeparts(arr[i]), (const InputArray**)inputArrays, i);

            } else if (queryMode == parallel) {
                pthread_mutex_lock(&queryJobDoneMutex);
                while (available_threads == 0) {
                    pthread_cond_wait(&queryJobDoneCond, &queryJobDoneMutex);
                }
                pthread_mutex_unlock(&queryJobDoneMutex);
                    scheduler->schedule(new queryJob(makeparts(arr[i]), (const InputArray**)inputArrays, i), -1);
                    pthread_mutex_lock(&queryJobDoneMutex);
                    available_threads--;
                    pthread_mutex_unlock(&queryJobDoneMutex);
            }
        }

        if (queryMode == parallel) {
            pthread_mutex_lock(&queryJobDoneMutex);
            while (queryJobDone >0){
                pthread_cond_wait(&queryJobDoneCond, &queryJobDoneMutex);
            }
            pthread_mutex_unlock(&queryJobDoneMutex);
        }
        for(int i=0;i<lines;i++)
        {
            delete[] lastJobDoneArrays[i];
            std::cout<<QueryResult[i]<<std::endl;
            delete[] QueryResult[i];
        }
        delete[] QueryResult;
        delete[] predicateJobsDoneMutexes;
        delete[] predicateJobsDoneConds;
        delete[] lastJobDoneArrays;
        delete[] jobsCounter;
        delete[] jobsCounterMutexes;
        delete[] jobsCounterConds;

        for(int i=0;i<lines;i++)
            delete[] arr[i];
        delete[] arr;
        arr=NULL;
    }
    
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
