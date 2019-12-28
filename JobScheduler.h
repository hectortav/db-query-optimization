#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

#include <cstdlib>
#include <pthread.h>
#include <iostream>
#define THREADS_NUM 8
#define QUEUE_SIZE 1000

static pthread_mutex_t jobQueueMutex = PTHREAD_MUTEX_INITIALIZER; // mutex for JobQueue
extern pthread_mutex_t* jobsCounterMutexes; // mutex for jobsCounter
// static pthread_mutex_t jobSchedulerDestroyMutex = PTHREAD_MUTEX_INITIALIZER;

// jobQueueFullCond: JobQueue is currently full
// jobQueueEmptyCond: JobQueue is currently empty
static pthread_cond_t jobQueueFullCond = PTHREAD_COND_INITIALIZER, jobQueueEmptyCond = PTHREAD_COND_INITIALIZER; // condition variables for JobQueue
// static pthread_cond_t jobSchedulerDestroyCond = PTHREAD_COND_INITIALIZER;
extern int64_t* jobsCounter;

// Abstract Class Job
class Job
{
private:
    int jobId;

public:
    Job()
    {
        jobId = 0;
    };
    virtual ~Job() {}

    // This method should be implemented by subclasses.
    virtual void run() = 0;
    virtual int setJobId(int jobId)
    {
        this->jobId = jobId;
        return jobId;
    };

    int getJobId()
    {
        return jobId;
    };
};

class JobListNode
{
public:
    Job *job;
    JobListNode *nextNode;

    JobListNode(Job *job);
    ~JobListNode();
};

class JobQueue
{
private:
    JobListNode *firstNode, *lastNode;
    int maxSize, currentSize;

public:
    JobQueue(int maxSize);
    ~JobQueue();

    int getCurrentSize();
    bool isEmpty();
    bool isFull();
    int insertJobAtEnd(Job *job);
    JobListNode *getNodeFromStart();
};

// Class JobScheduler
class JobScheduler
{
private:
    JobQueue *jobQueue;
    pthread_t *threadIds;
    int nextJobId, threadsNum;

public:
    // Initializes the JobScheduler with the number of open threads.
    JobScheduler(int threadsNum, int jobsMaxNum);
    // Free all resources that are allocated by JobScheduler
    ~JobScheduler();

    // Waits Until executed all jobs in the queue.void Barrier();
    // Add a job in the queue and returns a job id
    int schedule(Job *job, int queryIndex);
    int getThreadsNum();
    // void* threadWork(void*);
};

void *threadWork(void *arg);

class ExitJob : Job
{
public:
    ExitJob() {}
    ~ExitJob() {}

    void run() {}
    int setJobId(int jobId)
    {
        return Job::setJobId(-1);
    }
};

#endif