#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

#include <cstdlib>
#include <pthread.h>
#include <iostream>

static pthread_mutex_t jobQueueMutex = PTHREAD_MUTEX_INITIALIZER; // mutex for JobQueue

// jobQueueFullCond: JobQueue is currently full
// jobQueueEmptyCond: JobQueue is currently empty
static pthread_cond_t jobQueueFullCond = PTHREAD_COND_INITIALIZER, jobQueueEmptyCond = PTHREAD_COND_INITIALIZER; // condition variables for JobQueue

// Abstract Class Job
class Job
{
public:
    int jobId;

    Job() = default;
    virtual ~Job() {}

    // This method should be implemented by subclasses.
    virtual int run() = 0;
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
    int schedule(Job *job);
    void threadWork();
};

#endif