#include "JobScheduler.h"

pthread_mutex_t* jobsCounterMutexes; // mutexes for jobsCounter
int64_t* jobsCounter;

JobListNode::JobListNode(Job *job)
{
    this->job = job;
    this->nextNode = NULL;
}

JobListNode::~JobListNode()
{
    delete job;
}

JobQueue::JobQueue(int maxSize)
{
    firstNode = NULL;
    lastNode = NULL;
    this->maxSize = maxSize;
    currentSize = 0;
}

JobQueue::~JobQueue()
{
    JobListNode *curNode = firstNode;
    while (curNode != NULL)
    {
        JobListNode *nextNode = curNode->nextNode;
        delete curNode;
        curNode = nextNode;
    }
}

int JobQueue::getCurrentSize()
{
    return currentSize;
}

bool JobQueue::isEmpty()
{
    return currentSize == 0;
}

bool JobQueue::isFull()
{
    return currentSize == maxSize;
}

int JobQueue::insertJobAtEnd(Job *job)
{
    if (currentSize == maxSize)
        return 1;

    if (lastNode == NULL)
    {
        lastNode = new JobListNode(job);
        firstNode = lastNode;
    }
    else
    {
        lastNode->nextNode = new JobListNode(job);
        lastNode = lastNode->nextNode;
    }

    currentSize++;

    return 0;
}

JobListNode *JobQueue::getNodeFromStart()
{
    if (currentSize == 0)
        return NULL;

    JobListNode *nodeToReturn = firstNode;

    firstNode = firstNode->nextNode;
    if (currentSize == 1)
    {
        firstNode = lastNode = NULL;
    }

    currentSize--;

    return nodeToReturn;
}

JobScheduler::JobScheduler(int threadsNum, int jobsMaxNum)
{
    jobQueue = new JobQueue(jobsMaxNum);
    threadIds = new pthread_t[threadsNum];
    for (int i = 0; i < threadsNum; i++)
    {
        pthread_create(&threadIds[i], NULL, threadWork, (void *)jobQueue);
    }
    nextJobId = 0;
    this->threadsNum = threadsNum;
}

JobScheduler::~JobScheduler()
{
    for (int i = 0; i < threadsNum; i++)
    {
        ExitJob *job = new ExitJob();
        schedule((Job *)job, -1);
    }

    for (int i = 0; i < threadsNum; i++)
    {
        pthread_join(threadIds[i], NULL);
    }

    delete jobQueue;
    jobQueue = NULL;
    delete[] threadIds;
    threadIds = NULL;
}

int JobScheduler::schedule(Job *job, int queryIndex)
{
    
    pthread_mutex_lock(&jobQueueMutex);
    while (jobQueue->isFull())
    {
        pthread_cond_wait(&jobQueueFullCond, &jobQueueMutex);
    }
    
    int jobId = job->setJobId(nextJobId++);
    
    if (jobQueue->insertJobAtEnd(job) != 0)
    {
        // Should never happen
        std::cout << "Job could not be inserted into queue" << std::endl;
        pthread_mutex_unlock(&jobQueueMutex);

        return -1;
    }
    if (queryIndex != -1) {
        pthread_mutex_lock(&jobsCounterMutexes[queryIndex]);
        jobsCounter[queryIndex]++;
        pthread_mutex_unlock(&jobsCounterMutexes[queryIndex]);
    }
    pthread_mutex_unlock(&jobQueueMutex);
    pthread_cond_signal(&jobQueueEmptyCond);
    return jobId;
}

int JobScheduler::getThreadsNum() {
    return threadsNum;
}

void *threadWork(void *arg)
{
    JobQueue *jobQueue = (JobQueue *)arg;
    while (1)
    {
        pthread_mutex_lock(&jobQueueMutex);

        while (jobQueue->isEmpty())
        {
            pthread_cond_wait(&jobQueueEmptyCond, &jobQueueMutex);
        }

        JobListNode *curJobListNode = jobQueue->getNodeFromStart();

        pthread_mutex_unlock(&jobQueueMutex);
        pthread_cond_signal(&jobQueueFullCond);

        if (curJobListNode->job->getJobId() == -1) // only ExitJob has jobId == -1
        {
            // thread should exit
            delete curJobListNode;
            pthread_exit((void **)0);
        }
        curJobListNode->job->run();

        delete curJobListNode;
    }

    pthread_exit((void **)0);
}