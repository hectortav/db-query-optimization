#include "JobScheduler.h"

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
        delete curNode;
        curNode = NULL;
        curNode = curNode->nextNode;
    }
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
        lastNode = NULL;
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
}

JobScheduler::~JobScheduler()
{
    for (int i = 0; i < threadsNum; i++)
    {
        ExitJob *job = new ExitJob();
        schedule((Job *)job);
    }

    for (int i = 0; i < threadsNum; i++)
    {
        pthread_join(threadIds[i], NULL);
    }

    delete jobQueue;
    delete[] threadIds;
}

int JobScheduler::schedule(Job *job)
{
    pthread_mutex_lock(&jobQueueMutex);
    while (jobQueue->isFull())
    {
        pthread_cond_wait(&jobQueueFullCond, &jobQueueMutex);
    }

    if (jobQueue->insertJobAtEnd(job) != 0)
    {
        // Should never happen
        std::cout << "Job could not be inserted into queue" << std::endl;
        pthread_mutex_unlock(&jobQueueMutex);

        return -1;
    }

    pthread_mutex_unlock(&jobQueueMutex);
    pthread_cond_signal(&jobQueueEmptyCond);

    return job->setJobId(nextJobId++);
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

        // get node from cyclic buffer by copying its contents to curBufferNode
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