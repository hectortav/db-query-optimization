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
        JobListNode *nextNode = curNode->nextNode;
        // std::cout << "size: " << currentSize << std::endl;
        delete curNode;
        // curNode = NULL;
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
        schedule((Job *)job);
        //std::cout << "queue size: " << jobQueue->getCurrentSize()<<std::endl;
    }

    for (int i = 0; i < threadsNum; i++)
    {
        pthread_join(threadIds[i], NULL);
        // std::cout << "Thread " << threadIds[i] << " exited" << std::endl;
    }

    delete jobQueue;
    jobQueue = NULL;
    delete[] threadIds;
    threadIds = NULL;
    // std::cout << "JobScheduler deleted" << std::endl;
}

int JobScheduler::schedule(Job *job)
{
    pthread_mutex_lock(&jobQueueMutex);
    // std::cout<<jobQueue->getCurrentSize()<<std::endl;
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

    pthread_mutex_unlock(&jobQueueMutex);
    pthread_cond_signal(&jobQueueEmptyCond);

    //std::cout << "Job scheduled" << std::endl;
    return jobId;
}

void *threadWork(void *arg)
{
    JobQueue *jobQueue = (JobQueue *)arg;
    while (1)
    {
        // std::cout<<"thread "<<pthread_self()<<", current queue size: "<<jobQueue->getCurrentSize()<<std::endl;

        pthread_mutex_lock(&jobQueueMutex);
        // std::cout << "thread " << pthread_self() << ", 2, queue is empty: " << jobQueue->isEmpty() << std::endl;

        while (jobQueue->isEmpty())
        {
            pthread_cond_wait(&jobQueueEmptyCond, &jobQueueMutex);
        }
        // std::cout << "1" << std::endl;
        // get node from cyclic buffer by copying its contents to curBufferNode
        JobListNode *curJobListNode = jobQueue->getNodeFromStart();
                // std::cout << "thread " << pthread_self() << " got Job with id " << curJobListNode->job->getJobId() << std::endl;

        // std::cout << "2" << std::endl;
        // std::cout<<"thread "<<pthread_self()<<", 3"<<std::endl;

        pthread_mutex_unlock(&jobQueueMutex);
        pthread_cond_signal(&jobQueueFullCond);
        // std::cout << "3" << std::endl;
        // std::cout<<"thread "<<pthread_self()<<", 4"<<std::endl;

        if (curJobListNode->job->getJobId() == -1) // only ExitJob has jobId == -1
        {
            //std::cout << "thread " << pthread_self() << ", 5" << std::endl;

            // thread should exit
            delete curJobListNode;
            pthread_exit((void **)0);
        }
        // printf("3\n");

        // printf("before run\n");
        curJobListNode->job->run();
        // std::cout<<"thread "<<pthread_self()<<"after run"<<std::endl;

        delete curJobListNode;
    }

    pthread_exit((void **)0);
}