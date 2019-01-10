#ifndef CARAMEL_JOBSCHEDULER_H
#define CARAMEL_JOBSCHEDULER_H

#include <pthread.h>
#include "JobQueue.h"

pthread_cond_t queueCond;       // Condition variable waiting for all jobs of a kind to finish.
pthread_cond_t threadCond;      // Condition variable waiting for the scheduler to add jobs to the queue.

typedef struct JobScheduler {

    pthread_t * tp;
    JobQueue* queue;
    int activeThreads;      // Protected by queue's mutex.
    int stage;      // The stage of the jobs scheduling. Becomes 1 after all histogram jobs have been schedules, 2
                    // for the partition jobs and 3 after the joinJobs
    int jobs_done;	//counter of completed jobs in a stage. when the stage changes, the counter is reset.
    pthread_mutex_t scheduler_mtx;
    sem_t barrier_sem;	//binary semaphore that is initialized to 0
    					//it suspends the main thread until all jobs of a type/stage are completed
    int exitflag;		//initialized to 0;
} JobScheduler;


JobScheduler* jobSchedulerCreate();

void jobSchedulerDestroy(JobScheduler* jobScheduler);

void schedule(JobScheduler* js, JobQueueElem* job);

void barrier(JobScheduler* js);

void stop(JobScheduler* js);

void* thread_start(void* argv);

int* splitRelation(relation* rel);

#endif //CARAMEL_JOBSCHEDULER_H
