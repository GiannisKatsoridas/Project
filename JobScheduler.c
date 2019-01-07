#include <stdlib.h>
#include <stdio.h>
#include "JobScheduler.h"
#include "Globals.h"
#include "Jobs.h"

JobScheduler *jobSchedulerCreate() {

    JobScheduler* js = malloc(sizeof(JobScheduler));

    js->tp = malloc((int) THREAD_NUM * sizeof(pthread_t));
    JobQueueInit(&js->queue, (int) THREAD_NUM);

    pthread_cond_init(&queueCond, NULL);
    pthread_cond_init(&threadCond, NULL);

    semInit(&histogramsSem, 1);

    js->stage = 0;

    return js;
}

void jobSchedulerDestroy(JobScheduler *jobScheduler) {

    free(jobScheduler->tp);
    JobQueueDestroy(&jobScheduler->queue);

}

int schedule(JobScheduler* js, JobQueueElem *job) {

    JobQueuePush(js->queue, job);

    pthread_cond_signal(&threadCond);

    return job->JobID;
}

void barrier(JobScheduler* js){

    pthread_mutex_lock(&js->queue->mtx);

    js->stage++;

    while(js->queue->counter > 0 && js->activeThreads > 0){
        pthread_cond_wait(&queueCond, &js->queue->mtx);
    }

    pthread_mutex_unlock(&js->queue->mtx);

}

void stop(JobScheduler* js){

    pthread_cond_broadcast(&threadCond);

    for(int i=0; i<js->queue->size; i++){

        if(pthread_join(js->tp[i], NULL)!=0){
            perror("Thread no. %d failed to join.");
            return;
        }

    }

}


void* thread_start(void* argv){

    JobScheduler* js = (JobScheduler*) &argv[0];

    pthread_mutex_lock(&js->queue->mtx);

    while(js->queue->counter > 0 || js->stage < 1) {

        while (js->queue->counter <= 0 && js->stage < 1) {
            pthread_cond_wait(&threadCond, &js->queue->mtx);
        }

        if(js->stage >= 1 && js->queue->counter == 0)
            break;

        js->activeThreads++;

        JobQueueElem *job = JobQueuePop(js->queue);

        pthread_mutex_unlock(&js->queue->mtx);

        switch (job->jobType) {

            case 1:
                HistogramJob(job);
                break;

            case 2:
                PartitionJob(job);
                break;

            case 3:
                JoinJob(job);
                break;

            default:
                perror("Error. Wrong JobType.\n");
                return NULL;

        }

        pthread_mutex_lock(&js->queue->mtx);
        pthread_cond_signal(&queueCond);
        js->activeThreads--;
    }

    pthread_mutex_unlock(&js->queue->mtx);

    pthread_exit(NULL);

    return NULL;
}

int *splitRelation(relation *rel) {

    int size = rel->num_tuples;
    int threadsNum = (int) THREAD_NUM;

    int* ret = malloc(threadsNum * sizeof(int));

    if(size < threadsNum){
        for(int i=0; i<size; i++)
            ret[i] = 1;
        for(int i=size; i<threadsNum; i++)
            ret[i] = 0;

        return ret;
    }

    if(size%threadsNum == 0) {

        ret[0] = size/threadsNum;

        for (int i = 1; i < threadsNum; i++)
            ret[i] = ret[i-1] + size / threadsNum;

    }
    else {

        ret[0] = 0;

        for(int i=1; i<threadsNum; i++)
            ret[i] = ret[i-1] + size/threadsNum + 1;

    }

    return ret;
}

