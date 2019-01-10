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

    js->stage = 0;

    js->jobs_done = 0;
    mtx_init(&js->scheduler_mtx);
    semInit(&js->barrier_sem, 0);
    js->exitflag = 0;

    return js;
}

void jobSchedulerDestroy(JobScheduler *jobScheduler) {

    JobQueueDestroy(&jobScheduler->queue);
    free(jobScheduler->tp);
    mtx_destroy(&jobScheduler->scheduler_mtx);
    sem_destroy(&jobScheduler->barrier_sem);

}

int schedule(JobScheduler* js, JobQueueElem *job) {

    P(js->queue->empty);
    mtx_lock(&js->queue->queue_mtx);

    JobQueuePush(js->queue, job);

    mtx_unlock(&js->queue->queue_mtx);
    V(js->queue->full);

    //pthread_cond_signal(&threadCond);

    return job->JobID;
}

void barrier(JobScheduler* js){

    /*mtx_lock(&js->queue->queue_mtx);

    js->stage++;

    while(js->queue->counter > 0 && js->activeThreads > 0){
        pthread_cond_wait(&queueCond, &js->queue->queue_mtx);
    }

    mtx_unlock(&js->queue->queue_mtx);*/

    P(&js->barrier_sem);
    mtx_lock(&js->scheduler_mtx);
    js->exitflag = 1;
    mtx_unlock(&js->scheduler_mtx);
}

void stop(JobScheduler* js){

    //pthread_cond_broadcast(&threadCond);

    for(int i=0; i< (int) THREAD_NUM; i++){
        V(js->queue->full);
        if(pthread_join(js->tp[i], NULL)!=0){
            perror("Thread no. %d failed to join.");
            return;
        }

    }

}


void* thread_start(void* argv){

    JobScheduler* js = (JobScheduler*) &(argv[0]);

    while(1)
    {
        P(js->queue->full);

        mtx_lock(&js->scheduler_mtx);
        if (js->exitflag==1)
        {
            mtx_unlock(&js->scheduler_mtx);
            break;
        }
        mtx_unlock(&js->scheduler_mtx);

        mtx_lock(&js->queue->queue_mtx);

        /*while(js->queue->counter > 0 || js->stage < 1) {

            while (js->queue->counter <= 0 && js->stage < 1) {
                V(js->queue->full);
                pthread_cond_wait(&threadCond, &js->queue->queue_mtx);
                P(js->queue->full);
            }

            if(js->stage >= 1 && js->queue->counter == 0)
                break;

            js->activeThreads++;*/

        JobQueueElem *job = JobQueuePop(js->queue);

        mtx_unlock(&js->queue->queue_mtx);
        V(js->queue->empty);

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

            /*}

            P(js->queue->full);
            mtx_lock(&js->queue->queue_mtx);
            pthread_cond_signal(&queueCond);
            js->activeThreads--;*/
        }

        mtx_unlock(&js->queue->queue_mtx);
        V(js->queue->empty);

        mtx_lock(&js->scheduler_mtx);
        js->jobs_done++;
        if ((job->jobType == 1) && (js->jobs_done == THREAD_NUM))
        {
            js->jobs_done = 0;
            V(&js->barrier_sem);
            //mtx_unlock(&js->scheduler_mtx);
            //js->exitflag = 1;
        }
        else if ((job->jobType == 2) && (js->jobs_done == THREAD_NUM))
        {
            js->jobs_done = 0;
            V(&js->barrier_sem);
            //mtx_unlock(&js->scheduler_mtx);
            //js->exitflag = 1;
        }
        else if ((job->jobType == 3) && (js->jobs_done == job->hash1_value))
        {
            js->jobs_done = 0;
            V(&js->barrier_sem);
            //mtx_unlock(&js->scheduler_mtx);
            //js->exitflag = 1;
            
        }
        mtx_unlock(&js->scheduler_mtx);
    }

    pthread_exit(NULL);
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

