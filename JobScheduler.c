#include <stdlib.h>
#include <stdio.h>
#include "JobScheduler.h"
#include "Globals.h"
#include "Jobs.h"

JobScheduler *jobSchedulerCreate() {

    JobScheduler* js = malloc(sizeof(JobScheduler));

    js->tp = malloc((int) THREAD_NUM * sizeof(pthread_t));
    JobQueueInit(&js->queue, 10);

    js->thread_histograms_R = malloc((int) THREAD_NUM * sizeof(int*));
    js->thread_histograms_S = malloc((int) THREAD_NUM * sizeof(int*));

    pthread_cond_init(&queueCond, NULL);
    pthread_cond_init(&threadCond, NULL);

    js->stage = 0;

    js->jobs_done = 0;
    mtx_init(&js->scheduler_mtx);
    semInit(&js->barrier_sem, 0);
    js->exitflag = 0;

    return js;
}

void jobSchedulerDestroy(JobScheduler *js) {

    JobQueueDestroy(&js->queue);
    free(js->tp);
    mtx_destroy(&js->scheduler_mtx);
    sem_destroy(&js->barrier_sem);

}

void schedule(JobScheduler* js, JobQueueElem *job) {

    P(js->queue->empty);
    mtx_lock(&js->queue->queue_mtx);

    JobQueuePush(js->queue, job);

    mtx_unlock(&js->queue->queue_mtx);
    V(js->queue->full);

    //pthread_cond_signal(&threadCond);

    //return job->JobID;
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
    js->exitflag++;
    mtx_unlock(&js->scheduler_mtx);
}

void stop(JobScheduler* js){

    //pthread_cond_broadcast(&threadCond);

    V(js->queue->full);
    for(int i=0; i< (int) THREAD_NUM; i++){
        if(pthread_join(js->tp[i], NULL)!=0){
            perror("Thread no. %d failed to join.");
            return;
        }

    }

}


void* thread_start(void* argv){

    JobScheduler* js = (JobScheduler*) argv;

    while(1)
    {
        P(js->queue->full);

        mtx_lock(&js->scheduler_mtx);
        if (js->exitflag==2)
        {
            V(js->queue->full);
            mtx_unlock(&js->scheduler_mtx);
            break;
        }
        mtx_unlock(&js->scheduler_mtx);

        mtx_lock(&js->queue->queue_mtx);

        JobQueueElem *job = JobQueuePop(js->queue);

        mtx_unlock(&js->queue->queue_mtx);
        V(js->queue->empty);

        switch (job->jobType) {

            case 1:
                HistogramJob(js, job);
                break;

            case 2:
                PartitionJob(js, job);
                break;

            case 3:
                JoinJob(job);
                break;

            default:
                perror("Error. Wrong JobType.\n");
                return NULL;
        }


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

/**
 * Function to create the initial psums for each thread to use. Each thread copies their designated values
 * from the initial relations to the new ones.
 * @param js
 * @param buckets
 */
void makePsums(JobScheduler *js, int buckets) {

    js->thread_psums = malloc(2*sizeof(int**));         // Allocation. The size is 2 since 2 relations are being joined.
    js->thread_psums[0] = malloc((int) THREAD_NUM * sizeof(int*));  // Allocation. The psum table of each thread for the first relation.
    js->thread_psums[1] = malloc((int) THREAD_NUM * sizeof(int*));  // Allocation. The psum table of each thread for the second relation.

    for(int t=0; t<(int) THREAD_NUM; t++) {     // Allocation of the psums themselves - for each thread for each relation.
        js->thread_psums[0][t] = malloc(buckets*sizeof(int));
        js->thread_psums[1][t] = malloc(buckets*sizeof(int));
        for (int i = 0; i < buckets; i++) {
            js->thread_psums[0][t][i] = 0;
            js->thread_psums[1][t][i] = 0;
        }
    }


    for(int b=0; b<buckets; b++) {

        for (int t = 0; t < (int) THREAD_NUM; t++) {

            if(b == 0 && t == 0)        // If it is the first bucket of the first thread then leave to 0 and continue.
                continue;

            if(t != 0) {   // If it is not the initial thread of the table, then take the previous thread's psum and add to it the histogram value of the given bucket of the thread
                js->thread_psums[0][t][b] = js->thread_psums[0][t - 1][b] + js->thread_histograms_R[t-1][b];
                js->thread_psums[1][t][b] = js->thread_psums[1][t - 1][b] + js->thread_histograms_S[t-1][b];
            }
            else {  // If it is the initial thread of the table, do the same procedure, only with the last thread of the previous bucket.
                js->thread_psums[0][t][b] = js->thread_psums[0][THREAD_NUM-1][b - 1] + js->thread_histograms_R[THREAD_NUM-1][b-1];
                js->thread_psums[1][t][b] = js->thread_psums[1][THREAD_NUM-1][b - 1] + js->thread_histograms_S[THREAD_NUM-1][b-1];
            }

        }
    }


}

