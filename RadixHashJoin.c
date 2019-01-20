#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include "Index.h"
#include "JobScheduler.h"

resultsWithNum* RadixHashJoin(relation* relR, relation* relS){

    int jobIDCounter = 0;
    relation* rels[2];
    rels[0] = relR;
    rels[1] = relS;

    JobScheduler* js = jobSchedulerCreate();

    pthread_mutex_t hist_mtx;
    mtx_init(&hist_mtx);
    pthread_mutex_t res_mtx;
    mtx_init(&res_mtx);

    //create threadpool
    for(int i=0; i<(int) THREAD_NUM; i++)
    {
        pthread_create(&js->tp[i], NULL, thread_start, js);
    }

    suffix = RADIX_N;

    //PHASE 1 : creating histogram


    //split relation into THREAD_NUM piece
    //save start of each piece
    int *indexes[2];
    indexes[R] = splitRelation(relR);
    indexes[S] = splitRelation(relS);

    int buckets = power_of_2(suffix);

    //initialize histograms and psums
    int *histograms[2];
    histograms[R] = initializeHistogram(buckets);
    histograms[S] = initializeHistogram(buckets);


    int *psums[2];
    psums[R] = NULL;
    psums[S] = NULL;

    for(int i=0; i<(int) THREAD_NUM; i++){
        //for each relation piece 

        //save its bounds
        int start[2], end[2];
        start[R] = indexes[R][i];
        start[S] = indexes[S][i];

        if(i != ((int) THREAD_NUM - 1)){
            end[R] = indexes[R][i+1];
            end[S] = indexes[S][i+1];
        }
        else{
            end[R] = relR->num_tuples;
            end[S] = relS->num_tuples;
        }

        //create a HistogramJob
        JobQueueElem* job = JobCreate(jobIDCounter++, i, HIST_TYPE, rels, buckets, start, end, histograms, psums, &hist_mtx, NULL, 0, NULL, NULL);

        //push the job in the Job Queue
        schedule(js, job);

    }

    //wait for all HistogramJobs to be completed
    barrier(js);

    // PHASE 2: Partitioning
    makePsums(js, buckets);

    // Initialize the new relations.

    relation* newRels[2];
    newRels[R] = malloc(sizeof(relation));
    newRels[S] = malloc(sizeof(relation));

    newRels[R]->num_tuples = relR->num_tuples;
    newRels[R]->tuples = malloc(newRels[R]->num_tuples*sizeof(tuple));

    newRels[S]->num_tuples = relS->num_tuples;
    newRels[S]->tuples = malloc(newRels[S]->num_tuples*sizeof(tuple));

    for(int i=0; i<(int) THREAD_NUM; i++){

        int start[2], end[2];
        start[R] = indexes[R][i];
        start[S] = indexes[S][i];

        if(i != ((int) THREAD_NUM - 1)){
            end[R] = indexes[R][i+1];
            end[S] = indexes[S][i+1];
        }
        else{
            end[R] = relR->num_tuples;
            end[S] = relS->num_tuples;
        }

        JobQueueElem* job = JobCreate(jobIDCounter++, i, PART_TYPE, rels, buckets, start, end, psums, NULL, &hist_mtx, newRels, 0, NULL, NULL);

        schedule(js, job);
    }

    barrier(js);

    psums[R] = js->thread_psums[R][THREAD_NUM-1];      // The initial psum is actually the psum of the last thread.
    psums[S] = js->thread_psums[S][THREAD_NUM-1];

    //PHASE 3: Join
    resultsWithNum* res = create_resultsWithNum();

    for (int bucket_id = 0; bucket_id < buckets; bucket_id++)
    {
        //save its bounds
        int start[2], end[2];
        if(bucket_id == 0)
        {
            start[R] = 0;
            start[S] = 0;
        }
        else
        {
            start[R] = psums[R][bucket_id-1];
            start[S] = psums[S][bucket_id-1];
        }

        end[R] = psums[R][bucket_id] -1;
        end[S] = psums[S][bucket_id] -1;

        JobQueueElem* job = JobCreate(jobIDCounter++, -1, JOIN_TYPE, rels, buckets, start, end, histograms, psums, &hist_mtx, newRels, bucket_id, res, &res_mtx);

        schedule(js, job);
    }

    barrier(js);

    stop(js);

    free(indexes[R]);
    free(indexes[S]);
    free(histograms[R]);
    free(histograms[S]);
    free(newRels[R]->tuples);
    free(newRels[R]);
    free(newRels[S]->tuples);
    free(newRels[S]);

    freeJobScheduler(js);

    return res;
}