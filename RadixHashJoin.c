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
    int* indexesR = splitRelation(relR);
    int* indexesS = splitRelation(relS);

    int buckets = power_of_2(suffix);

    //initialize histograms and psums
    int* histogramR = initializeHistogram(buckets);
    int* histogramS = initializeHistogram(buckets);
    int **histograms[2];
    histograms[0] = histogramR;
    histograms[1] = histogramS;
    int *psumR = NULL;
    int *psumS = NULL;
    int **psums = malloc(2* sizeof(int*));
    psums[0] = psumR;
    psums[1] = psumS;

    for(int i=0; i<(int) THREAD_NUM; i++){
        //for each relation piece 

        //save its bounds
        int start[2], end[2];
        start[0] = indexesR[i];
        start[1] = indexesS[i];

        if(i != ((int) THREAD_NUM - 1)){
            end[0] = indexesR[i+1];
            end[1] = indexesS[i+1];
        }
        else{
            end[0] = relR->num_tuples;
            end[1] = relS->num_tuples;
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

    relation* relation_R_new = malloc(sizeof(relation));
    relation* relation_S_new = malloc(sizeof(relation));
    relation_R_new->num_tuples = relR->num_tuples;
    relation_R_new->tuples = malloc(relation_R_new->num_tuples*sizeof(tuple));
    relation_S_new->num_tuples = relS->num_tuples;
    relation_S_new->tuples = malloc(relation_S_new->num_tuples*sizeof(tuple));

    relation** newRels = malloc(2*sizeof(relation*));
    newRels[0] = relation_R_new;
    newRels[1] = relation_S_new;

    for(int i=0; i<(int) THREAD_NUM; i++){

        int start[2], end[2];
        start[0] = indexesR[i];
        start[1] = indexesS[i];

        if(i != ((int) THREAD_NUM - 1)){
            end[0] = indexesR[i+1];
            end[1] = indexesS[i+1];
        }
        else{
            end[0] = relR->num_tuples;
            end[1] = relS->num_tuples;
        }


        JobQueueElem* job = JobCreate(jobIDCounter++, i, PART_TYPE, rels, buckets, start, end, NULL, psums, &hist_mtx, newRels, 0, NULL, NULL);

        schedule(js, job);
    }

    barrier(js);

    psumR = js->thread_psums[0][THREAD_NUM-1];      // The initial psum is actually the psum of the last thread.
    psumS = js->thread_psums[1][THREAD_NUM-1];

    psums[0] = psumR;
    psums[1] = psumS;

    //PHASE 3: Join
    resultsWithNum* res = create_resultsWithNum();

    for (int bucket_id = 0; bucket_id < buckets; bucket_id++)
    {
        //save its bounds
        int start[2], end[2];
        if(bucket_id == 0)
        {
            start[0] = 0;
            start[1] = 0;
        }
        else
        {
            start[0] = psumR[bucket_id-1];
            start[1] = psumS[bucket_id-1];
        }

        end[0] = psumR[bucket_id] -1;
        end[1] = psumS[bucket_id] -1;

        printf("BUCKET #%d - R:[%5d,%5d] -> %5d , S:[%5d,%5d] -> %5d\n", bucket_id, start[0], end[0], histograms[0][bucket_id], start[1], end[1], histograms[1][bucket_id]);

        JobQueueElem* job = JobCreate(jobIDCounter++, -1, JOIN_TYPE, rels, buckets, start, end, histograms, psums, &hist_mtx, newRels, bucket_id, res, &res_mtx);

        schedule(js, job);
    }

    barrier(js);

    stop(js);
/*
    //start of payload comparison between buckets of R and S

    //create index
    hash_index *indx = NULL;
    index_create(&indx, HASH2_RANGE);

    //create 2 temporary sets of relations, histograms and psums: x and y
    //y will be the relation whose bucket will be the smallest (and consequently indexed)
    //x will be the relation whose bucket will be the biggest

    relation *x = NULL;
    int *x_histogram = NULL;
    int *x_psum = NULL;

    relation *y = NULL;
    int *y_histogram = NULL;
    int *y_psum = NULL;

    int column_id;// == 1 if tuples(bucket(R)) > tuples(bucket(S)), == 2 otherwise

    //declare temporary position variables
    int pos = -1;
    int first_pos = -1;
    int last_pos = -1;



    //for each relation bucket
    for (int i = 0; i < power_of_2(suffix); i++)
    {
        //printf("BUCKET #%d\n", i);
        //compare size of bucket i of the relations R and S
        if (histogramR[i] > histogramS[i])
        {//bucket i of relation S is smaller; S will be hashed
            x = relation_R_new;
            x_histogram = histogramR;
            x_psum = psumR;

            y = relation_S_new;
            y_histogram = histogramS;
            y_psum = psumS;

            column_id = 1;
        }
        else
        {//bucket i of relation R is smaller; R will be hashed
            y = relation_R_new;
            y_histogram = histogramR;
            y_psum = psumR;

            x = relation_S_new;
            x_histogram = histogramS;
            x_psum = psumS;

            column_id = 2;
        }
        //fprintf(stderr, "BUCKET #%d\n",i);
        if(y_histogram[i] == 0)
            continue;

        //create index for bucket i of relation y
        index_fill(indx, y, y_histogram[i], y_psum[i]);
        
        //compare elements in bucket i of relation x to those on bucket i of relation y

        //assign boundaries of the bucket i of relation x
        first_pos = x_psum[i] - x_histogram[i]; //starting position of bucket i within relation x
        last_pos = x_psum[i] -1;  //ending position of bucket i within relation x
        pos = last_pos;     //variable for current position

        int bucket_start;
        if(i>0)
            bucket_start = y_psum[i-1];
        else
            bucket_start = 0;

        //values of tuple to be searched
        int32_t key;
        int32_t payload;

        while(pos >= first_pos)
        {
            key = x->tuples[pos].key;
            payload = x->tuples[pos].payload;

            search_val(y, bucket_start, indx, key, payload, column_id, res);
            
            pos--;
        }

    }

    index_destroy(&indx);
    */

    //print_results(results);

    //freeJobScheduler(js);

    free(psums);
    free(newRels);

    free(indexesR);
    free(indexesS);
    free(histogramR);
    free(histogramS);
    free(relation_R_new->tuples);
    free(relation_R_new);
    free(relation_S_new->tuples);
    free(relation_S_new);

    freeJobScheduler(js);

    return res;
}