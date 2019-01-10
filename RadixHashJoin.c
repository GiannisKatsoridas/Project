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

    for(int i=0; i<(int) THREAD_NUM; i++)
    {
        pthread_create(&js->tp[i], NULL, thread_start, js);
    }

    suffix = RADIX_N;

    int* indexesR = splitRelation(relR);
    int* indexesS = splitRelation(relS);

    int buckets = power_of_2(suffix);

    int* histogramR = initializeHistogram(buckets);
    int* histogramS = initializeHistogram(buckets);
    int *histograms[2];
    histograms[0] = histogramR;
    histograms[1] = histogramS;
    int* psumR = initializeHistogram(buckets);
    int* psumS = initializeHistogram(buckets);
    int *psums[2];
    psums[0] = psumR;
    psums[1] = psumS;


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

        JobQueueElem* job = JobCreate(jobIDCounter++, HIST_TYPE, rels, power_of_2(suffix), start, end, histograms, psums, &hist_mtx, NULL, 0, NULL, NULL);

        schedule(js, job);
        //printf("%d\n", i);

    }

    barrier(js);

    stop(js);
    
    //printf("1\n");

    jobSchedulerDestroy(js);



//    int* histogramR = create_histogram(relR);    // Creates the histogram of the relation R
//    int* histogramS = create_histogram(relS);    // Creates the histogram of the relation S

//    int* psumR = create_psum(histogramR, power_of_2(suffix));     // Creates the accumulative histogram of the relation R
//    int* psumS = create_psum(histogramS, power_of_2(suffix));     // Creates the accumulative histogram of the relation S

    relation* relation_R_new = create_relation_new(relR, psumR, power_of_2(suffix));    // Create the new relation
                                                                                          // used for the Join
    relation* relation_S_new = create_relation_new(relS, psumS, power_of_2(suffix));    // Create the new relation
                                                                                          // used for the Join

    /*printf("Relation R after hashing:\n");
    print_relation(relation_R_new, stdout);
    printf("Relation S after hashing:\n");
    print_relation(relation_S_new, stdout);*/


    resultsWithNum* res = create_resultsWithNum();

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

    /*fprintf(stdout, "\nHISTOGRAMS\n");
    for (int j = 0; j < power_of_2(suffix); j++)
    {
        fprintf(stdout, "histR[%2d] = %2d\t", j, histogramR[j]);

        fprintf(stdout, "histS[%2d] = %2d\n", j, histogramS[j]);
    }
    fprintf(stdout, "\nPSUMS\n");
    for (int j = 0; j < power_of_2(suffix); j++)
    {
        fprintf(stdout, "psumR[%2d] = %2d\t", j, psumR[j]);

        fprintf(stdout, "psums[%2d] = %2d\n", j, psumS[j]);
    }*/
    
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
    

    //print_results(results);

    free(histogramR);
    free(histogramS);
    free(relation_R_new->tuples);
    free(relation_R_new);
    free(relation_S_new->tuples);
    free(relation_S_new);
    free(psumR);
    free(psumS);

    return res;
}