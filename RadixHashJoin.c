#include <stdio.h>
#include <stdlib.h>
#include "Results.h"

#define HASH2_RANGE 10

int hash2(int32_t payload)
{
    int index =( payload >> RADIX_N ) % HASH2_RANGE;
    //printf("%d -> %d\n", payload, index);
    return index;
}

result* RadixHashJoin(relation* relR, relation* relS){

    suffix = RADIX_N;
    printf("Relation R before hashing:\n");
    print_relation(relR, stdout);
    printf("Relation S before hashing:\n");
    print_relation(relS, stdout);

    int* histogramR = create_histogram(relR);    // Creates the histogram of the relation R
    int* histogramS = create_histogram(relS);    // Creates the histogram of the relation S


    int* psumR = create_psum(histogramR, power_of_2(suffix));     // Creates the accumulative histogram of the relation R
    int* psumS = create_psum(histogramS, power_of_2(suffix));     // Creates the accumulative histogram of the relation S

    relation* relation_R_new = create_relation_new(relR, psumR, power_of_2(suffix));    // Create the new relation
                                                                                          // used for the Join
    relation* relation_S_new = create_relation_new(relS, psumS, power_of_2(suffix));    // Create the new relation
                                                                                          // used for the Join

    printf("Relation R after hashing:\n");
    print_relation(relation_R_new, stdout);
    printf("Relation S after hashing:\n");
    print_relation(relation_S_new, stdout);


    result* results = create_results_page();
    int results_num=0;

    //start of payload comparison between buckets of R and S

    //create bucket array with size equal to HASH2_RANGE
    int *bucket_array = malloc(HASH2_RANGE * sizeof(int));

    //declare chain array
    int *chain = NULL;

    //create 2 temporary sets of relations, histograms and psums: x and y
    //y will be the relation whose bucket will be the smallest
    //x will be the relation whose bucket will be the biggest

    relation *x = NULL;
    int *x_histogram = NULL;
    int *x_psum = NULL;

    relation *y = NULL;
    int *y_histogram = NULL;
    int *y_psum = NULL;

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
       // printf("BUCKET #%d\n", i);
        //compare size of bucket i of the relations R and S
        if (histogramR[i] > histogramS[i])
        {//bucket i of relation S is smaller; S will be hashed
            x = relation_R_new;
            x_histogram = histogramR;
            x_psum = psumR;

            y = relation_S_new;
            y_histogram = histogramS;
            y_psum = psumS;
        }
        else
        {//bucket i of relation R is smaller; R will be hashed
            y = relation_R_new;
            y_histogram = histogramR;
            y_psum = psumR;

            x = relation_S_new;
            x_histogram = histogramS;
            x_psum = psumS;
        }
        //fprintf(stderr, "BUCKET #%d\n",i);
        if(y_histogram[i] == 0)
            continue;
        //initialize bucket array
        for (int j = 0; j < HASH2_RANGE; j++)
        {
            bucket_array[j] = -1;
        }

        //now the bucket i of the relation y is the one that will be hashed
        //create the chain array
        chain = malloc(y_histogram[i] * sizeof(int));
        for (int j = 0; j < y_histogram[i]; j++)
        {
            chain[j] = -1;
        }

        
        //assign boundaries of the bucket i of relation y
        first_pos = y_psum[i] - y_histogram[i]; //starting position of bucket i within relation y
        last_pos = y_psum[i] -1;  //ending position of bucket i within relation y
        pos = last_pos;     //variable for current position
        //printf("y: bucket[%d]: [%d, %d]\n", i, first_pos, last_pos);
        while(pos >= first_pos)
        {//accessing bucket elements from last to first; insert their position to the index chain 
            //printf("y->pos = %d..\n", pos);
            int n = hash2(y->tuples[pos].payload);
            if (bucket_array[n]== -1)
            {
                bucket_array[n] = pos - first_pos;
            }
            else
            {
                int temp_pos = bucket_array[n];
                while(chain[temp_pos] != -1)
                {
                    temp_pos = chain[temp_pos];
                }
                chain[temp_pos] = pos - first_pos;
            }
            pos--;
        }
        
        /*fprintf(stdout, "\nBUCKET ARRAY\n");
        for (int j = 0; j < HASH2_RANGE; j++)
        {
            fprintf(stdout, "bucket[%02d] = %02d\n", j, bucket_array[j]);
        }
        fprintf(stdout, "\nCHAIN ARRAY\n");
        for (int j = 0; j < y_histogram[i]; j++)
        {
            fprintf(stdout, "chain[%02d] = %02d\n", j, chain[j]);
        }
        fprintf(stdout, "\n");*/
        //compare elements in bucket i of relation x to those on bucket i of relation y

        //assign boundaries of the bucket i of relation x
        first_pos = x_psum[i] - x_histogram[i]; //starting position of bucket i within relation x
        last_pos = x_psum[i] -1;  //ending position of bucket i within relation x
        pos = last_pos;     //variable for current position

        while(pos >= first_pos)
        {
            //printf("x->pos = %d..\n", pos);
            int n = hash2(x->tuples[pos].payload);
            //printf("BUCKET #%d -> %d\n",i, n );
            if (bucket_array[n]!= -1)
            {
                int curr_pos = bucket_array[n];
                int real_pos = curr_pos ;
                if(i>0)
                    real_pos = real_pos + y_psum[i-1];
                
                /*fprintf(stdout, "compairing (x[%d], y[%d]) = (xbucket[%d], ybucket[%d]) = (%d, %d)\n",
                 pos, real_pos,
                 pos- first_pos, curr_pos,
                 x->tuples[pos].payload, y->tuples[real_pos].payload);*/
                
                if (x->tuples[pos].payload == y->tuples[real_pos].payload)
                {
                    //fprintf(stdout, "equals!\n");
                    if (histogramR[i] > histogramS[i])
                        results_num = add_result(results, x->tuples[pos].key, y->tuples[real_pos].key);
                    else
                        results_num = add_result(results, y->tuples[real_pos].key, x->tuples[pos].key);                                        
                }
                while(chain[curr_pos] != -1)
                {
                    curr_pos = chain[curr_pos];
                    real_pos = curr_pos ;
                    if(i>0)
                        real_pos = real_pos + y_psum[i-1];
                    
                    /*fprintf(stdout, "compairing (x[%d], y[%d]) = (xbucket[%d], ybucket[%d]) = (%d, %d)\n",
                     pos, real_pos,
                     pos- first_pos, curr_pos,
                     x->tuples[pos].payload, y->tuples[real_pos].payload);*/
                    
                    if (x->tuples[pos].payload == y->tuples[real_pos].payload)
                    {
                        //fprintf(stdout, "equals!\n");
                        if (histogramR[i] > histogramS[i])
                            results_num = add_result(results, x->tuples[pos].key, y->tuples[real_pos].key);
                        else
                            results_num = add_result(results, y->tuples[real_pos].key, x->tuples[pos].key);                                        
                    }
                }

            }

            pos--;
        }

        free(chain);
        chain = NULL;
    }

    free(bucket_array);

    

    /*for(int i=0; i<20; i++){

        results_num = add_result(results, i+1, i-1);

    }*/
    

    print_results(results, results_num);

    free(histogramR);
    free(histogramS);
    free(relation_R_new->tuples);
    free(relation_R_new);
    free(relation_S_new->tuples);
    free(relation_S_new);
    free(psumR);
    free(psumS);


    return results;
}