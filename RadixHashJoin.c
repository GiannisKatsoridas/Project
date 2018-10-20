#include <stdio.h>
#include <stdlib.h>
#include "DataParse.h"

result* RadixHashJoin(relation* relR, relation* relS){

    int* histogramR = create_histogram(relR, 1, 1);    // Creates the histogram of the relation R
    int* histogramS = create_histogram(relS, 1, 2);    // Creates the histogram of the relation S

    int* psumR = create_psum(histogramR, power_of_2(suffix_R));     // Creates the accumulative histogram of the relation R
    int* psumS = create_psum(histogramS, power_of_2(suffix_S));     // Creates the accumulative histogram of the relation S

    relation* relation_R_new = create_relation_new(relR, psumR, power_of_2(suffix_R));    // Create the new relation
                                                                                                // used for the Join
    relation* relation_S_new = create_relation_new(relS, psumS, power_of_2(suffix_S));    // Create the new relation
                                                                                                // used for the Join

    free(histogramR);
    free(histogramS);
    free(relation_R_new->tuples);
    free(relation_R_new);
    free(relation_S_new->tuples);
    free(relation_S_new);
    free(psumR);
    free(psumS);


    return NULL;
}