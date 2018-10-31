#include <stdio.h>
#include <stdlib.h>
#include "Results.h"

result* RadixHashJoin(relation* relR, relation* relS){

    suffix = RADIX_N;

    int* histogramR = create_histogram(relR);    // Creates the histogram of the relation R
    int* histogramS = create_histogram(relS);    // Creates the histogram of the relation S


    int* psumR = create_psum(histogramR, power_of_2(suffix));     // Creates the accumulative histogram of the relation R
    int* psumS = create_psum(histogramS, power_of_2(suffix));     // Creates the accumulative histogram of the relation S

    relation* relation_R_new = create_relation_new(relR, psumR, power_of_2(suffix));    // Create the new relation
                                                                                          // used for the Join
    relation* relation_S_new = create_relation_new(relS, psumS, power_of_2(suffix));    // Create the new relation
                                                                                          // used for the Join


    /**
     * Example: create a results list with random numbers.
     */

    result* results = create_results_page();
    int results_num=0;

    for(int i=0; i<20; i++){

        results_num = add_result(results, i+1, i-1);

    }
    
    /**
     * End of example
     */

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