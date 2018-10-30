#include <stdio.h>
#include <stdlib.h>
#include "Results.h"

result* RadixHashJoin(relation* relR, relation* relS){
    printf("Relation R before hashing:\n");
    print_relation(relR, stdout);
    printf("Relation S before hashing:\n");
    print_relation(relS, stdout);
    int* histogramR = create_histogram(relR, 1, 1);    // Creates the histogram of the relation R
    int* histogramS = create_histogram(relS, suffix_R, 2);    // Creates the histogram of the relation S

    /**
     * The two histograms must have the same size. To do so, the histogram S starts with
     * R's suffix as the base and if it needs a bigger suffix then R's histogram gets recreated
     * with the same suffix as S.
     */

    if(suffix_S > suffix_R){
        free(histogramR);
        histogramR = create_histogram(relR, suffix_S, 1);
    }

    int* psumR = create_psum(histogramR, power_of_2(suffix_R));     // Creates the accumulative histogram of the relation R
    int* psumS = create_psum(histogramS, power_of_2(suffix_S));     // Creates the accumulative histogram of the relation S

    relation* relation_R_new = create_relation_new(relR, psumR, power_of_2(suffix_R));    // Create the new relation
                                                                                          // used for the Join
    relation* relation_S_new = create_relation_new(relS, psumS, power_of_2(suffix_S));    // Create the new relation
                                                                                          // used for the Join

    printf("Relation R after hashing:\n");
    print_relation(relation_R_new, stdout);
    printf("Relation S after hashing:\n");
    print_relation(relation_S_new, stdout);
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

    //print_results(results, results_num);

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