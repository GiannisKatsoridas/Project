#ifndef _RESULTS_H_
#define _RESULTS_H_

#include "DataParse.h"

//static int const page_size = 1048576;
static int const page_size = 50;

int tuples_per_page;


result* create_results_page();      // Creates a new results page to add new results tuples, if the one already
                                    // there is already full.

int add_result(result* res, int32_t value_R, int32_t value_S);     // Adds the values given into the results list.
                                                                   // Returns the current number of results.

void print_results(result *res, int results_num);   // Prints the results from the given list.


void print_relation(relation *rel, FILE *fp);

unsigned int int_to_int(unsigned int k);

void freeResults(result* results);

#endif