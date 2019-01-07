#ifndef _RESULTS_H_
#define _RESULTS_H_

#include "DataParse.h"

//static int const page_size = 1048576;
static int const page_size = 500;
static int tuples_per_page = 10;


void setTuplesPerPage();

int getResultTuplesPerPage();

//result* create_results_page();      // Creates a new results page to add new results tuples, if the one already
                                    // there is already full.

resultsWithNum* create_resultsWithNum();

void add_result(resultsWithNum* res, int32_t value_R, int32_t value_S);     // Adds the values given into the results list.
                                                                   // Returns the current number of results.

void print_results(result *res);   // Prints the results from the given list.


void print_relation(relation *rel, FILE *fp);

unsigned int int_to_int(unsigned int k);

void freeResultsWithNum(resultsWithNum* results);

void freeResults(result* results);



void concatResults(resultsWithNum* res1, resultsWithNum* res2);       // Add all results from res2 to res1.

#endif