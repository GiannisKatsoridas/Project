#ifndef _DATAPARSE_H_
#define _DATAPARSE_H_


#include "Tables.h"

int suffix;

relation* get_relation(char* name, int size);   // Gets the relation from the file "name", where the relation size is "size"


//given the table and the column in that table, the function below creates the relation needed for radix
//relation* get_relation_from_table(table *t, int columnID);	

int* create_histogram(relation* rel); // create the histogram of the relation by using
                                      // the suffix.

int power_of_2(int power);  // Simply calculates the nth power of 2

int* create_psum(int* histogram, int size);       // Create the accumulative histogram used to show the starting position of
                                                  // each bucket.

relation* create_relation_new(relation* relation, int* psum, int buckets);  // Create the new hashed relation.

void freeRelation(relation *rel);

#endif