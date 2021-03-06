//
// Created by kats on 10/11/2018.
//
#ifndef _TABLES_H_
#define _TABLES_H_

#include "Query.h"
#include "Results.h"

#define MAX_TABLE_RANGE 500000000               // The maximum range between max and min values of a table for which
                                                // the distinct values will be found.

int relationsNum;

typedef struct tableMetadata {
    uint64_t min;
    uint64_t max;
    int distincts;
    int size;
} Metadata;//metadata FOR EACH COLUMN OF THE TABLE

typedef struct Table {
	/// The number of tuples
    uint64_t size;
  	/// The number of columns
    uint64_t columns_size;
    /// The join column containing the keys
    uint64_t** columns;

    Metadata* metadata;//metadata FOR EACH COLUMN OF THE TABLE

	int tableID;//table ID in database
    //struct IntermediateResults *inRes;//contains all keys (rowIDs) needed for the next join/comparison
} table;






/**
 * Loads the relation from the binary file "filename" and using mmap translates it to
 * a table* object to be used. Simply translated the SIGMOD C++ version to a C one.
 * @param fileName: The binary file of the relation
 * @return the table* object that contains the relation and pointers to its metadata and data
 */
table* loadRelation(const char* fileName);

/**
 * Loops all the files given as input from stdin and create an array of pointers to table*
 * objects that represent all the relations
 * @return the table** object containing all the relations
 */
table** createTablesArray();

/**
 * Parses each relation from the table** array of relations and for each one finds the min and max value
 * as well as the total number of distinct values. Then fills their Metadata* field
 * @param t: the table** object
 */
void parseTableData(table** t);

/**
 * For a certain column of a certain relation of the array of relations t, finds the min and max value and inserts
 * them into the corresponding metadata object
 * @param t: the array of relations
 * @param i: the index of the relation in use
 * @param j: the index of the column in use
 */
void findMinMax(table** t, int i, int j);

/**
 * For a certain column of a certain relation of the array of relations t, finds the total number of distinct values
 * and inserts them into the corresponding metadata object
 * @param t: the array of relations
 * @param i: the index of the relation in use
 * @param j: the index oof the column in use
 */
void findAllDistincts(table** t, int i, int j);

/**
 * Frees the table struct given as an argument.
 * @param t: the array of relations to be freed.
 */
void freeTable(table** t);




//void saveTableKeysFromResult(table *t, result *results, int resultColumn);
//void saveResult(table *t1, table *t2, result *results);

//relation *constructRelationForNextJoin(table *t, int columnID);
#endif
