//
// Created by kats on 10/11/2018.
//

#include <stdint.h>

int relationsNum;

typedef struct Table {
    uint64_t size;
    uint64_t columns_size;
    uint64_t** columns;
} table;

table* loadRelation(const char* fileName);      // The function to load the relations from the files.
                                                // Simply translated the SIGMOD C++ version into a C one.

table** createTablesArray();                    // Create the table with the relations and the metadata

void freeTable(table** t);                      // Free above table
