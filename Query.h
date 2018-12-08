//
// Created by kats on 23/11/2018.
//

#ifndef _QUERY_H
#define _QUERY_H

#include "Globals.h"

#define JOIN 0
#define LESS_THAN 1
#define GREATER_THAN 2
#define EQUAL 3

//action priorities defined; the lower the better
#define EQ_PRIORITY 0               //priority of equal comparison is 0
#define JOIN_SAME_REL_PRIORITY 1    //priority of join between columns of the same relation is 1
#define DIFF_THAN_PRIORITY 2        //priority of less-than or greater-than comparisons is 2
#define JOIN_PRIORITY 3             //priority of radix-join is 3

int queries_num;

typedef struct column {

    int relation;
    int column;

} Column;

typedef struct comparison {

    int relationA;
    int columnA;
    int relationB;
    int columnB;    // If the action is not = then 'relationB' holds the comparison value and columnB is -1
    int action;     // 0 = Equals, 1 = Less than, 2 = Greater than

    int priority;   //the lower the priority number, the quicker the action will perform
} Comparison;

typedef struct query_relations_t {

    int query_relations_num;
    int* query_relations;

} Query_Relation_t;

typedef struct comparison_t {

    int comparisons_num;
    Comparison* comparisons;

} Comparison_t;

typedef struct columns_t {

    int columns_num;
    Column* columns;

} Column_t;

typedef struct query {

    Query_Relation_t* query_relation_set;//relation tables that will be used in this query ("FROM")
    Comparison_t* comparison_set;   //query predicates ("WHERE")
    Column_t* column_set;       //columns that will ("SELECT")

} Query;




/**
 * Get all the queries from stdin and returns then in an array of Query type objects
 * @return The queries
 */
Query* getQueries();
/**
 * Given the query as a string, creates the Query object
 * @param line: The Query as a string
 * @param length: The length of the string - for calculating purposes
 * @return The Query object
 */
Query getQueryFromLine(char* line, size_t length);
/**
 * Given the columns part of the query, returns an array of Column objects, corresponding to the columns
 * @param line: the column part of the query
 * @return The array of columns
 */
Column_t* getColumns(char* line);
/**
 * Given the relations part of the query, returns an array of integers, corresponding to the relations
 * @param line: the relation part of the query
 * @return The array of relations
 */
Query_Relation_t* getQueryRelations(char* line);
/**
 * Given the comparisons part of the query, returns an array of Comparison objects, corresponding to the comparisons
 * @param line: the comparison part of the query
 * @return The array of comparisons
 */
Comparison_t* getComparisons(char* line);
/**
 * Given a specific comparison, eg "1.2=3.2" returns a Comparison object with this specific comparison
 * @param line: the comparison
 * @return The comparison object
 */
Comparison getComparisonFromQuery(char* line);
/**
 * Given a specific column, eg "1.2" returns a Column object with this specific column
 * @param line: the column
 * @return The column object
 */
Column getColumnsFromQuery(char *line);
/**
 * Frees the Query struct
 * @param queries: the struct
 */
void freeQueries(Query* queries);


int getQueriesNum();


#endif