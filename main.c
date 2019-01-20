#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Actions.h"

int main() {

    setTuplesPerPage();
    //table* t = loadRelation("r1");

    //freopen("files_and_queries.txt", "r", stdin);

    table** t = createTablesArray();

    parseTableData(t);

/*    relation* r1 = createRelationFromTable(t[0], 0);
    relation* r2 = createRelationFromTable(t[1], 0);
    resultsWithNum* res = RadixHashJoin(r1, r2);

    uint64_t sum = 0;

    result* pointer = res->results;

    int tuples_printed = 0;

    while(pointer!=NULL){

        for(int i=0; (i<tuples_per_page) && (tuples_printed<res->results_amount); i++){

            sum += t[0]->columns[0][res->results->results[i].relation_R];
            tuples_printed++;
        }

        pointer = pointer->next;

    }

    fprintf(stderr, "Result Amount: %d, Sum: %lu\n", res->results_amount, sum);*/

    //printf("Done\n");

    //freopen("queries.txt","r",stdin);

    Query* queries = getQueries();
    int queries_num;
    while(queries != NULL){
        //fprintf(stderr, "Reading batch...\n");
        queries_num = getQueriesNum();
        for(int k=0; k<queries_num; k++) {
            executeQuery(t, &(queries[k]));
        }

        freeQueries(queries);
        //fprintf(stderr, "Ready to read another batch...\n");
        queries = getQueries();
    }


    freeTable(t);
    freeQueries(queries);

 /*   relation* rel1 = get_relation((char *)relR_name, RELR_SIZE);
    relation* rel2 = get_relation((char *)relS_name, RELS_SIZE);

    relation* rel3 = get_relation((char *)relR_name, RELR_SIZE);
    relation* rel4 = get_relation((char *)relS_name, RELS_SIZE);

    resultsWithNum* res1 = RadixHashJoin(rel1, rel2);
    resultsWithNum* res2 = RadixHashJoin(rel3, rel4);

    concatResults(res1, res2);

    print_resultsWithNum(res1);
*/

    return 0;
}
