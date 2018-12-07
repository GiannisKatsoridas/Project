#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Actions.h"

int main() {

    //table* t = loadRelation("r1");

    table** t = createTablesArray();

    parseTableData(t);

    printf("Done\n");

/*    printf("%d relations.\n", relationsNum);
    for (int i = 0; i < relationsNum; i++)
    {
        printf("%lu X %lu\n", t[i]->size, t[i]->columns_size);
        for(int j=0 ; j < t[i]->columns_size ; j++)
        {
            printf("[%d] : Min: %lu, Max: %lu, Distinct values: %d\n", j, 
                t[i]->metadata[j].min,
                t[i]->metadata[j].max,
                t[i]->metadata[j].distincts);
        }
        printf("\n");
    }*/

    freopen("queries.txt","r",stdin);

    Query* queries = getQueries();
    int queries_num;
    while(queries != NULL){
        queries_num = getQueriesNum();
        for(int k=0; k<queries_num; k++) {
            executeQuery(t, &(queries[k]));
        }

        freeQueries(queries);
        queries = getQueries();
    }


    freeTable(t);
    freeQueries(queries);

    return 0;
}
