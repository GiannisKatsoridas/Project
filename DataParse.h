#include "Globals.h"

void set_relation_R();      // Sets the global relation* as the first relation
void set_relation_S();      // Sets the global relation* as the second relation

int find_suffix();          // Returns the suffix needed for the RADIX algorithm
int find_suffix_R();        // Returns the suffix needed for the RADIX algorithm from the relation R
int find_suffix_S();        // Returns the suffix needed for the RADIX algorithm from the relation S