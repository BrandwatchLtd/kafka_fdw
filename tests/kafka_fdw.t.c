#include <stddef.h>
#include <tests/tap/basic.h>
#include "kafka_fdw.c"

void
test_estimate_costs(void);

int main(void) {
    plan(2);

    test_estimate_costs();

    return 0;
}

void test_estimate_costs(void) {
    Cost startup_cost;
    Cost total_cost;
    QualCost baserestrictcost;
    PlannerInfo root;
    RelOptInfo baserel;

    baserestrictcost.startup = 1;
    baserestrictcost.per_tuple = 1;

    baserel.pages = 1;
    baserel.tuples = 1;
    baserel.baserestrictcost = baserestrictcost;

    estimate_costs(&root, &baserel, &startup_cost, &total_cost);

    ok(startup_cost > 0, "Startup cost is greater than zero");
    ok(total_cost > 0, "Total cost is greater than zero");
}
