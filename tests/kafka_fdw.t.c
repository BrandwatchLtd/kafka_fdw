#include <stddef.h>
#include <tests/tap/basic.h>
#include "kafka_fdw.c"

int main(void) {
    plan(4);

    ok(1, "the first test");
    is_int(42, 42, NULL);
    diag("a diagnostic, ignored by the harness");
    ok(0, "a failing test");
    skip("a skipped test");

    return 0;
}

void test_estimate_costs() {
    Cost startup_cost;
    Cost total_cost;
}
