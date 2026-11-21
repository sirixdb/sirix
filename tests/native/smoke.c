#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libsirix_smoke.h"

static int no_jvm(void) {
    FILE *maps = fopen("/proc/self/maps", "r");
    if (!maps) return 0;
    char line[4096];
    int clean = 1;
    while (fgets(line, sizeof(line), maps)) {
        if (strstr(line, "libjvm")) clean = 0;
    }
    if (ferror(maps)) clean = 0;
    fclose(maps);
    return clean;
}

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc != 2 || getenv("JAVA_HOME") || !getenv("PATH") ||
        strcmp(getenv("PATH"), "/nonexistent") != 0) return 90;
    graal_isolate_t *isolate = NULL;
    graal_isolatethread_t *thread = NULL;
    if (graal_create_isolate(NULL, &isolate, &thread) != 0) return 91;
    int answer = smoke_core(thread, argv[1]);
    printf("core_answer=%d\n", answer);
#ifdef QUERY_SMOKE
    int query_answer = answer == 42 ? smoke_query(thread, argv[1]) : -1;
    printf("query_answer=%d\n", query_answer);
    if (query_answer != 42) answer = -1;
#endif
    int clean = no_jvm();
    printf("libjvm_loaded=%d\n", !clean);
#ifndef OMIT_SHUTDOWN
    int shutdown = smoke_shutdown(thread);
    printf("snapshot_shutdown=%d\n", shutdown);
    if (shutdown != 0) answer = -1;
#endif
    smoke_threads(thread);
    puts("teardown_begin");
    int teardown = graal_tear_down_isolate(thread);
    printf("teardown_result=%d\n", teardown);
    return answer == 42 && clean && teardown == 0 ? 0 : 92;
}
