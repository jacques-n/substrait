// Compile with:
// $ clang++/g++ -I./lib/build/native/nativeCompile -L./lib/build/native/nativeCompile example.cpp -listhmus-native -o example

// Run with:
// $ LD_LIBRARY_PATH=$(pwd)/lib/build/native/nativeCompile ./example

#include "libisthmus-native.h"
#include "stdio.h"

int main(int argc, char** argv)
{
    graal_isolate_t* isolate = NULL;
    graal_isolatethread_t* thread = NULL;

    if (graal_create_isolate(NULL, &isolate, &thread) != 0) {
        fprintf(stderr, "graal_create_isolate error\n");
        return 1;
    }

    char* table = "CREATE TABLE T1(foo int, bar bigint)";
    char* query = "SELECT foo FROM T1";
    char* plan = getCalcitePlan(thread, query, table);

    printf("Got plan: \n");
    printf("%s\n", plan);

    if (graal_detach_thread(thread) != 0) {
        fprintf(stderr, "graal_detach_thread error\n");
        return 1;
    }

    return 0;
}
