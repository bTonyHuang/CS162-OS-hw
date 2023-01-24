#include <stdio.h>
#include <sys/resource.h>

int main() {
    struct rlimit lim;
    long stackSize=0;
    long proLimit=0;
    long maxFile=0;

    if(!getrlimit(RLIMIT_STACK,&lim))
        stackSize=lim.rlim_cur;
    else{
        printf("function failed\n");
        return 1;
    }

    if(!getrlimit(RLIMIT_NPROC,&lim))
        proLimit=lim.rlim_cur;
    else{
        printf("function failed\n");
        return 1;
    }

    if(!getrlimit(RLIMIT_NOFILE,&lim))
        maxFile=lim.rlim_cur;
    else{
        printf("function failed\n");
        return 1;
    }

    printf("stack size: %ld\n", stackSize);
    printf("process limit: %ld\n", proLimit);
    printf("max file descriptors: %ld\n", maxFile);
    return 0;
}
