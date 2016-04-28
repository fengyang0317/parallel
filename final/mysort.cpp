#include <iostream>
#include <sys/time.h>
#include <stdio.h>
#include "mpi.h"
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string>
#include <string.h>
#include <sys/mman.h>
#include <algorithm>
using namespace std;

int numtasks, rank;

#define handle_error(msg) \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

struct mykey {
    char v[10];
    long long ix;
    bool operator <(const mykey &b) const {
        return strcmp(v, b.v) < 0;
    }
};

struct myrow {
    char v[100];
    int ix;
    bool operator <(const myrow &b) const {
        return strcmp(v, b.v) > 0;
    }
};

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);
    cout << rank << " is on " << processor_name << endl;

    long long i;
    int fd;
    char *addr;
    long long len;
    long long *sizebuf;
    if (rank) {
        struct stat sb;
        fd = open("/localdisk/yfeng23/part", O_RDONLY);
        if (fd == -1)
            handle_error("open");

        if (fstat(fd, &sb) == -1)
            handle_error("fstat");

        len = sb.st_size;
        len /= 100;
        MPI_Gather(&len, 1, MPI_LONG_LONG, sizebuf, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
        cout << "process " << rank << " " << len << endl;
        mykey *key = new mykey[len];

        addr = (char*)mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED)
            handle_error("mmap");
        for (i = 0; i < len; i++) {
            key[i].ix = i;
            memcpy(key[i].v, addr + 100 * i, 10);
        }

        sort(key, key + len);
        for (i = 0; i < len; i++) {
            MPI_Send(addr + key[i].ix * 100, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }
        munmap(addr, sb.st_size);
        close(fd);
    }
    else {
        struct timeval start;
        struct timeval finish;
        gettimeofday(&start, 0);
        sizebuf = new long long[numtasks];
        MPI_Gather(&len, 1, MPI_LONG_LONG, sizebuf, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
        long long total = 0;
        myrow buf[numtasks];
        MPI_Status stat;
        for (int j = 1; j < numtasks; j++) {
            total += sizebuf[j];
            MPI_Recv(buf[j].v, 100, MPI_CHAR, j, 0, MPI_COMM_WORLD, &stat);
            buf[j].ix = j;
            sizebuf[j]--;
        }
        cout << "total " << total << endl;
        fd = open("/localdisk/yfeng23/res", O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
        lseek(fd, total * 100 - 1, SEEK_SET);
        write(fd, "", 1);
        addr = (char*)mmap(NULL, total * 100, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        make_heap(buf + 1, buf + numtasks);
        for (i = 0; i < total; i++) {
            //printf("%d %d\t%d\n",sizebuf[1], sizebuf[2], buf[1].ix);
            memcpy(addr + i * 100, buf[1].v, 100);
            pop_heap(buf + 1, buf + numtasks);
            if (sizebuf[buf[numtasks - 1].ix]) {
                MPI_Recv(buf[numtasks - 1].v, 100, MPI_CHAR, buf[numtasks - 1].ix, 0, MPI_COMM_WORLD, &stat);
                sizebuf[buf[numtasks - 1].ix]--;
                push_heap(buf + 1, buf + numtasks);
            }
            else {
                numtasks--;
            }
            if (i % (total/10) == 0) {
                puts("10\% finished");
            }
        }
        munmap(addr, total * 100);
        close(fd);
        gettimeofday(&finish, 0);
        cout << finish.tv_sec - start.tv_sec << endl;
    }

    MPI_Finalize();
    return 0;
}
