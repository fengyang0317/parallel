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

#define BUF 1000
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
    char buf[BUF + BUF][100];
    //char (*buf)[100];
    int p;
    int ix;
    bool operator <(const myrow &b) const {
        return strcmp(buf[p], b.buf[p]) > 0;
    }
};

bool lt(const myrow *a, const myrow *b) {
    return strcmp(a->buf[a->p], b->buf[b->p]) > 0;
}

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

        int td = open("/localdisk/yfeng23/tmp", O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
        lseek(td, sb.st_size - 1, SEEK_SET);
        write(td, "", 1);
        char *taddr;
        taddr = (char*)mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, td, 0);

        for (i = 0; i < len; i++) {
            memcpy(taddr + i * 100, addr + key[i].ix * 100, 100);
        }
        munmap(addr, sb.st_size);
        close(fd);
        for (i = 0; i < len; i+=BUF) {
            MPI_Send(taddr + i * 100, 100 * BUF, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }
        munmap(taddr, sb.st_size);
        close(td);
    }
    else {
        struct timeval start;
        struct timeval finish;
        gettimeofday(&start, 0);
        sizebuf = new long long[numtasks];
        MPI_Gather(&len, 1, MPI_LONG_LONG, sizebuf, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
        long long total = 0;
        myrow row[numtasks];
        myrow *prow[numtasks];
        MPI_Status stat;
        MPI_Request reqs[numtasks];
        for (int j = 1; j < numtasks; j++) {
            total += sizebuf[j];
            //row[j].buf = new char[BUF + BUF][100];
            MPI_Recv(row[j].buf, 100 * BUF, MPI_CHAR, j, 0, MPI_COMM_WORLD, &stat);
            //MPI_Irecv(row[j].buf, 100 * BUF, MPI_CHAR, j, 0, MPI_COMM_WORLD, &reqs[j]);
            //MPI_Wait(&reqs[j], &stat);
            MPI_Irecv(row[j].buf + BUF, 100 * BUF, MPI_CHAR, j, 0, MPI_COMM_WORLD, &reqs[j]);
            //MPI_Wait(&reqs[j], &stat);
            row[j].ix = j;
            row[j].p = 0;
            sizebuf[j] -= BUF;
            prow[j] = row + j;
        }
        cout << "total " << total << endl;
        fd = open("/localdisk/yfeng23/res", O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
        lseek(fd, total * 100 - 1, SEEK_SET);
        write(fd, "", 1);
        addr = (char*)mmap(NULL, total * 100, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        make_heap(prow + 1, prow + numtasks, lt);
        for (int j = 1; j < numtasks; j++) {
            printf("%d %d |",j,prow[j]->p);
            for (int k = 0; k < 10; k++)
                putchar(prow[j]->buf[0][k]);
            puts("");
        }
        //MPI_Finalize();
        //return 0;
        for (i = 0; i < total; i++) {
            //printf("%d %d\t%d\n",siz * 100ebuf[1], sizebuf[2], prow[1]->ix);
            memcpy(addr + i * 100, prow[1]->buf[prow[1]->p], 100);
            //puts(prow[1]->buf[prow[1]->p]);
            pop_heap(prow + 1, prow + numtasks, lt);
            prow[numtasks - 1]->p++;
            if (prow[numtasks - 1]->p == BUF + BUF)
                prow[numtasks - 1]->p = 0;
            if (prow[numtasks - 1]->p % BUF == 0) {
                int j = prow[numtasks - 1]->ix;
                if (sizebuf[j]) {
                    //puts("start wait");
                    MPI_Wait(&reqs[j], &stat);
                    //puts("end wait");
                    if (prow[numtasks - 1]->p == BUF) {
                        MPI_Irecv(prow[numtasks - 1]->buf, 100 * BUF, MPI_CHAR, j, 0, MPI_COMM_WORLD, &reqs[j]);
                    }
                    else {
                        MPI_Irecv(prow[numtasks - 1]->buf + BUF, 100 * BUF, MPI_CHAR, j, 0, MPI_COMM_WORLD, &reqs[j]);
                    }
                    sizebuf[j] -= BUF;
                    push_heap(prow + 1, prow + numtasks, lt);
                }
                else {
                    numtasks--;
                }
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
