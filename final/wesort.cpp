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
#include <assert.h>
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

bool cmp(char *a, char *b) {
    return strcmp(a, b) < 0;
}

int main(int argc, char *argv[]) {
    struct timeval start;
    struct timeval finish;
    gettimeofday(&start, 0);

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);
    cout << rank << " is on " << processor_name << endl;

    long long i;
    int j;
    int fd;
    struct stat sb;
    char *addr;
    long long len;

    char *keybuf;
    char sp[numtasks - 1][10];

    fd = open("/localdisk/yfeng23/part", O_RDONLY);
    if (fd == -1)
        handle_error("open");

    if (fstat(fd, &sb) == -1)
        handle_error("fstat");

    len = sb.st_size;
    len /= 100;
    cout << "process " << rank << " " << len << endl;
    char *key = new char[len / 10];
    mykey *mkey = new mykey[len];

    addr = (char*)mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED)
        handle_error("mmap");
    for (i = 0; i < len; i++) {
        mkey[i].ix = i;
        memcpy(mkey[i].v, addr + 100 * i, 10);
        if (i % 100 == 0)
            memcpy(key + i / 10, addr + 100 * i, 10);
    }
    if (!rank) {
        keybuf = new char[numtasks * len / 10];
    }
    MPI_Gather(key, len / 10, MPI_CHAR, keybuf, len / 10, MPI_CHAR, 0, MPI_COMM_WORLD);

    if (!rank) {
        char *p[len * numtasks / 100];
        for (i = 0; i < len * numtasks / 100; i++)
            p[i] = keybuf + i * 10;
        sort(p, p + len * numtasks / 100, cmp);
        for (int j = 0; j < numtasks - 1; j++) {
            memcpy(sp[j], p[(j + 1) * len / 100], 10);
            //write(1, sp[j], 10);
            //puts("");
        }
        //puts("get sp");
    }
    MPI_Bcast(sp, sizeof(sp), MPI_CHAR, 0, MPI_COMM_WORLD);

    MPI_Request rec1[numtasks];
    long long sizebuf[numtasks];
    for (j = 0; j < numtasks; j++) {
        if (j == rank) {
            rec1[j] = MPI_REQUEST_NULL;
        }
        else {
            MPI_Irecv(&sizebuf[j], 1, MPI_LONG_LONG, j, 1, MPI_COMM_WORLD, &rec1[j]);
        }
    }
    sort(mkey, mkey + len);

    int td = open("/localdisk/yfeng23/res", O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
    lseek(td, sb.st_size - 1, SEEK_SET);
    write(td, "", 1);
    char *taddr;
    taddr = (char*)mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, td, 0);

    j = 0;
    long long pre = 0, dif;
    long long locs, loct;
    MPI_Request reqs[numtasks];
    reqs[rank] = MPI_REQUEST_NULL;
    for (i = 0; i < len; i++) {
        memcpy(taddr + i * 100, addr + mkey[i].ix * 100, 100);
        //if (rank == 1) {
        //    write(1, taddr + i * 100, 10);
        //    write(1, sp[j], 10);
        //    puts("");
        //}
        if (j < numtasks - 1 && strcmp(sp[j], taddr + i * 100) < 0) {
            if (j == rank) {
                locs = pre;
                loct = i;
            }
            else {
                dif = i - pre;
                MPI_Send(&dif, 1, MPI_LONG_LONG, j, 1, MPI_COMM_WORLD);
                //printf("%d sending %lld to %d\n", rank, dif, j);
                MPI_Isend(taddr + pre * 100, dif * 100, MPI_CHAR, j, 0, MPI_COMM_WORLD, &reqs[j]);
            }
            pre = i;
            j++;
        }
    }
    if (numtasks - 1 == rank) {
        locs = pre;
        loct = len;
    }
    else {
        dif = len - pre;
        MPI_Send(&dif, 1, MPI_LONG_LONG, j, 1, MPI_COMM_WORLD);
        MPI_Isend(taddr + pre * 100, dif * 100, MPI_CHAR, j, 0, MPI_COMM_WORLD, &reqs[j]);
    }
    munmap(addr, sb.st_size);
    close(fd);
    delete []mkey;

    fd = open("/localdisk/yfeng23/tmp", O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
    lseek(fd, len * 150 - 1, SEEK_SET);
    write(fd, "", 1);
    addr = (char*)mmap(NULL, len * 150, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    pre = loct - locs;
    memcpy(addr, taddr + locs * 100, pre * 100);
    //printf("%d echo\n", rank);
    
    int fi;
    MPI_Request rec2[numtasks];
    rec2[rank] = MPI_REQUEST_NULL;
    MPI_Status status;
    for (j = numtasks - 1; j; j--) {
        MPI_Waitany(numtasks, rec1, &fi, &status);
        MPI_Irecv(addr + pre * 100, sizebuf[fi] * 100, MPI_CHAR, fi, 0, MPI_COMM_WORLD, &rec2[fi]);
        pre += sizebuf[fi];
    }
    //if (rank == 9) {
    //    for (j = 0; j < numtasks; j++)
    //        cout << sizebuf[j] << " ";
    //    puts("");
    //}

    MPI_Status all_status[numtasks];
    MPI_Waitall(numtasks, rec2, all_status);
    mkey = new mykey[pre];
    for (i = 0; i < pre; i++) {
        mkey[i].ix = i;
        memcpy(mkey[i].v, addr + 100 * i, 10);
    }
    sort(mkey, mkey + pre);
    MPI_Waitall(numtasks, reqs, all_status);
    munmap(taddr, sb.st_size);
    printf("%d has %lld\n", rank, pre);
    if (pre < len) {
        ftruncate(td, pre * 100);
    }
    else if (pre > len) {
        lseek(td, pre * 100 - 1, SEEK_SET);
        write(td, "", 1);
    }
    taddr = (char*)mmap(NULL, pre * 100, PROT_READ | PROT_WRITE, MAP_SHARED, td, 0);

    for (i = 0; i < pre; i++) {
        memcpy(taddr + i * 100, addr + mkey[i].ix * 100, 100);
    }
    munmap(addr, len * 110);
    close(fd);
    munmap(taddr, pre * 100);
    close(td);
    if (rank == numtasks - 1) {
        gettimeofday(&finish, 0);
        cout << "time:" << finish.tv_sec - start.tv_sec << endl;
    }
    MPI_Finalize();
    return 0;
}
