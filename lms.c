/*
 * SMART LEARNING MANAGEMENT SYSTEM (LMS) - using Operating System concepts
 * ------------------------------------------------------------------------
 * Compile :  gcc lms.c -o lms -pthread
 * Run     :  ./lms
 *
 * OS concepts used:
 *  1. File system calls (open, read, write, lseek, close) -> student records
 *  2. Multithreading (pthread)                            -> assignment submission
 *  3. Process synchronization (mutex + semaphore)         -> Producer-Consumer
 *  4. CPU scheduling (FCFS, SJF, Round Robin)             -> doubt-session queue
 *  5. Memory management (FIFO, LRU page replacement)      -> lecture content cache
 *  6. Process creation + IPC (fork, pipe, wait)           -> report generation
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/wait.h>

/* ====================================================================
 * MODULE 1: STUDENT RECORDS (File handling using system calls)
 * ==================================================================== */
#define DB "students.dat"

typedef struct {
    int   id;
    char  name[30];
    float marks;
} Student;

void add_student(void) {
    Student s, t;
    printf("Enter student ID   : ");
    scanf("%d", &s.id);
    printf("Enter student name : ");
    scanf("%29s", s.name);
    s.marks = 0;

    /* check duplicate ID */
    int fd = open(DB, O_RDONLY);
    if (fd >= 0) {
        while (read(fd, &t, sizeof(Student)) == (ssize_t)sizeof(Student)) {
            if (t.id == s.id) {
                printf("ID already exists!\n");
                close(fd);
                return;
            }
        }
        close(fd);
    }

    fd = open(DB, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) { perror("open"); return; }
    write(fd, &s, sizeof(Student));
    close(fd);
    printf("Student added successfully.\n");
}

void view_students(void) {
    Student s;
    int fd = open(DB, O_RDONLY);
    if (fd < 0) { printf("No records found.\n"); return; }
    printf("\n%-6s %-20s %-6s\n", "ID", "NAME", "MARKS");
    printf("-------------------------------------\n");
    while (read(fd, &s, sizeof(Student)) == (ssize_t)sizeof(Student))
        printf("%-6d %-20s %-6.2f\n", s.id, s.name, s.marks);
    close(fd);
}

void update_marks(void) {
    Student s;
    int id, found = 0;
    float m;
    printf("Enter student ID : ");
    scanf("%d", &id);
    printf("Enter new marks  : ");
    scanf("%f", &m);

    int fd = open(DB, O_RDWR);
    if (fd < 0) { printf("No records found.\n"); return; }
    while (read(fd, &s, sizeof(Student)) == (ssize_t)sizeof(Student)) {
        if (s.id == id) {
            s.marks = m;
            lseek(fd, -(off_t)sizeof(Student), SEEK_CUR);  /* go back one record */
            write(fd, &s, sizeof(Student));
            found = 1;
            break;
        }
    }
    close(fd);
    printf(found ? "Marks updated.\n" : "Student not found.\n");
}

/* ====================================================================
 * MODULE 2: ASSIGNMENT SUBMISSION (Producer-Consumer problem)
 *   Producers = student threads submitting assignments
 *   Consumers = grader (teacher) threads evaluating them
 *   Buffer    = bounded submission queue (size 5)
 * ==================================================================== */
#define BUF_SIZE 5

int buffer[BUF_SIZE];
int in = 0, out = 0;
int total_assignments, taken = 0;

sem_t empty_slots;                 /* counts free slots in buffer   */
sem_t full_slots;                  /* counts filled slots in buffer */
pthread_mutex_t buf_lock = PTHREAD_MUTEX_INITIALIZER;   /* protects buffer */
pthread_mutex_t ctl_lock = PTHREAD_MUTEX_INITIALIZER;   /* protects 'taken' */

void *student_thread(void *arg) {
    int sid = (int)(long)arg;

    sem_wait(&empty_slots);                 /* wait for a free slot */
    pthread_mutex_lock(&buf_lock);
    buffer[in] = sid;
    in = (in + 1) % BUF_SIZE;
    printf("[Student %d] submitted assignment\n", sid);
    pthread_mutex_unlock(&buf_lock);
    sem_post(&full_slots);                  /* one more item available */
    return NULL;
}

void *grader_thread(void *arg) {
    int gid = (int)(long)arg;

    while (1) {
        /* reserve one assignment so graders stop exactly after all are done */
        pthread_mutex_lock(&ctl_lock);
        if (taken >= total_assignments) {
            pthread_mutex_unlock(&ctl_lock);
            break;
        }
        taken++;
        pthread_mutex_unlock(&ctl_lock);

        sem_wait(&full_slots);              /* wait for an assignment */
        pthread_mutex_lock(&buf_lock);
        int sid = buffer[out];
        out = (out + 1) % BUF_SIZE;
        pthread_mutex_unlock(&buf_lock);
        sem_post(&empty_slots);             /* one more free slot */

        usleep(200000);                     /* simulate grading time */
        int score = 60 + (sid * 37) % 41;   /* simple demo score 60-100 */
        printf("    [Grader %d] graded Student %d -> %d/100\n", gid, sid, score);
    }
    return NULL;
}

void assignment_module(void) {
    int n;
    printf("How many students submit assignments (1-20)? ");
    scanf("%d", &n);
    if (n < 1 || n > 20) { printf("Invalid number.\n"); return; }

    total_assignments = n;
    taken = 0; in = out = 0;
    sem_init(&empty_slots, 0, BUF_SIZE);
    sem_init(&full_slots, 0, 0);

    pthread_t stu[20], gr[2];
    printf("\n--- Submission started (buffer size = %d, 2 graders) ---\n", BUF_SIZE);

    for (long i = 0; i < 2; i++)
        pthread_create(&gr[i], NULL, grader_thread, (void *)(i + 1));
    for (long i = 0; i < n; i++)
        pthread_create(&stu[i], NULL, student_thread, (void *)(i + 1));

    for (int i = 0; i < n; i++) pthread_join(stu[i], NULL);
    for (int i = 0; i < 2; i++) pthread_join(gr[i], NULL);

    sem_destroy(&empty_slots);
    sem_destroy(&full_slots);
    printf("--- All assignments graded ---\n");
}

/* ====================================================================
 * MODULE 3: DOUBT-SESSION SCHEDULER (CPU Scheduling)
 *   Each student's doubt = a "process"; burst time = minutes needed.
 *   Teacher = the CPU.
 * ==================================================================== */
#define MAXP 20

void print_table(int n, int pid[], int bt[], int wt[], int tat[]) {
    float tw = 0, tt = 0;
    printf("\n%-8s %-8s %-8s %-10s\n", "Doubt", "Burst", "Waiting", "Turnaround");
    for (int i = 0; i < n; i++) {
        printf("D%-7d %-8d %-8d %-10d\n", pid[i], bt[i], wt[i], tat[i]);
        tw += wt[i]; tt += tat[i];
    }
    printf("Average waiting time    = %.2f\n", tw / n);
    printf("Average turnaround time = %.2f\n", tt / n);
}

void fcfs(int n, int bt[]) {
    int pid[MAXP], b[MAXP], wt[MAXP], tat[MAXP];
    for (int i = 0; i < n; i++) { pid[i] = i + 1; b[i] = bt[i]; }
    wt[0] = 0;
    for (int i = 1; i < n; i++) wt[i] = wt[i - 1] + b[i - 1];
    for (int i = 0; i < n; i++) tat[i] = wt[i] + b[i];
    printf("\n== FCFS (First Come First Served) ==");
    print_table(n, pid, b, wt, tat);
}

void sjf(int n, int bt[]) {
    int pid[MAXP], b[MAXP], wt[MAXP], tat[MAXP];
    for (int i = 0; i < n; i++) { pid[i] = i + 1; b[i] = bt[i]; }
    /* sort by burst time (bubble sort) */
    for (int i = 0; i < n - 1; i++)
        for (int j = 0; j < n - i - 1; j++)
            if (b[j] > b[j + 1]) {
                int t = b[j]; b[j] = b[j + 1]; b[j + 1] = t;
                t = pid[j]; pid[j] = pid[j + 1]; pid[j + 1] = t;
            }
    wt[0] = 0;
    for (int i = 1; i < n; i++) wt[i] = wt[i - 1] + b[i - 1];
    for (int i = 0; i < n; i++) tat[i] = wt[i] + b[i];
    printf("\n== SJF (Shortest Job First) ==");
    print_table(n, pid, b, wt, tat);
}

void round_robin(int n, int bt[], int q) {
    int pid[MAXP], b[MAXP], rem[MAXP], wt[MAXP], tat[MAXP];
    int time = 0, done = 0;
    for (int i = 0; i < n; i++) { pid[i] = i + 1; b[i] = rem[i] = bt[i]; }

    printf("\n== Round Robin (quantum = %d) ==\nGantt chart: ", q);
    while (done < n) {
        for (int i = 0; i < n; i++) {
            if (rem[i] > 0) {
                int run = rem[i] < q ? rem[i] : q;
                printf("| D%d (%d-%d) ", pid[i], time, time + run);
                time += run;
                rem[i] -= run;
                if (rem[i] == 0) {
                    tat[i] = time;
                    wt[i] = tat[i] - b[i];
                    done++;
                }
            }
        }
    }
    printf("|");
    print_table(n, pid, b, wt, tat);
}

void scheduling_module(void) {
    int n, bt[MAXP], q;
    printf("Number of doubt requests (1-%d): ", MAXP);
    scanf("%d", &n);
    if (n < 1 || n > MAXP) { printf("Invalid number.\n"); return; }
    for (int i = 0; i < n; i++) {
        printf("Time needed (minutes) for doubt D%d: ", i + 1);
        scanf("%d", &bt[i]);
        if (bt[i] < 1) { printf("Time must be positive.\n"); return; }
    }
    printf("Time quantum for Round Robin: ");
    scanf("%d", &q);
    if (q < 1) { printf("Quantum must be positive.\n"); return; }

    fcfs(n, bt);
    sjf(n, bt);
    round_robin(n, bt, q);
}

/* ====================================================================
 * MODULE 4: LECTURE CONTENT CACHE (Page Replacement)
 *   Lecture video/notes ids = pages, cache slots = frames.
 * ==================================================================== */
void fifo_replace(int ref[], int n, int frames) {
    int mem[10], next = 0, faults = 0;
    for (int i = 0; i < frames; i++) mem[i] = -1;

    printf("\n== FIFO Page Replacement ==\n");
    for (int i = 0; i < n; i++) {
        int hit = 0;
        for (int j = 0; j < frames; j++)
            if (mem[j] == ref[i]) { hit = 1; break; }
        if (!hit) {
            mem[next] = ref[i];
            next = (next + 1) % frames;
            faults++;
        }
        printf("Lecture %2d -> [", ref[i]);
        for (int j = 0; j < frames; j++) {
            if (mem[j] == -1) printf(" -"); else printf(" %d", mem[j]);
        }
        printf(" ] %s\n", hit ? "HIT" : "MISS (fault)");
    }
    printf("Total page faults (FIFO) = %d\n", faults);
}

void lru_replace(int ref[], int n, int frames) {
    int mem[10], last_used[10], faults = 0;
    for (int i = 0; i < frames; i++) { mem[i] = -1; last_used[i] = -1; }

    printf("\n== LRU Page Replacement ==\n");
    for (int i = 0; i < n; i++) {
        int hit = -1;
        for (int j = 0; j < frames; j++)
            if (mem[j] == ref[i]) { hit = j; break; }

        if (hit >= 0) {
            last_used[hit] = i;
        } else {
            /* pick empty frame, else least recently used */
            int victim = 0;
            for (int j = 0; j < frames; j++) {
                if (mem[j] == -1) { victim = j; break; }
                if (last_used[j] < last_used[victim]) victim = j;
            }
            mem[victim] = ref[i];
            last_used[victim] = i;
            faults++;
        }
        printf("Lecture %2d -> [", ref[i]);
        for (int j = 0; j < frames; j++) {
            if (mem[j] == -1) printf(" -"); else printf(" %d", mem[j]);
        }
        printf(" ] %s\n", hit >= 0 ? "HIT" : "MISS (fault)");
    }
    printf("Total page faults (LRU)  = %d\n", faults);
}

void memory_module(void) {
    int n, frames, ref[50];
    printf("Number of lecture requests (1-50): ");
    scanf("%d", &n);
    if (n < 1 || n > 50) { printf("Invalid number.\n"); return; }
    printf("Enter lecture ids (e.g. 1 2 3 1 4): ");
    for (int i = 0; i < n; i++) scanf("%d", &ref[i]);
    printf("Cache size in frames (1-10): ");
    scanf("%d", &frames);
    if (frames < 1 || frames > 10) { printf("Invalid frames.\n"); return; }

    fifo_replace(ref, n, frames);
    lru_replace(ref, n, frames);
}

/* ====================================================================
 * MODULE 5: REPORT GENERATION (fork + pipe = process creation + IPC)
 *   Child process reads students.dat and computes the report.
 *   It sends the result to the parent through a pipe.
 * ==================================================================== */
void report_module(void) {
    int fd[2];
    if (pipe(fd) == -1) { perror("pipe"); return; }

    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return; }

    if (pid == 0) {
        /* ---------- CHILD ---------- */
        close(fd[0]);                       /* child only writes */
        Student s;
        int count = 0, hi_id = -1;
        float sum = 0, hi = -1;
        char msg[256];

        int f = open(DB, O_RDONLY);
        if (f >= 0) {
            while (read(f, &s, sizeof(Student)) == (ssize_t)sizeof(Student)) {
                count++;
                sum += s.marks;
                if (s.marks > hi) { hi = s.marks; hi_id = s.id; }
            }
            close(f);
        }
        if (count == 0)
            snprintf(msg, sizeof msg, "No student records to report.\n");
        else
            snprintf(msg, sizeof msg,
                     "Child PID          : %d\n"
                     "Total students     : %d\n"
                     "Class average      : %.2f\n"
                     "Topper (ID %d)     : %.2f marks\n",
                     getpid(), count, sum / count, hi_id, hi);
        write(fd[1], msg, strlen(msg));
        close(fd[1]);
        _exit(0);
    } else {
        /* ---------- PARENT ---------- */
        close(fd[1]);                       /* parent only reads */
        char buf[256];
        ssize_t r;
        printf("\n--- Report received from child process (parent PID %d) ---\n", getpid());
        while ((r = read(fd[0], buf, sizeof buf - 1)) > 0) {
            buf[r] = '\0';
            printf("%s", buf);
        }
        close(fd[0]);
        wait(NULL);                         /* avoid zombie process */
    }
}

/* ====================================================================
 * MAIN MENU
 * ==================================================================== */
int main(void) {
    int ch;
    do {
        printf("\n========= SMART LEARNING MANAGEMENT SYSTEM =========\n");
        printf(" 1. Add student                  (file system calls)\n");
        printf(" 2. View students                (file system calls)\n");
        printf(" 3. Update marks                 (lseek / write)\n");
        printf(" 4. Assignment submission        (threads + semaphores)\n");
        printf(" 5. Doubt-session scheduler      (FCFS / SJF / RR)\n");
        printf(" 6. Lecture content cache        (FIFO / LRU paging)\n");
        printf(" 7. Generate class report        (fork + pipe)\n");
        printf(" 0. Exit\n");
        printf("Enter choice: ");
        if (scanf("%d", &ch) != 1) break;

        switch (ch) {
            case 1: add_student();        break;
            case 2: view_students();      break;
            case 3: update_marks();       break;
            case 4: assignment_module();  break;
            case 5: scheduling_module();  break;
            case 6: memory_module();      break;
            case 7: report_module();      break;
            case 0: printf("Goodbye!\n"); break;
            default: printf("Invalid choice.\n");
        }
    } while (ch != 0);
    return 0;
}
