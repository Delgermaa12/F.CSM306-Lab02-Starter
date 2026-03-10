#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>
#include <deque>
#include <unordered_map>
#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    int nthreads;
};

/*
 * TaskSystemParallelThreadPoolSpinning
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    int nthreads;
    std::vector<std::thread> workers;
    std::atomic<bool> shutdownFlag{false};
    std::atomic<bool> hasWork{false};
    std::atomic<int> nextTask{0};
    std::atomic<int> totalTasks{0};
    std::atomic<int> doneCount{0};

    IRunnable *currentRunnable{nullptr};

    void workerLoop(int tid);
};

/*
 * TaskSystemParallelThreadPoolSleeping
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    struct TaskLaunch
    {
        TaskID id{0};
        IRunnable *runnable{nullptr};
        int num_total_tasks{0};
        int next_task{0};
        int done_tasks{0};
        int remaining_deps{0};
        bool finished{false};
        std::vector<TaskID> dependents;
    };

    int nthreads;
    std::vector<std::thread> workers;
    std::atomic<bool> shutdownFlag{false};

    // IRunnable *currentRunnable{nullptr};
    // int total{0};
    // int next{0};
    // int done{0};
    // bool workAvailable{false};
    std::mutex mtx;
    std::condition_variable cvWork;
    std::condition_variable cvDone;

    std::unordered_map<TaskID, TaskLaunch> launches;
    std::deque<TaskID> readyQueue;

    TaskID nextLaunchID{0};
    int unfinishedLaunches{0};

    void workerLoop(int tid);
};
#endif