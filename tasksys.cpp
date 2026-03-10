#include "tasksys.h"

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads), nthreads(num_threads)
{
    //
    // TODO: CSM306 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{

    //
    // TODO: CSM306 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // std::atomic<int> next(0);
    // int use_threads=nthreads;
    // if(use_threads<=0) use_threads=1;
    // std::vector<std::thread> ts;
    // ts.reserve(use_threads);

    // for(int t= 0; t<use_threads; t++)
    // {
    //     ts.emplace_back([&, t]() {
    //         while(true)
    //         {
    //             int id=next.fetch_add(1, std::memory_order_relaxed);
    //             if(id>= num_total_tasks) break;
    //             runnable->runTask(id, num_total_tasks);
    //         }
    //     });
    // }

    // for (auto &th: ts) th.join();

    int use_threads=nthreads;
    if (use_threads<=0) use_threads= 1;

    std::vector<std::thread> ts;
    ts.reserve(use_threads);

    for (int t=0; t<use_threads; t++)
    {
        ts.emplace_back([=]() {
            int start=(num_total_tasks *t) /use_threads;
            int end  =(num_total_tasks* (t+1))/ use_threads;

            for (int i=start; i<end; i++)
            {
                runnable->runTask(i, num_total_tasks);
            }
        });
    }
    for (auto &th :ts) th.join();

}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads), nthreads(num_threads)
{
    //
    // TODO: CSM306 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    if(nthreads<=0) nthreads=1;
    workers.reserve(nthreads);
    for(int i=0; i<nthreads; i++){
        workers.emplace_back([this, i](){ workerLoop(i); });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdownFlag.store(true, std::memory_order_release);
    hasWork.store(true, std::memory_order_release);
    for(auto &th: workers) th.join();
}
void TaskSystemParallelThreadPoolSpinning::workerLoop(int tid) {
    while(!shutdownFlag.load(std::memory_order_acquire)){
        if(!hasWork.load(std::memory_order_acquire)){
            std::this_thread::yield();
            continue;
        }
        int id=nextTask.fetch_add(1, std::memory_order_relaxed);
        int total=totalTasks.load(std::memory_order_acquire);
        if(id<total){
            currentRunnable->runTask(id, total);
            int doneNow=doneCount.fetch_add(1, std::memory_order_relaxed)+1;
            if(doneNow>=total){
                hasWork.store(false, std::memory_order_release);
            }
        }
        else{
            std::this_thread::yield();
        }
        
    }
}
void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{

    //
    // TODO: CSM306 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // for (int i = 0; i < num_total_tasks; i++)
    // {
    //     runnable->runTask(i, num_total_tasks);
    // }
    if(num_total_tasks<=0) return;
    currentRunnable=runnable;
    nextTask.store(0, std::memory_order_release);
    doneCount.store(0, std::memory_order_release);
    totalTasks.store(num_total_tasks, std::memory_order_release);

    hasWork.store(true, std::memory_order_release);
    while(doneCount.load(std::memory_order_acquire)<num_total_tasks){
        std::this_thread::yield();
    }

    hasWork.store(false, std::memory_order_release);
    
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads), nthreads(num_threads)
{
    //
    // TODO: CSM306 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    if(nthreads <= 0)
        nthreads = 1;
    workers.reserve(nthreads);
    for (int i =0; i < nthreads; i++){
        workers.emplace_back([this, i](){ workerLoop(i); });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    //
    // TODO: CSM306 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {std::lock_guard<std::mutex> lk(mtx);
    shutdownFlag.store(true, std::memory_order_release);
workAvailable=true;
    }
    cvWork.notify_all();
    for(auto &th: workers) th.join();
}
void TaskSystemParallelThreadPoolSleeping::workerLoop(int tid) {
    while(true){
        IRunnable *r;
        int id, total_tasks=0;
        {
            std::unique_lock<std::mutex> lk(mtx);
            cvWork.wait(lk, [this](){ 
                return shutdownFlag.load(std::memory_order_acquire) || (workAvailable && next< total); });
            if(shutdownFlag.load(std::memory_order_acquire)) return;
            r=currentRunnable;
            id=next++;
            total_tasks= total;
        }
            r->runTask(id, total_tasks);
            std::lock_guard<std::mutex> lk(mtx);
            done++;
            if(done>=total){
                workAvailable=false;
                cvDone.notify_all();
            }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{

    //
    // TODO: CSM306 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++)
    // {
    //     runnable->runTask(i, num_total_tasks);
    // }

    if(num_total_tasks<=0) return;
    {
        std::lock_guard<std::mutex> lk(mtx);
        currentRunnable=runnable;
        next=0;
        done=0;
        total=num_total_tasks;
        workAvailable=true;
    }
    cvWork.notify_all();
    {
        std::unique_lock<std::mutex> lk(mtx);
        cvDone.wait(lk, [this](){ return done>=total; });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CSM306 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CSM306 students will modify the implementation of this method in Part B.
    //

    return;
}