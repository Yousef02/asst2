#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <atomic>
#include <chrono>
#include <set>
#include <deque>
#include <atomic>
#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        std::vector<std::thread> workers;
        std::mutex tasks_l;
        std::condition_variable cv;
        int num_threads;
        int num_total_tasks;
        int in_progress;
        int task_id;
        bool done;
        IRunnable* runnable;
        bool spin;

        std::mutex dbg_l;

        std::atomic<int> task_id_for_async;

        // int bulk_in_progress = 0;
        std::atomic<int> bulk_in_progress;

        struct task_batch_toolbox {
            TaskID task_id;
            IRunnable* runnable;
            int num_total_tasks;
            int in_progress;
            std::vector<TaskID> deps;
        };

        // need two queues to store tasks; one for tasks that are ready to run
        // and one for tasks that are waiting for dependencies

        // task_id -> task
        
        // std::vector<task_batch_toolbox> ready_tasks;
        std::deque<task_batch_toolbox> ready_tasks;

        // std::vector<task_batch_toolbox> waiting_tasks;
        std::deque<task_batch_toolbox> waiting_tasks;

        std::set<TaskID> done_set;

        // need another cv to wait on ready_tasks being empty
        std::condition_variable ready_cv;

        // need a lock for the ready_tasks and waiting_tasks
        std::mutex lists_l;

        std::mutex bulk_worker_mutex;

        bool bulk_worker_active = false;

        // test case idea: throw an error if ready is done but waiting is not

        // run is going to be just runAsync with no deps followed by a call to sync

        // sync is basically a cv waiting on ready_list to be empty, then it migrates waiting_list to ready_list, then waits again on
        // ready_list being empty, then returns.

        // the populating of this->whatever variables is going to happen in the worker threads, and through the struct I made
};

#endif
