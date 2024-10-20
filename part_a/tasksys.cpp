#include "tasksys.h"
#include <thread>
#include <functional>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

void TaskSystemParallelSpawn::doTasks() {
    tasks_l.lock();
    while (task_id < num_total_tasks) {
        int idx = task_id++;
        tasks_l.unlock();
        runnable->runTask(idx, num_total_tasks);
        tasks_l.lock();
    }
    tasks_l.unlock();
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads - 1;  // Will use main thread as worker thread.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::thread workers[num_threads];
    this->num_total_tasks = num_total_tasks;
    this->task_id = 0;
    this->runnable = runnable;

    // Spawn worker threads.
    for (int i=0; i<num_threads; i++) {
        workers[i] = std::thread([this]() {
            TaskSystemParallelSpawn::doTasks();
        });
    }

    // Use main thread as worker.
    TaskSystemParallelSpawn::doTasks();

    // Join worker threads.
    for (int i=0; i<num_threads; i++) {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void TaskSystemParallelThreadPoolSpinning::doTasks() {
    tasks_l.lock();
    while (task_id < num_total_tasks) {
        int id = task_id++;
        in_progress++;
        tasks_l.unlock();
        runnable->runTask(id, num_total_tasks);
        tasks_l.lock();
        in_progress--;
    }
    tasks_l.unlock();
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads - 1;  // Will use main thread as worker thread.

    // Spawn worker threads.
    for (int i=0; i<this->num_threads; i++) {
        workers.emplace_back([this]() {
            while (spin) {
                TaskSystemParallelThreadPoolSpinning::doTasks();
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {  
    // Join worker threads.
    this->spin = false;
    for (int i=0; i<num_threads; i++) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    tasks_l.lock();
    this->num_total_tasks = num_total_tasks;
    this->task_id = 0;
    this->runnable = runnable;
    tasks_l.unlock();

    // Use main thread as worker.
    TaskSystemParallelThreadPoolSpinning::doTasks();

    // Spin until all tasks are done.
    tasks_l.lock();
    while (in_progress > 0) {
        tasks_l.unlock();
        tasks_l.lock();
    }
    tasks_l.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

/* 
 * This function ASSUMES that caller owns class lock (tasks_l).
 * Caller will still own lock when doTasks() returns.
*/
void TaskSystemParallelThreadPoolSleeping::doTasks() {
    while (task_id < num_total_tasks) {
        int id = task_id++;
        in_progress++;
        tasks_l.unlock();
        runnable->runTask(id, num_total_tasks);
        tasks_l.lock();
        in_progress--;

        if ((in_progress == 0) && (task_id == num_total_tasks)) {
            // All tasks done, notify main thread.
            done.notify_one();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads - 1;

    for (int i=0; i<this->num_threads; i++) {
        workers.emplace_back([this]() {
            std::unique_lock<std::mutex> lk(tasks_l);
            while (running) {
                // Sleep until tasks to come in.
                while ((task_id == num_total_tasks) && running) {
                    tasks_cv.wait(lk);
                }

                // Do tasks.
                TaskSystemParallelThreadPoolSleeping::doTasks();
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Join worker threads.
    this->running = false;
    tasks_cv.notify_all();
    for (int i=0; i<num_threads; i++) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::unique_lock<std::mutex> lk(tasks_l);
    this->num_total_tasks = num_total_tasks;
    this->task_id = 0;
    this->runnable = runnable;
    
    // Notify worker threads of tasks to be done.
    tasks_cv.notify_all();

    // Use main thread as worker.
    TaskSystemParallelThreadPoolSleeping::doTasks();

    // Sleep until all tasks are done.
    while (in_progress > 0) {
        done.wait(lk);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
