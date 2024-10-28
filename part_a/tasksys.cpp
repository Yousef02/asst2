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

/* 
 * This function ASSUMES that caller owns class lock (tasks_l).
 * Caller will still own lock when doTasks() returns.
 */
void TaskSystemParallelThreadPoolSpinning::doTasks() {
    while (task_id < num_total_tasks) {
        int id = task_id++;
        in_progress++;
        tasks_l.unlock();
        runnable->runTask(id, num_total_tasks);
        tasks_l.lock();
        in_progress--;
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads - 1;  // Will use main thread as worker thread.

    // Spawn worker threads.
    for (int i=0; i<this->num_threads; i++) {
        workers.emplace_back([this]() {
            tasks_l.lock();
            while (spin) {
                tasks_l.unlock();
                tasks_l.lock();
                TaskSystemParallelThreadPoolSpinning::doTasks();
            }
            tasks_l.unlock();
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {  
    // Join worker threads.
    tasks_l.lock();
    this->spin = false;
    tasks_l.unlock();
    for (int i=0; i<num_threads; i++) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    tasks_l.lock();
    this->num_total_tasks = num_total_tasks;
    this->task_id = 0;
    this->runnable = runnable;

    // Use main thread as worker.
    TaskSystemParallelThreadPoolSpinning::doTasks();

    // Spin until all tasks are done.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads) {
    this->num_threads = num_threads;

    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this]() {
	        std::unique_lock<std::mutex> lock(tasks_l);
	        while (!done) {
                // Wait for tasks or termination signal
                cv.wait(lock, [this] {
                    return task_id < num_total_tasks || done;
                });

                // If there is a task to process, grab its ID
                while (task_id < num_total_tasks) {
                    int id = task_id++;
                    lock.unlock();
                    runnable->runTask(id, num_total_tasks);
                    lock.lock();
                    tasks_done++;
                    if (tasks_done == num_total_tasks) {
                        lock.unlock();
                        cv.notify_all();
                        lock.lock();
                    }
                    
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    std::unique_lock<std::mutex> lock(tasks_l);
    done = true;
    lock.unlock();
    cv.notify_all();
    for (int i = 0; i < num_threads; i++) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    {
        std::unique_lock<std::mutex> lock(tasks_l);
        this->num_total_tasks = num_total_tasks;
        this->task_id = 0;
        this->runnable = runnable;
        this->tasks_done = 0;
    }
    cv.notify_all(); // Wake up all threads to start processing tasks

    // Wait for all tasks to finish
    std::unique_lock<std::mutex> lock(tasks_l);
    while (tasks_done < num_total_tasks) {
        cv.wait(lock); // Wait for tasks to be completed
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

