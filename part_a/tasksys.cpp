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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::thread workers[num_threads];
    task_idx = 0;

    for (int i=0; i<num_threads; i++) {
        workers[i] = std::thread([this, runnable, num_total_tasks]() {
            while (true) {
                int idx = -1;
                tasks_l.lock();
                if (task_idx < num_total_tasks) {
                    idx = task_idx;
                    task_idx++;
                } 
                tasks_l.unlock();
                if (idx != -1) {
                    runnable->runTask(idx, num_total_tasks);
                } else {
                    return;
                }
            }
        });
    }

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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads - 1;  // main thread is also used as a worker.

    for (int i=0; i<this->num_threads; i++) {
        workers.emplace_back([this]() {
            while (spin) {
                int id = -1;

                tasks_l.lock();
                if (task_id < num_total_tasks) {
                    id = task_id++;
                    in_progress++;
                } 
                tasks_l.unlock();

                if (id != -1) {
                    runnable->runTask(id, num_total_tasks);
                    tasks_l.lock();
                    in_progress--;
                    tasks_l.unlock();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {  
    this->spin = false;
    for (int i=0; i<num_threads; i++) {
      workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->tasks_l.lock();
    this->num_total_tasks = num_total_tasks;
    this->task_id = 0;
    this->runnable = runnable;

    // Use main thread as worker.
    while (task_id < num_total_tasks) {
        int id = task_id++;
        in_progress++;
        this->tasks_l.unlock();
        runnable->runTask(id, num_total_tasks);
        this->tasks_l.lock();
        in_progress--;
    }
    this->tasks_l.unlock();

    // Only return when all tasks are done.
    while (spin) {
        this->tasks_l.lock();
        if ((this->task_id == this->num_total_tasks) && (this->in_progress == 0)) {
            this->tasks_l.unlock();
            return;
        }
        this->tasks_l.unlock();
    }
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads - 1;  // main thread is also used as a worker.

    for (int i=0; i<this->num_threads; i++) {
        workers.emplace_back([this]() {
            std::unique_lock<std::mutex> lock(tasks_l);
            while (running) {
                // Wait for tasks to come in.
                while (task_id == num_total_tasks && running) {
                    cv.wait(lock);
                }

                // Do tasks.
                while (task_id < num_total_tasks) {
                    int id = task_id++;
                    in_progress++;
                    lock.unlock();
                    runnable->runTask(id, num_total_tasks);
                    lock.lock();
                    in_progress--;

                    if ((task_id == num_total_tasks) && (in_progress == 0)) {
                        // Notify main thread that all tasks are done.
                        cv.notify_all();
                    }
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    this->running = false;
    // Wake up threads waiting for jobs.
    cv.notify_all();
    for (int i=0; i<this->num_threads; i++) {
      workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::mainThreadTasks(std::unique_lock<std::mutex> &lock) {
    while (task_id < num_total_tasks) {
        int id = task_id++;
        in_progress++;
        lock.unlock();
        runnable->runTask(id, num_total_tasks);
        lock.lock();
        in_progress--;
    }

    // Only return when all tasks are done.
    while (in_progress > 0) {
        cv.wait(lock);
    }

    lock.unlock();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::unique_lock<std::mutex> lock(tasks_l);
    this->task_id = 0;
    this->num_total_tasks = num_total_tasks;
    this->runnable = runnable;
    
    // Notify worker threads of tasks to be done.
    cv.notify_all();

    // Use main thread also as a worker.
    TaskSystemParallelThreadPoolSleeping::mainThreadTasks(lock);
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
