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
    this->num_threads = num_threads - 1;  // Will use main thread as worker thread.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

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

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::thread workers[num_threads];
    this->num_total_tasks = num_total_tasks;
    this->task_id = 0;
    this->runnable = runnable;

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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;

    for (int i=0; i<num_threads; i++) {
        workers.emplace_back([this]() {
            while (spin) {
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

    // Spin until all tasks are done.
    while ((this->task_id < this->num_total_tasks) || (this->in_progress > 0)) {
        this->tasks_l.unlock();
        this->tasks_l.lock();
    }
    this->tasks_l.unlock();
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
    this->num_threads = num_threads;

    for (int i=0; i<num_threads; i++) {
        workers.emplace_back([this]() {
            std::unique_lock<std::mutex> lk(tasks_l);
            while (running) {
                // Wait for tasks to come in.
                while (task_id == num_total_tasks && running) {
                    cv.wait(lk);
                }

                // Do tasks.
                while (task_id < num_total_tasks) {
                    int id = task_id++;
                    in_progress++;
                    lk.unlock();
                    runnable->runTask(id, num_total_tasks);
                    lk.lock();
                    in_progress--;

                    if (in_progress == 0 && task_id == num_total_tasks) {
                        // All tasks done, notify main thread.
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
    cv.notify_all();

    // Only return when all tasks are done.
    while ((this->task_id < this->num_total_tasks) || (this->in_progress > 0)) {
        cv.wait(lk);
    }
    lk.unlock();
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
