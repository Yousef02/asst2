#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    this->workers.reserve(num_threads);
    this->num_threads = num_threads;
    this->num_total_tasks = 0;
    this->in_progress = 0;
    this->task_id = 0;
    this->done = false;
    this->bulk_in_progress = 0;

    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this]() {

            // wait on cv before starting. return if something is in ready_tasks
            {
                std::unique_lock<std::mutex> lock(tasks_l);
                ready_cv.wait(lock, [this] {
                    return !ready_tasks.empty() || done;
                });
            }


            while (true) {
                // Ensure only one thread enters the bulk processing section
                {
                    std::lock_guard<std::mutex> bulk_lock(bulk_worker_mutex);
                    if (bulk_in_progress == 0 && !bulk_worker_active) {
                        bulk_worker_active = true;  // Mark that a thread is handling the bulk work
                    } else {
                        continue;  // If another thread is working on it, skip the loop
                    }
                }


                while (bulk_in_progress == 0) {                    
                    std::cout << "Bulk in progress" << std::endl;
                    while (ready_tasks.empty() && waiting_tasks.empty()) {
                        std::cout << "both are empty" << std::endl;
                        std::unique_lock<std::mutex> lock(lists_l);
                        cv.wait(lock, [this] {
                            return !ready_tasks.empty() || !waiting_tasks.empty() || done;
                        });
                        if (done) {
                            return;
                        }
                    }
                    if (ready_tasks.empty() && !waiting_tasks.empty()) {
                        std::cout << "Migrating waiting to ready" << std::endl;
                        std::unique_lock<std::mutex> lock(lists_l);
                        task_batch_toolbox item = waiting_tasks.front();
                        waiting_tasks.pop_front();
                       
                        ready_tasks.push_back(item);
                    }

                    std::cout << "getting ready task" << std::endl;
                    task_batch_toolbox item = ready_tasks.front();
                    ready_tasks.pop_front();

                    {
                        std::unique_lock<std::mutex> lock(tasks_l);
                        this->num_total_tasks = item.num_total_tasks;
                        this->task_id = 0;
                        this->runnable = runnable;
                        this->done = false;
                        this->in_progress = 0;
                    }
                    std::cout << "Running task " << item.task_id << std::endl;
                    

                    // Wait for all tasks to finish
                    // std::unique_lock<std::mutex> lock(tasks_l);
                    // while (task_id < num_total_tasks || in_progress > 0) {
                    //     cv.wait(lock); // Wait for tasks to be completed
                    // 
                    bulk_in_progress++;
                    ready_cv.notify_all(); // Wake up all threads to start processing tasks
                    std::cout << "Bulk in progress count: " << bulk_in_progress << std::endl;
                }


                // Once bulk work is complete, allow other threads to enter
                {
                    std::lock_guard<std::mutex> bulk_lock(bulk_worker_mutex);
                    bulk_worker_active = false;  // Mark that no thread is working on bulk processing anymore
                }
                // lists_l.unlock();
            // }



            // while (true) {


                
                int id = -1;
                
                std::unique_lock<std::mutex> lock(tasks_l);

                

                // Wait for tasks or termination signal
                cv.wait(lock, [this] {
                    return task_id < num_total_tasks || done;
                });

                // Exit thread if we're done
                if (done && task_id >= num_total_tasks) {
                    // if (ready_tasks.e
                    return;
                }

                // If there is a task to process, grab its ID
                if (task_id < num_total_tasks) {
                    id = task_id++;
                    in_progress++;
                }
                lock.unlock();

                // Process the task outside the critical section
                if (id != -1) {
                    std::cout << "Running runTask " << num_total_tasks << std::endl;
                    runnable->runTask(id, num_total_tasks);

                    // Update the task completion state
                    lock.lock();
                    in_progress--;
                    if (in_progress == 0 && task_id >= num_total_tasks) {
                        bulk_in_progress--;
                        cv.notify_all();
                        ready_cv.notify_all();

                    }
                    lock.unlock();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    std::unique_lock<std::mutex> lock(tasks_l);
    std::cout << "Destroying" << std::endl;
    // done = true;
    sync();
    done = true;
    lock.unlock();
    cv.notify_all();
    for (int i = 0; i < num_threads; i++) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    // 1 - check if deps are empty or satisfied, then just put on ready queue
    task_id_for_async++;
    bool deps_satisfied = false;
    for (long unsigned int i = 0; i < deps.size(); i++) {
        if (done_set.find(deps[i]) == done_set.end()) {
            deps_satisfied = false;
            break;
        }
        deps_satisfied = true;
    }

    
    if (deps.empty() || deps_satisfied) {
        task_batch_toolbox toPush = {task_id_for_async, runnable, num_total_tasks, 0};
        bool notify = ready_tasks.empty();
        std::cout << "Pushing to ready" << std::endl;
        std::cout << "Task ID: " << task_id_for_async << std::endl;
        ready_tasks.push_back(toPush);
        if (notify){
            std::cout << "Notifying" << std::endl;
            // print size of ready_tasks
            ready_cv.notify_all();
        }
        
        // run(runnable, num_total_tasks); maybe no because it should return immediately.
        return task_id_for_async;
    }
    task_batch_toolbox to_add = {task_id_for_async, runnable, num_total_tasks, 0, deps};
    waiting_tasks.push_back(to_add);
    bulk_worker_mutex.unlock();
    return task_id_for_async;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    // if (!ready_tasks.empty()) {
    //     for (int i = 0; i < ready_tasks.size(); i++) {
    //         task_batch_toolbox item = ready_tasks[i];
    //         run(item.runnable, item.num_total_tasks);
    //         done_set.insert(item.task_id);
    //     }
    //     ready_tasks.clear();
    // }

    // if (!waiting_tasks.empty()) {
    //     for (int i = 0; i < waiting_tasks.size(); i++) {
    //         task_batch_toolbox item = waiting_tasks[i];
    //         run(item.runnable, item.num_total_tasks);

    //     }
    // }


    // when sync is called, all tasks called by runAsyncWithDeps should be run and waithed for to be finished

    // wait for ready list to be empty -> migrate waiting list to ready list -> wait for ready list to be empty -> return

    std::cout << "Syncing" << std::endl;

    std::unique_lock<std::mutex> lock(tasks_l);
    while (!ready_tasks.empty()) {
        ready_cv.wait(lock);
    }

    while (!waiting_tasks.empty()) {
        task_batch_toolbox item = waiting_tasks.front();
        waiting_tasks.pop_front();
        ready_tasks.push_back(item);
    }

    while (!ready_tasks.empty()) {
        ready_cv.wait(lock);
    }

    lock.unlock();
    std::cout << "Synced" << std::endl;
    return;
}
