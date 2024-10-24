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

            while (true) {

                tasks_l.lock();
                while (bulk_in_progress == 0) {
                    std::cout << "Waiting for bulk" << std::endl;
                    bulk_in_progress++;
                    tasks_l.unlock();



                   std::cout << "Bulk in progress" << std::endl;
                    while (ready_tasks.empty() && waiting_tasks.empty()) {
                        std::unique_lock<std::mutex> lock(tasks_l);
                        cv.wait(lock, [this] {
                            return !ready_tasks.empty() || !waiting_tasks.empty() || done;
                        });
                        if (done) {
                            return;
                        }
                    }
                    if (ready_tasks.empty() && !waiting_tasks.empty()) {
                        std::unique_lock<std::mutex> lock(tasks_l);
                        task_batch_toolbox item = waiting_tasks.front();
                        waiting_tasks.pop_front();
                        // we probablt do not need to check deps here because the first thing in the waiting_tasks should be the one with all deps satisfied

                        // bool deps_satisfied = false;
                        // for (int i = 0; i < item.deps.size(); i++) {
                        //     if (done_set.find(item.deps[i]) == done_set.end()) {
                        //         deps_satisfied = false;
                        //         break;
                        //     }
                        //     deps_satisfied = true;
                        // }

                        // if (deps_satisfied) {
                        ready_tasks.push_back(item);
                        // } else {
                        //     waiting_tasks.push_back(item);
                        // }
                    }

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
                    cv.notify_all(); // Wake up all threads to start processing tasks

                    // Wait for all tasks to finish
                    // std::unique_lock<std::mutex> lock(tasks_l);
                    // while (task_id < num_total_tasks || in_progress > 0) {
                    //     cv.wait(lock); // Wait for tasks to be completed
                    // 
                }
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
                    runnable->runTask(id, num_total_tasks);

                    // Update the task completion state
                    lock.lock();
                    in_progress--;
                    if (in_progress == 0 && task_id >= num_total_tasks) {
                        // cv.notify_all();
                        bulk_in_progress--;
                        cv.notify_all();

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
    // {
    //     std::unique_lock<std::mutex> lock(tasks_l);
    //     this->num_total_tasks = num_total_tasks;
    //     this->task_id = 0;
    //     this->runnable = runnable;
    //     this->done = false;
    //     this->in_progress = 0;
    // }
    // cv.notify_all(); // Wake up all threads to start processing tasks

    // // Wait for all tasks to finish
    // std::unique_lock<std::mutex> lock(tasks_l);
    // while (task_id < num_total_tasks || in_progress > 0) {
    //     cv.wait(lock); // Wait for tasks to be completed
    // }

    // std::cout << "All tasks completed!" << std::endl;

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
        ready_tasks.push_back(toPush);
        if (notify){
            cv.notify_all();
        }
        
        // run(runnable, num_total_tasks); maybe no because it should return immediately.
        return task_id_for_async;
    }
    task_batch_toolbox to_add = {task_id_for_async, runnable, num_total_tasks, 0, deps};
    waiting_tasks.push_back(to_add);
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
        cv.wait(lock);
    }

    while (!waiting_tasks.empty()) {
        task_batch_toolbox item = waiting_tasks.front();
        waiting_tasks.pop_front();
        ready_tasks.push_back(item);
    }

    while (!ready_tasks.empty()) {
        cv.wait(lock);
    }

    lock.unlock();
    std::cout << "Synced" << std::endl;
    return;
}
