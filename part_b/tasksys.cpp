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
                bool move_on = false;
                

                // Ensure only one thread enters the bulk processing section
                {
                    std::lock_guard<std::mutex> bulk_lock(bulk_worker_mutex);
                    if (bulk_in_progress == 0 && !bulk_worker_active) {
                        bulk_worker_active = true;  // Mark that a thread is handling the bulk work
                    } else {
                        move_on = true;
                    }
                }
                // std::cout << "HIIIIII" << std::endl;

                while (bulk_in_progress == 0 && !move_on) {
                    bulk_in_progress++;
                    
                    // std::cout << "bulk_in_progress == 0" << std::endl;
                    std::unique_lock<std::mutex> lists_lock(lists_l);  // Ensure proper locking
                    while (ready_tasks.empty() && waiting_tasks.empty()) {
                        cv.wait(lists_lock, [this] {
                            return !ready_tasks.empty() || !waiting_tasks.empty() || done;
                        });
                        if (done) {
                            return;
                        }
                    }

                    if (ready_tasks.empty() && !waiting_tasks.empty()) {
                        task_batch_toolbox item = waiting_tasks.front();
                        waiting_tasks.pop_front();
                        ready_tasks.push_back(item);
                    }

                    // dbg_l.lock();
                    task_batch_toolbox item = ready_tasks.front();
                    ready_tasks.pop_front();
                    // std::cout << " processing batch " << item.task_id << std::endl;
                    // dbg_l.unlock();

                    {
                        std::unique_lock<std::mutex> lock(tasks_l);
                        this->num_total_tasks = item.num_total_tasks;
                        this->task_id = 0;
                        this->runnable = item.runnable;  // Ensure runnable is assigned from item
                        this->done = false;
                        this->in_progress = 0;
                        this->task_id_for_bulk = item.task_id;
                    }

                    
                }
                
                {
                    std::lock_guard<std::mutex> bulk_lock(bulk_worker_mutex);
                    bulk_worker_active = false;  // Allow other threads to process after bulk work
                }

                int id = -1;
                std::unique_lock<std::mutex> lock(tasks_l);

                // Wait for tasks or termination signal
                cv.wait(lock, [this] {
                    return task_id < num_total_tasks || done;
                });

                // Exit thread if we're done
                if (done && task_id >= num_total_tasks) {
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
                    // std::cout << "Thread " << std::this_thread::get_id() << " processing task " << id << std::endl;
                    runnable->runTask(id, num_total_tasks);

                    lock.lock();
                    in_progress--;
                    if (in_progress == 0 && task_id >= num_total_tasks) {
                        
                        bulk_in_progress--;
                        // done_set.insert(task_id_for_bulk);
                        cv.notify_all();
                        ready_cv.notify_all();
                    }
                    // dbg_l.lock();
                    // std::cout << "in_progress: " << in_progress << std::endl;
                    // std::cout << "task_id: " << this->task_id << std::endl;
                    // std::cout << "num_total_tasks: " << num_total_tasks << std::endl;
                    // dbg_l.unlock();
                    lock.unlock();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    
    
    bulk_worker_mutex.lock();
    done = true;
    std::cout << "Calling sync one last time" << std::endl;
    // sync();
    bulk_worker_mutex.unlock();
    cv.notify_all();

    ready_cv.notify_all();


    for (int i = 0; i < num_threads; i++) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    task_id_for_async++;
    bool deps_satisfied = true;  // Default to true if no dependencies

    for (const TaskID& dep : deps) {
        if (done_set.find(dep) == done_set.end()) {
            deps_satisfied = false;
            break;
        }
    }

    if (deps.empty() || deps_satisfied) {
        task_batch_toolbox toPush = {task_id_for_async, runnable, num_total_tasks, 0};
        bool notify = ready_tasks.empty();

        ready_tasks.push_back(toPush);
        if (notify) {
            ready_cv.notify_all();
        }

        return task_id_for_async;
    }

    task_batch_toolbox to_add = {task_id_for_async, runnable, num_total_tasks, 0, deps};
    waiting_tasks.push_back(to_add);
    cv.notify_all();
    return task_id_for_async;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::cout << "syncing" << std::endl;
    std::unique_lock<std::mutex> lock(lists_l);
    while (!ready_tasks.empty()) {
        ready_cv.wait(lock);
    }

std::cout << "sync done" << std::endl;

    while (!waiting_tasks.empty()) {
        task_batch_toolbox item = waiting_tasks.front();
        waiting_tasks.pop_front();
        ready_tasks.push_back(item);
    }

    while (!ready_tasks.empty()) {
        ready_cv.wait(lock);
    }

    lock.unlock();
    
}

