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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      stop_flag(false),
      total_tasks_in_program(0),
      finished_tasks_count(0),
      upcoming_task_id(0) {
    
    for (int i = 0; i < num_threads; i++) {
        worker_thread_list.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThreadFunction, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> guard(task_queue_lock); 
        stop_flag = true;
    }
    task_ready_cv.notify_all(); // Notify all threads for shutdown
    
    for (std::thread& worker_thread : worker_thread_list) {
        worker_thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();  // Wait until all tasks are completed
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    
    std::unique_lock<std::mutex> lock(task_completion_lock);

    total_tasks_in_program += num_total_tasks;  // Update total task count
    TaskID task_group_id = upcoming_task_id++;

    BulkTask_toolbox to_insert = {runnable, num_total_tasks, 0, 0, {}};
    task_batches[task_group_id] = to_insert;
    BulkTask_toolbox& current_task_group = task_batches[task_group_id];
    
    // Check dependencies and adjust unresolved dependency count
    for (long unsigned int i = 0; i < deps.size(); i++) {
        TaskID dep = deps[i];
        auto dep_task_iter = task_batches.find(dep);

        if (dep_task_iter->second.finished_task_count < dep_task_iter->second.total_task_count 
                                                    && dep_task_iter != task_batches.end()) {
            dep_task_iter->second.dependent_task_ids.push_back(task_group_id);
            current_task_group.pending_dependencies++;
        }
    }

    if (current_task_group.pending_dependencies == 0) {
        // No dependencies, add task to ready queue
        {
            std::lock_guard<std::mutex> guard(task_queue_lock); 
            BulkTask_toolbox& task_batch = task_batches[task_group_id];
            for (int i = 0; i < task_batch.total_task_count; ++i) {
                ready_task_queue.push(Task_toolbox{task_batch.runnable_task, i, task_group_id, task_batch.total_task_count});
            }
        }
        task_ready_cv.notify_all();  // Notify workers about new tasks
    }

    return task_group_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(task_completion_lock);
    while (finished_tasks_count < total_tasks_in_program) {
        all_tasks_done_cv.wait(lock);
    }
    // Only return when all tasks are done
}

// function passed to threads in the constructor
// they will wait until there is a task to execute
// and then execute it. after executing the task, they will
// check if the task is part of a group of tasks and if all
// tasks in the group are done, they will notify the dependent
// tasks.
void TaskSystemParallelThreadPoolSleeping::workerThreadFunction() {
    while (true) {
        Task_toolbox current_task;
        bool task_available = false;

        {
            std::unique_lock<std::mutex> lock(task_queue_lock);
            while (!stop_flag && ready_task_queue.empty()) {
                task_ready_cv.wait(lock);  // Wait until tasks are available
            }

            if (stop_flag && ready_task_queue.empty()) {
                return;  // Exit if shutdown and no tasks remain
            }

            if (!ready_task_queue.empty()) {
                current_task = ready_task_queue.front();
                ready_task_queue.pop();
                task_available = true;
            }
        }

        if (task_available) {
            // Execute task
            current_task.runnable_task->runTask(current_task.task_index, current_task.group_total_tasks);

            int current_finished = finished_tasks_count.fetch_add(1);  // Update completion count

            {
                std::unique_lock<std::mutex> lock(task_completion_lock);
                BulkTask_toolbox& parent_batch = task_batches[current_task.task_group_id];

                // Update parent task group to check if all tasks are done
                // If all tasks are done, notify dependent tasks
                if (++parent_batch.finished_task_count == parent_batch.total_task_count) {
                    for (TaskID dependent_id : parent_batch.dependent_task_ids) {
                        BulkTask_toolbox& dependent_batch = task_batches[dependent_id];
                        if (--dependent_batch.pending_dependencies == 0) {
                            {
                                std::lock_guard<std::mutex> guard(task_queue_lock); 
                                BulkTask_toolbox& task_batch = task_batches[dependent_id];
                                for (int i = 0; i < task_batch.total_task_count; ++i) {
                                    ready_task_queue.push(Task_toolbox{task_batch.runnable_task, i, dependent_id, task_batch.total_task_count});
                                }
                            }
                            task_ready_cv.notify_all();  // Notify workers about new tasks  // Queue dependent tasks
                        }
                    }
                }

                int total_done = current_finished + 1;
                if (total_done == total_tasks_in_program) { 
                    all_tasks_done_cv.notify_all();  // Signal when all tasks are done
                }
            }
        }
    }
}