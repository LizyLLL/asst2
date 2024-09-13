#include "tasksys.h"
#include <thread>
#include <unordered_map>

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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads_(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    threads_.resize(num_threads);
    stop_ = 0;
    bulk_task_ = -1;
    for (int i = 0; i < num_threads; i++) {
         threads_[i] = std::thread([this] {
             for(;;) {
               TaskID task;
               {
                 std::unique_lock<std::mutex> lock(queue_mutex_);
                 condition_.wait(lock, [this] { return stop_ || !queue_.empty();});
                 if (stop_ || queue_.empty()) {
                      break;
                 }
                 task = queue_.front();
                 queue_.pop();
               }
               runnable_->runTask(task, num_threads_);
               finished_tasks_++;
               if (finished_tasks_ == num_total_tasks_) {

                 for (auto& bulk_id:outs[bulk_task_]) {
                     in_mutex_.lock();
                     in[bulk_id]--;
                     if (in[bulk_id] == 0) {
                         ready_mutex_.lock();
                         ready_bulk_.push(bulk_id);
                         ready_mutex_.unlock();
                         wait_tasks_--;
                     }
                     in_mutex_.unlock();
                 }
                 this->scheduling();
               }
             }
         });
    }
}

void TaskSystemParallelThreadPoolSleeping::scheduling() {
    {
        std::unique_lock<std::mutex> lock(schedule_mutex_);
        if (finished_tasks_ != num_total_tasks_ && bulk_task_ != -1) {
             return;
        }

        ready_mutex_.lock();
        if (ready_bulk_.empty()) {
          finished_.notify_all();
          ready_mutex_.unlock();
          return;
        }
        auto bulk_id = ready_bulk_.front();
        ready_bulk_.pop();
        ready_mutex_.unlock();
        this->num_total_tasks_ = total_tasks_[bulk_id];
        this->runnable_ = runnables_[bulk_id];
        finished_tasks_ = 0;
        bulk_task_ = bulk_id;

        {
            std::unique_lock<std::mutex> lock(this->queue_mutex_);

            for  (int i = 0; i < num_total_tasks_; i++) {

                this->queue_.push(TaskID(i));

                condition_.notify_one();
            }
        }
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop_ = 1;
    condition_.notify_all();
    for(std::thread &worker: threads_) 
        worker.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    in.insert({tasks_, 0});
    outs.insert({tasks_, std::vector<TaskID>()});
    total_tasks_.resize(tasks_ + 1);
    runnables_.resize(tasks_ + 1);

    for  (auto& bulk_id: deps){
         outs[bulk_id].push_back(tasks_);
         in[tasks_]++;
    }

    total_tasks_[tasks_] = num_total_tasks_;
    runnables_[tasks_] = runnable;

    if (in[tasks_] == 0) {
      ready_mutex_.lock();
      ready_bulk_.push(tasks_);
      ready_mutex_.unlock();
    } else {
      wait_tasks_ += 1;
    }

    this->scheduling();
    return tasks_++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(ready_mutex_);
    finished_.wait(lock, [this] { return ready_bulk_.empty() && wait_tasks_ == 0; });

    return;
}
