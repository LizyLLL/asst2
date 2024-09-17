#include "tasksys.h"
#include <mutex>
#include <thread>
#include <unordered_map>
#include <iostream>
#include "CycleTimer.h"

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
    this->runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    this->sync();
    return;
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
    wait_tasks_ = 0;
    // std::cout<<"start"<<std::endl;
    for (int i = 0; i < num_threads; i++) {

         threads_[i] = std::thread([this] {
             for(;;) {
               TaskID task;

               {
                 std::unique_lock<std::mutex> lock(queue_mutex_);
                 this->condition_.wait(lock, [this] { return this->stop_ || !this->queue_.empty();});
                 if (this->stop_ || this->queue_.empty()) {
                      break;
                 }
                 task = this->queue_.front();
                 //std::cout<<task<<" ";
                 this->queue_.pop();
               }
               // double start_time = CycleTimer::currentSeconds();
               this->runnable_->runTask(task, num_total_tasks_);
               // double end_time = CycleTimer::currentSeconds();
               // std::cout<<(end_time - start_time) * 1000<<"\n";
               finished_tasks_++;
               if (finished_tasks_ == this->num_total_tasks_) {
				 set_mutex_.lock();
                 finished_set_.insert(bulk_task_);
                 set_mutex_.unlock();
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
          // std::cout<<this->bulk_task_<<std::endl;
          finished_.notify_all();
          ready_mutex_.unlock();
          return;
        }
        auto bulk_id = ready_bulk_.front();
        //std::cout<<bulk_id<<std::endl;
        ready_bulk_.pop();
        ready_mutex_.unlock();
        this->num_total_tasks_ = total_tasks_[bulk_id];
        // std::cout<<this->num_total_tasks_<<std::endl;
        this->runnable_ = runnables_[bulk_id];
        finished_tasks_ = 0;
        bulk_task_ = bulk_id;

        {
            std::unique_lock<std::mutex> lock(this->queue_mutex_);

            for  (int i = 0; i < num_total_tasks_; i++) {
				//  std::cout<<num_total_tasks_<<" ";
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
    // std::cout<<"run"<<std::endl;
    this->runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    this->sync();
    return;
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

    for (auto& bulk_id: deps) {
      {
      	 std::unique_lock<std::mutex> lock(set_mutex_);
         if (finished_set_.find(bulk_id) != finished_set_.end()) {
           // std::cout<<"finished"<<std::endl;
           continue;
         }
         outs[bulk_id].push_back(tasks_);
         in[tasks_]++;
      }
    }
    // std::cout<<tasks_<<std::endl;
    total_tasks_[tasks_] = num_total_tasks;
    runnables_[tasks_] = runnable;
    if (in[tasks_] == 0) {
      ready_mutex_.lock();
      ready_bulk_.push(tasks_);
      ready_mutex_.unlock();
      this->scheduling();
    } else {
      wait_tasks_ += 1;
    }


    return tasks_++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // std::cout<<1<<std::endl;
    std::unique_lock<std::mutex> lock(ready_mutex_);

    finished_.wait(lock, [this] {
        // std::cout<<finished_tasks_<<std::endl;
        return ready_bulk_.empty() && wait_tasks_ == 0
                && finished_tasks_ == num_total_tasks_;
        });
    // std::cout<<"sync finished"<<std::endl;
    return;
}
