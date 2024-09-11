#include "tasksys.h"
#include "itasksys.h"
#include <thread>
#include <iostream>
#include <mutex>
#include <queue>

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads_(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    
    std::vector<std::thread> workers(num_threads_);
    std::atomic<int> curr(0);

    for (int i = 1; i < num_threads_; i++) {
        workers[i] = std::thread(&TaskSystemParallelSpawn::runWithThread, this, runnable, std::ref(curr), num_total_tasks);
    }

    int turn = -1;
    while (turn < num_total_tasks) {
        turn = curr.fetch_add(1);
        runnable->runTask(turn, num_total_tasks);
    }

    for (int i = 1; i < num_threads_; i++) {
        workers[i].join();
    }
    
}

void TaskSystemParallelSpawn::runWithThread(IRunnable* runnable, std::atomic<int>& curr,int num_total_tasks) {
    int turn = -1;
    while (turn < num_total_tasks) {
        turn = curr.fetch_add(1);
        std::cout<<turn<<" ";
        runnable->runTask(turn, num_total_tasks);
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads_(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    workers_.resize(num_threads);
    finish_tasks_ = 0;
	for (int i = 0; i < num_threads; i++) {
        // std::cout<< i<< std::endl;
    	this->workers_[i] = std::thread([this] {
      		for(;;) {
                // std::cout<<"threadPoolConstruct"<<std::endl;
				TaskID task;
            	{
                 	std::unique_lock<std::mutex> lock(this->queue_mutex_);
                    this->condition_.wait(lock, [this]{ return this->stop_ || !this->tasks_.empty();});
                    if (this->stop_ || this->tasks_.empty()) {
                        //  std::cout<<this->stop_<<std::endl;
                    	break;
                    }
					task = this->tasks_.front();
                    this->tasks_.pop();
                }
                // std::cout << task << "  " << num_total_tasks_<<std::endl;
                this->runnable_->runTask(task, this->num_total_tasks_);
                ++finish_tasks_;
                //std::cout<<finish_tasks_<<" "<<num_total_tasks_<<std::endl;
                if (finish_tasks_ == this->num_total_tasks_) {
                    //std::cout<<finish_tasks_<<" finish all tasks"<<std::endl;
                    cond_sync_.notify_all();
                }
            }
      	});
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    condition_.notify_all();
    for(std::thread &worker: workers_)
        worker.join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    turn += 1;
    {

    	std::unique_lock<std::mutex> lock(this->queue_mutex_);
        this->num_total_tasks_ = num_total_tasks;
        this->runnable_ =  runnable;

        for  (int i = 0; i < num_total_tasks; i++) {

        	this->tasks_.push(TaskID(i));

            condition_.notify_one();
        }
    }
    std::mutex lck;
    {
        std::unique_lock<std::mutex> lock(lck);
        cond_sync_.wait(lock, [this] {return this->stop_ || finish_tasks_ == this->num_total_tasks_;});
        finish_tasks_ = 0;
    }
	return;
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
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

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
