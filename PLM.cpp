#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <random>

class Task
{
public:
    std::string taskID;
    std::string taskStatus;
    std::string workerID;
    std::string clientID;
    int priority;
    int capacity;
    std::vector<std::string> commands;
    int errorCode;

    static std::string generateId()
    {
        const std::string alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, alphanumeric.size() - 1);

        std::string id;
        for (int i = 0; i < 12; ++i)
        {
            id += alphanumeric[dis(gen)];
        }

        return id;
    }
    Task()
    {
        taskID = "";
        taskStatus = "";
        workerID = "";
        clientID = "";
        priority = 0;
        capacity = 0;
        errorCode = 0;
    }

    Task(std::string clientID, int priority, int capacity, std::vector<std::string> commands, int errorCode)
    {
        this->taskID = generateId();
        this->taskStatus = "Intiated";
        this->workerID = workerID;
        this->clientID = clientID;
        this->priority = priority;
        this->capacity = capacity;
        this->commands = commands;
        this->errorCode = errorCode;
    }
};


class Scheduler
{
public:
    static std::string getWorkerId(Task task)
    {
        // Implement your logic here
        return "Worker1";
    }
    

};

class Registry
{
public:
    Registry() {}
    static Registry getInstance()
    {
        static Registry instance;
        return instance;
    }
    void sentTask(Task task)
    {
        // send the task to the worker
    }
};



class HeartBeat
{
public:
    std::string taskID;
    std::string status;
    int errorCode;
};

class PLM
{
private:
    std::unordered_map<std::string, Task> storage; // Map to store taskId to Taskinfo

    // Private constructor to prevent instantiation
    PLM() {}

public:
    // Get instance function to return the singleton instance
    static PLM &getInstance()
    {
        static PLM instance;
        return instance;
    }

    // TODO : if scheduler is not responsing then at least store the task and periodically try to reassign the task using scheduler
    // TODO : (TIMEOUT SCENARIO) periodically check the status of the task and if it is not completed then reassign the task to the worker

    void assignWorker(Task& task)
    {
        std::string workerId = Scheduler::getWorkerId(task);
        task.workerID = workerId;
        storage[task.taskID] = task;
    }

    std::string createTask(std::string clientID, int priority, int capacity, std::vector<std::string> commands, int errorCode)
    {
        Task newTask(clientID, priority, capacity, commands, errorCode);
        storage[newTask.taskID] = newTask;
        assignWorker(newTask);
        intiateTask(newTask);
        std::cout<<"Task "<< newTask.taskID <<" created , workedId assigned: "<<newTask.workerID<<std::endl;
        return newTask.taskID;
    }

    void intiateTask(Task task)
    {
        // call the registry
        // or call directly to the worker using grpc class
        Registry::getInstance().sentTask(task);
    }

    void updateBasedOnHeartBeats(std::vector<HeartBeat> heartbeats)
    {
        for (HeartBeat hb : heartbeats)
        {
            if (storage.find(hb.taskID) != storage.end())
            {
                Task& task = storage[hb.taskID];
                task.taskStatus = hb.status;

                // assuming they will send 0 for success scenario
                task.errorCode = hb.errorCode ? hb.errorCode : 0;
                storage[hb.taskID] = task;
                if (hb.errorCode != 0)
                {
                    // some error occured
                    handleTaskError(task);
                }
            }
        }
    }

    void workerFailed(std::string workerId)
    {
        for (auto it = storage.begin(); it != storage.end(); it++)
        {
            if (it->second.workerID == workerId)
            {
                if (it->second.taskStatus != "Completed")
                    handleTaskError(it->second);
            }
        }
    }

    void handleTaskError(Task task)
    {
        // TODO : add logs
        // when something goes wrong get new workerId for the task
        std::cout<<"Reassigning task to new worker as there is error in the worker Node"<<std::endl;
        task.taskStatus = "Error";
        assignWorker(task);
        intiateTask(task);
        std::cout<<"Reassigned task to new worked node "<<task.workerID<<std::endl;
    }
};

int main()
{
    // Example usage
    PLM &plm = PLM::getInstance();

    // Simulate receiving task status update
    // plm.receiveTaskStatusUpdate("Worker1", "Task1", "In Progress");

    // Simulate worker node failure
    // plm.handleWorkerNodeFailure("Worker2");

    // Simulate task error
    // plm.handleTaskError("Worker3", "Task2");

     // Simulate  Node failure condition ( reassign task to new workerId using Scheduler) 
    // std::vector<std::string> commands = {"command1", "command2"};
    // std::string result = plm.createTask("Client1", 1, 10, commands, 0);

    // // Simulate task failure
    // std::vector<HeartBeat> heartbeats = {{result, "Failed", 1}};
    // plm.updateBasedOnHeartBeats(heartbeats);

    return 0;
}
