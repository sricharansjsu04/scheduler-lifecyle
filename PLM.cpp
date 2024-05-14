#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <random>
#include <mpi.h>

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
        this->taskStatus = "Initiated";
        this->workerID = "";
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
        static int workerCounter = 3;  // Starting from 3 since 0, 1, and 2 are reserved
        return "Worker" + std::to_string(workerCounter++);
    }
};

class Registry
{
public:
    Registry() {}
    static Registry& getInstance()
    {
        static Registry instance;
        return instance;
    }
    void sentTask(Task task)
    {
        // Simulate sending the task to the worker
        std::cout << "Task " << task.taskID << " sent to " << task.workerID << std::endl;
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
    static PLM& getInstance()
    {
        static PLM instance;
        return instance;
    }

    void assignWorker(Task& task)
    {
        // Send task to Scheduler (Rank 1) to get a worker ID
        MPI_Send(task.taskID.c_str(), task.taskID.length() + 1, MPI_CHAR, 1, 0, MPI_COMM_WORLD);

        // Receive worker ID from Scheduler
        char workerId[100];
        MPI_Recv(workerId, 100, MPI_CHAR, 1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        task.workerID = workerId;
        storage[task.taskID] = task;
    }

    std::string createTask(std::string clientID, int priority, int capacity, std::vector<std::string> commands, int errorCode)
    {
        Task newTask(clientID, priority, capacity, commands, errorCode);
        storage[newTask.taskID] = newTask;
        assignWorker(newTask);
        initiateTask(newTask);
        std::cout << "Task " << newTask.taskID << " created, workerId assigned: " << newTask.workerID << std::endl;
        return newTask.taskID;
    }

    void initiateTask(Task& task)
    {
        // Send task to Registry (Rank 2)
        std::string taskData = task.taskID + ";" + task.workerID;
        MPI_Send(taskData.c_str(), taskData.length() + 1, MPI_CHAR, 2, 0, MPI_COMM_WORLD);
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
                    // some error occurred
                    handleTaskError(task);
                }
            }
        }
    }

    void workerFailed(std::string workerId)
    {
        for (auto it = storage.begin(); it != storage.end(); ++it)
        {
            if (it->second.workerID == workerId)
            {
                if (it->second.taskStatus != "Completed")
                    handleTaskError(it->second);
            }
        }
    }

    void handleTaskError(Task& task)
    {
        // Update task status to indicate error
        task.taskStatus = "Error";

        // Log the error
        std::cout << "Error in task " << task.taskID << " assigned to worker " << task.workerID << ". Reassigning...\n";

        // Reassign task to a new worker
        assignWorker(task);

        // Re-initiate the task
        initiateTask(task);
        std::cout << "Reassigned task "<<task.taskID <<" to new worker node " << task.workerID << std::endl;
    }
};

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    PLM& plm = PLM::getInstance();

    if (world_rank == 0)
    {
        // Rank 0: PLM (main process)

        // Create a new task
        std::vector<std::string> commands = {"command1", "command2"};
        std::string taskID = plm.createTask("Client1", 1, 10, commands, 0);

        // Simulate task failure
        std::vector<HeartBeat> heartbeats = {{taskID, "Failed", 1}};
        plm.updateBasedOnHeartBeats(heartbeats);
    }
    else if (world_rank == 1)
    {
        // Rank 1: Scheduler

        while (true)
        {
            // Receive task ID from PLM
            char taskID[100];
            MPI_Recv(taskID, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Simulate assigning a worker (normally this would be more complex)
            std::string workerId = Scheduler::getWorkerId(Task());
            MPI_Send(workerId.c_str(), workerId.length() + 1, MPI_CHAR, 0, 1, MPI_COMM_WORLD);

            std::cout << "Scheduler assigned worker: " << workerId << " to task: " << taskID << std::endl;
        }
    }
    else if (world_rank == 2)
    {
        // Rank 2: Registry

        while (true)
        {
            // Receive task data from PLM
            char taskData[100];
            MPI_Recv(taskData, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Simulate parsing task data
            std::string taskStr(taskData);
            size_t delimiterPos = taskStr.find(';');
            std::string taskID = taskStr.substr(0, delimiterPos);
            std::string workerID = taskStr.substr(delimiterPos + 1);

            // Simulate sending the task to the worker
            std::cout << "Registry received task ID: " << taskID << " for worker: " << workerID << std::endl;
        }
    }
    else
    {
        // Rank 3+: Workers

        while (true)
        {
            // Simulate receiving a task from Registry
            char taskID[100];
            MPI_Recv(taskID, 100, MPI_CHAR, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Simulate working on the task
            std::cout << "Worker " << world_rank << " received task ID: " << taskID << std::endl;
        }
    }

    MPI_Finalize();
    return 0;
}