from Task import Task
import random

class Job:
    ID = 0
    ArrivalTime = 0
    MapTask = []
    ReduceTask = []

    SimulationStartTime = -1
    SimulationEndTime = 0

    totalShuffleBytes = 0
    alpha = -1
    maxShuffleBytes = -1

    def __init__(self, line, numM):
        arr = line.split(" ")
        self.ID = int(arr[0])
        # self.ArrivalTime = int(arr[1])%5
        self.ArrivalTime = int(random.random()*10)

        nM = int(arr[2])
        self.MapTask = []
        for i in range(nM):
            self.MapTask.append(Task(self, "Mapper", int(arr[3+i]), -1))
        
        self.ReduceTask = []
        for i in range(int(arr[3+nM])):
            arr1 = arr[3+nM+1+i].split(":")
            self.ReduceTask.append(Task(self, "Reduce", int(arr1[0]), float(arr1[1])))
            self.maxShuffleBytes = max(self.maxShuffleBytes, float(arr1[1]))
        self.alpha = -1
        self.calcAlpha(numM)
    
    def calcAlpha(self, numM):
        NUMBER_MACHINE = numM

        sendBytes = [0 for i in range(NUMBER_MACHINE)]
        recvBytes = [0 for i in range(NUMBER_MACHINE)]

        perMBytes = sum([x.Size for x in self.ReduceTask]) / len(self.MapTask)

        for m in self.MapTask:
            sendBytes[m.MachineId] += perMBytes
        for r in self.ReduceTask:
            recvBytes[r.MachineId] += r.Size

        self.alpha = max(max(sendBytes), max(recvBytes))
    
    def printJ(self):
        print(self.ID, self.ArrivalTime, len(self.MapTask), len(self.ReduceTask))
    
    def createFlows(self):
        for r in self.ReduceTask:
            r.createFlow()
    
    def isComplete(self, currTime):
        for r in self.ReduceTask:
            if not r.isComplete():
                return False
        
        self.SimulationEndTime = currTime
        return True

    def printD(self):
        for r in self.ReduceTask:
            r.printD()