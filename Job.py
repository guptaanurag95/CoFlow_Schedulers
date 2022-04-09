from Task import Task

class Job:
    ID = 0
    ArrivalTime = 0
    MapTask = []
    ReduceTask = []

    SimulationStartTime = 0
    SimulationEndTime = 0

    totalShuffleBytes = 0

    def __init__(self, line):
        arr = line.split(" ")
        self.ID = int(arr[0])
        self.ArrivalTime = int(arr[1])
        
        nM = int(arr[2])
        self.MapTask = []
        for i in range(nM):
            self.MapTask.append(Task(self, "Mapper", int(arr[3+i]), -1))
        
        self.ReduceTask = []
        for i in range(int(arr[3+nM])):
            arr1 = arr[3+nM+1+i].split(":")
            self.ReduceTask.append(Task(self, "Reduce", int(arr1[0]), float(arr1[1])))
    
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

