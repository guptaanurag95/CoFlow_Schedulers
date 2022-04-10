from Flow import Flow

class Task():
    parentJob = None
    TaskType = ""
    MachineId = 0
    Size = 0
    flows = []
    BytesLeft = 0

    def __init__(self, parent, tType, mL, size):
        self.parentJob = parent
        self.TaskType = tType
        self.MachineId = mL
        self.Size = size
        self.BytesLeft = size
    
    def createFlow(self):
        self.flows = []

        avgFlowSize = self.Size / float(len(self.parentJob.MapTask))
        for m in self.parentJob.MapTask:
            self.flows.append(Flow(m, self, avgFlowSize))
    
    def isComplete(self):
        for f in self.flows:
            if not f.isComplete():
                return False
        return True
    
    def printD(self):
        print("============", self.MachineId, self.BytesLeft)