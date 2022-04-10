from Flow import Flow

class Task():
    parentJob = None
    TaskType = ""
    MachineId = 0
    Size = 0
    flows = []

    def __init__(self, parent, tType, mL, size):
        self.parentJob = parent
        self.TaskType = tType
        self.MachineList = mL
        self.Size = size
    
    def createFlow(self):
        self.flows = []

        avgFlowSize = self.Size / len(self.parentJob.MapTask)
        for m in self.parentJob.MapTask:
            self.flows.append(Flow(m, self, avgFlowSize))
    
    def isComplete(self):
        for f in flows:
            if not f.isComplete():
                return False
        return True