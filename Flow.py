class Flow:
    mapper = None
    reducer = None
    totalBytes = 0

    remainingBytes = 0
    currBts = 0

    def __init__(self, mI, rI, tB):
        self.mapper = mI
        self.reducer = rI
        self.totalBytes = tB

        self.remainingBytes = tB
        self.currBts = 0
    
    def isComplete(self):
        if remainingBytes <= 0:
            return True
        return False
