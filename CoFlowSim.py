import sys
from Job import Job
from functools import cmp_to_key

NUMBER_MACHINE = -1
NUMBER_COFLOW = -1
CoFlowList = []        

sendBpsFree = []
recvBpsFree = []

currTime = 0
timeStep = 0.5   # in milliseconds

def readFile(filename):
    global NUMBER_MACHINE, NUMBER_COFLOW, CoFlowList

    with open(filename) as file:
        lines = file.readlines()

        NUMBER_MACHINE = int(lines[0].split(" ")[0])
        NUMBER_COFLOW = int(lines[0].split(" ")[1])
        
        for l in lines[1:]:
            CoFlowList.append(Job(l, NUMBER_MACHINE))

def init():
    global sendBpsFree, recvBpsFree

    for i in range(NUMBER_MACHINE):
        sendBpsFree.append(1  * 1024 * 1024)
        recvBpsFree.append(1  * 1024 * 1024)

def sortJobs():
    global schedAlgo, activeJobs

    if schedAlgo == "SEBF":
        def compare(j1, j2):
            if j1.alpha == j2.alpha:
                return 0
            elif j1.alpha < j2.alpha:
                return -1
            return 1
        activeJobs.sort(key=cmp_to_key(compare))
    elif schedAlgo == "SCFC":
        def compare(j1, j2):
            if j1.maxShuffleBytes / len(j1.MapTask) == j2.maxShuffleBytes / len(j2.MapTask):
                return 0
            elif j1.maxShuffleBytes / len(j1.MapTask) < j2.maxShuffleBytes / len(j2.MapTask):
                return -1
            return 1
        activeJobs.sort(key=cmp_to_key(compare))
    elif schedAlgo == "NCFC":
        def compare(j1, j2):
            n1 = min(len(j1.MapTask), len(j1.ReduceTask))
            n2 = min(len(j2.MapTask), len(j2.ReduceTask))
            return n1-n2
        activeJobs.sort(key=cmp_to_key(compare))
    else:
        print("Inalid Scheduling Algo")
        exit()
        
    
def calcAlpha(currJ):
    global NUMBER_MACHINE

    sendBytes = [0 for i in range(NUMBER_MACHINE)]
    recvBytes = [0 for i in range(NUMBER_MACHINE)]

    for r in currJ.ReduceTask:
        recvBytes[r.MachineId] += r.BytesLeft
        for f in r.flows:
            sendBytes[f.mapper.MachineId] += f.remainingBytes
    
    for i in range(NUMBER_MACHINE):
        if (sendBytes[i] > 0 and sendBpsFree[i] <= 0) or (recvBytes[i] > 0 and recvBpsFree[i] <= 0):
            return -1
        
        if sendBytes[i]!=0:
            sendBytes[i] = sendBytes[i] * 8 / sendBpsFree[i]
        if recvBytes[i]!=0:
            recvBytes[i] = recvBytes[i] * 8 / recvBpsFree[i]
    return max(max(sendBytes), max(recvBytes))

def allocateBW():
    global activeJobs, sendBpsFree, recvBpsFree, skippedJob

    init()
    skippedJobs = []

    for j in activeJobs:
        for r in j.ReduceTask:
            for f in r.flows:
                f.currBts = 0
    
        sendUsed = [0 for i in range(NUMBER_MACHINE)]
        recvUsed = [0 for i in range(NUMBER_MACHINE)]

        currAlpha = calcAlpha(j)
        if currAlpha == -1:
            skippedJob.append(j)
            continue
        
        def updateRates(currJ, currAlpha):
            global recvBpsFree, sendBpsFree
            nonlocal sendUsed, recvUsed

            for r in currJ.ReduceTask:
                if recvBpsFree[r.MachineId] <= 0:
                    continue
                
                for f in r.flows:
                    src = f.mapper.MachineId
                    
                    currBps = f.remainingBytes * 8 / currAlpha
                    if currBps > sendBpsFree[src] or currBps > recvBpsFree[r.MachineId]:
                        currBps = min(sendBpsFree[src], recvBpsFree[r.MachineId])
                    
                    sendUsed[src] += currBps
                    recvUsed[r.MachineId] += currBps

                    f.currBts = currBps
        
        updateRates(j, currAlpha)

        for i in range(NUMBER_MACHINE):
            sendBpsFree[i] -= sendUsed[i]
            recvBpsFree[i] -= recvUsed[i]

def transferBytes():
    global activeJobs, sendBpsFree, recvBpsFree, skippedJobs, currTime

    arr = []
    for j in activeJobs:
        allFinished = True
        for r in j.ReduceTask:
            for f in r.flows:
                bytesMove = (f.currBts / 8) * ((timeStep/int(((1  * 1024 * 1024)/8.0)/1024)) / 1000.0)
                bytesMove = min(bytesMove, f.remainingBytes)
                
                f.remainingBytes -= bytesMove
                if f.remainingBytes <= 0:
                    sendBpsFree[f.mapper.MachineId] += f.currBts
                    recvBpsFree[r.MachineId] += f.currBts
                    f.currBts = 0
                
                r.BytesLeft -= bytesMove
                if r.BytesLeft < 0:
                    r.BytesLeft = 0
            if not r.isComplete():
                allFinished = False
        if not allFinished:
            arr.append(j)
        else:
            j.SimulationEndTime = currTime+timeStep
    allSkipped = True
    for j in arr:
        if j not in skippedJob:
            allSkipped = False
    if allSkipped:
        activeJobs = []
    else:
        activeJobs = [x for x in arr]
                    

if len(sys.argv) != 3:
    print("Invalid number of arguments. Run as python CoFlowSim.py <inputFile> <Algo>")

inputF = sys.argv[1]
schedAlgo = sys.argv[2]

readFile(inputF)
init()

for c in CoFlowList:
    c.createFlows()

numJobsComplete = 0
activeJobs = []
skippedJob = []
currIndex = 0

while numJobsComplete < len(CoFlowList):
    # add new jobs to active list

    numNewJob = 0
    while currIndex < len(CoFlowList):
        if CoFlowList[currIndex].ArrivalTime > currTime:
            break

        CoFlowList[currIndex].SimulationStartTime = currTime
        activeJobs.append(CoFlowList[currIndex])
        numNewJob += 1
        currIndex += 1
    
    # sort them according to algo
    sortJobs()

    # allocate bandwidth
    if numNewJob > 0:
        allocateBW()

    # increament transfer size
    numActiveJobs = len(activeJobs)
    transferBytes()
    if numActiveJobs > len(activeJobs):
        numJobsComplete += numActiveJobs - len(activeJobs)
        allocateBW()
    
    currTime += timeStep

totalSimTime = 0
count = 0
for c in CoFlowList:
    if c.SimulationEndTime > 0:
        totalSimTime += c.SimulationEndTime - c.SimulationStartTime
        count += 1

print(schedAlgo, "- Average CCT:", totalSimTime/count)