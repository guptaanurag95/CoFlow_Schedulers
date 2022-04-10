import sys
from Job import Job

NUMBER_MACHINE = -1
NUMBER_COFLOW = -1
CoFlowList = []        

sendBpsFree = []
recvBpsFree = []

currTime = 0
timeStep = 1000   # in milliseconds

def readFile(filename):
    with open(filename) as file:
        lines = file.readlines()

        NUMBER_MACHINE = int(lines[0].split(" ")[0])
        NUMBER_COFLOW = int(lines[0].split(" ")[1])
        
        for l in lines[1:]:
            CoFlowList.append(Job(l))

def init():
    for i in range(NUMBER_MACHINE):
        sendBpsFree.append(1 * 1024 * 1024 * 1024)
        recvBpsFree.append(1 * 1024 * 1024 * 1024)


if len(sys.argv) != 3:
    print("Invalid number of arguments. Run as python CoFlowSim.py <inputFile> <Algo>")

inputF = sys.argv[1]
schedAlgo = sys.argv[2]

readFile(inputF)

for c in CoFlowList:
    c.createFlows()

numJobsComplete = 0
activeJobs = []
while numJobsComplete < len(CoFlowList):
    # add new jobs to active list
    # sort them according to algo
    # allocate bandwidth
    # increament transfer size
    # check if any active job in complete

    currA = []
    for j in activeJobs:
        if j.isComplete(currTime):
            numJobsComplete += 1
        else:
            currA.append(j)
    activeJobs = [x for x in currA]
    
    currTime += timeStep

for i in range(5):
    CoFlowList[i].printJ()