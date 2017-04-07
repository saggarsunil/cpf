import time as tm
import threading
import random
import sys
import re
from collections import defaultdict
import datetime


config_file = "config.txt"
FILENAME_PREFIX = "cpf_fridge_id_v2_"
SALES_DATA_PREFIX = "cpf_sales_id_v2_"

simulations = {}
ratings = {}

oldData =''

geo_map ={      '1':(18.776231,100.770825),
                        '2':(18.776231,100.770825),
                        '3':(18.776231,100.770825),
                        '4':(14.4385193,98.8508320999999),
                        '5':(14.4385193,98.8508320999999),
                        '6':(16.9023422,101.752385399999),
                        '7':(16.9023422,101.752385399999),
                        '8':(16.9078528999999,104.246024),
                        '9':(13.7500301,100.491288199999),
                        '10':(19.1766154,99.889766),
                        '11':(19.8240748,99.7629537999999),
                        '12':(13.1483308,101.0108221),
                        '13':(17.197149,104.1159296),
                        '14':(14.9647325,99.445044),
                        '15':(16.385771,101.2682479),
                        '16':(16.0640388,101.9664485),
                        '17':(16.0390625,99.2334250999999),
                        '18':(15.1186009,104.322009499999),
                        '19':(12.82390562,99.9387787),
                        '20':(18.7869693,98.9865803999999),
                        '21':(19.3582191,98.4404862999999),
                        '22':(12.6086907,102.115981899999),
                        '23':(15.246863,104.8707223),
                        '24':(15.6631698,100.1582897),
                        '25':(16.0084641,101.8974574),
                        '26':(15.8014266,102.0100568),
                        '27':(15.3881107,100.0232545),
                        '28':(15.116423,104.3273265),
                        '29':(12.53246,99.962088),
                        '30':(18.7745748,100.7716394),
                        '31':(18.5773114,99.0079066999999),
                        '32':(13.6652062,102.544846099999),
                        '33':(17.387112,102.7755279),
                        '34':(15.2189196,100.1553432),
                        '35':(14.9806235,102.1165637),
                        '36':(14.9666447,102.073297899999),
                        '37':(14.8936253,100.3967314),
                        '38':(16.4321938,102.823621399999),
                        '39':(14.355964,100.558532),
                        '40':(18.4956917,99.2710882),
                        '41':(18.1445774,100.1402831),
                        '42':(14.0552548,101.378484899999),
                        '43':(16.2455446,103.250247599999),
                        '44':(16.4827798,99.5226617999999),
                        '45':(15.206838,101.792625),
                        '46':(15.5806835,101.0651437),
                        '47':(14.82707,99.6966039999999),
                        '48':(16.1169701,103.7752843),
                        '49':(14.3149398,101.3223324),
                        '50':(19.3000751,97.9605411999999)
}

EVENT_INTERVAL=30  #time between each interval

## A single sample simulating rise in temperature
def simulateIncreaseInTemperature(fileName, num, ts, weight):

    print("Fridge: "+str(num)+"==== simulateIncreaseInTemperature ========")

    dat = ts.strftime("%Y%m%d%H%M%S")

    record = "TS;" + str(dat) + ";InTemp;" + str(random.randint(12, 30)) + ";AmTemp;22;Load;" + str(
        weight) + ";Status;OK;board_id;" + str(num) + ";fridge_id;" + str(
            num) + ";latitude;" + str(geo_map[str(num)][0]) + ";longitude;" + str(geo_map[str(num)][1]) + "\n"

    writeToFile(fileName,record)

    #Once you are done simulating, remove the record from config file
    stop_simulation("simulateIncreaseInTemperature.*")
    return ts,weight


##Generate 1 sample with different cordinates ( more than 100 meters apart)
def simulateFridgeMovement(fileName, num, ts, weight):

    print("Fridge: "+str(num)+"==== simulateFridgeMovement ========")

    dat = ts.strftime("%Y%m%d%H%M%S")
    record = "TS;" + str(dat) + ";InTemp;" + str(random.randint(-5, 0)) + ";AmTemp;22;Load;" + str(
        weight) + ";Status;OK;board_id;" + str(num) + ";fridge_id;" + str(
        num) + ";latitude;" + str(geo_map[str(num)][0]+1.0) + ";longitude;" + str(geo_map[str(num)][1]) + "\n"

    writeToFile(fileName, record)

    #Once you are done simulating, remove the record from config file
    #stop_simulation("simulateFridgeMovement.*")
    return ts,weight


## Generate 5 sample which dipicts continuos loss in weight
## leading to load less than 10 kgs
def simulateChangeInWeight(fileName, num, ts, weight):

    print("Fridge: "+str(num)+"==== simulateChangeinWeight and Sales Record ========")

    dat = ts.strftime("%Y%m%d%H%M%S")

    salesfileName = SALES_DATA_PREFIX + str(num) + ".txt"

    #Time to add in case of oldData
    sec = datetime.timedelta(seconds=30)

    while (weight > 10):
        record = "TS;" + str(dat) + ";InTemp;" + str(random.randint(-5, 0)) + ";AmTemp;22;Load;" + str(
            weight) + ";Status;OK;board_id;" + str(num) + ";fridge_id;" + str(
            num) + ";latitude;" + str(geo_map[str(num)][0]) + ";longitude;" + str(geo_map[str(num)][1]) + "\n"

        weight = weight - random.randint(10,20)
        writeToFile(fileName,record)

        if oldData:
            ts = ts + sec
        else:
            tm.sleep(EVENT_INTERVAL)
            ts = datetime.datetime.now()


        dat = ts.strftime("%Y%m%d%H%M%S")

    #Now weight is less than 10, print the record, to generate alert/critical
    if weight < 0:
      weight = 0

    record = "TS;" + str(dat) + ";InTemp;" + str(random.randint(-5, 0)) + ";AmTemp;22;Load;" + str(
          weight) + ";Status;OK;board_id;" + str(num) + ";fridge_id;" + str(
          num) + ";latitude;" + str(geo_map[str(num)][0]) + ";longitude;" + str(geo_map[str(num)][1]) + "\n"

    writeToFile(fileName,record)


    #If weight less than 0, then make a sales record
    # place a random order of between 200 - 500 Kgs
    order=0.0
    if weight < 10:
        #create a sales record
        #TS; <>;Fridge_id; <>;SalesRevenue; <>;OrderPlaced; <>
        #Rate per kg : 200 Baht
        order = random.choice([50,100,150])
        revenue = order*200
        salesrecord = "TS;" + str(dat) + ";fridge_id;" + str(num) + ";SalesRevenue;"+str(revenue) + ";OrderPlaced;"+str(order)+ "\n"

        print(salesrecord)
        writeToFile(salesfileName,salesrecord)
        #writeToFile(fileName,record)


    #Once you are done simulating, remove the record from config file
    stop_simulation("simulateChangeInWeight.*")

    return ts,order


## Generate 10 samples in increase in temperature
## starting from  15 to 35 degree celcius
def simulateFridgePowerOff(fileName, num, ts,weight):

    print("Fridge: "+str(num)+ "==== simulateFridgePowerOff ========")
    dat = ts.strftime("%Y%m%d%H%M%S")

    #Create 10 records with temperature > 15 celcius
    temp=15
    for i in range(0,10):
        record = "TS;" + str(dat) + ";InTemp;" + str(temp) + ";AmTemp;22;Load;" + str(
            weight) + ";Status;OK;board_id;" + str(num) + ";fridge_id;" + str(
            num) + ";latitude;" + str(geo_map[str(num)][0]) + ";longitude;" + str(geo_map[str(num)][1]) + "\n"
        temp += 2
        writeToFile(fileName,record)
        tm.sleep(EVENT_INTERVAL)

        ts = datetime.datetime.now()
        dat = ts.strftime("%Y%m%d%H%M%S")

    #Once you are done simulating, remove the record from config file
    stop_simulation("simulateFridgePowerOff.*")
    return ts,weight


## Remove the particular stanze from config file
def stop_simulation(expression):
    f = open(config_file, "r")
    lines = f.readlines()
    regex = re.compile(expression)
    new_lines = [x for x in lines if not regex.match(x)]

    f = open(config_file, "w")
    for line in new_lines:
        f.write(line)
    f.close()

## define the workload simulation logic here
## Also read the simulations DS and se if we need to
## call any specific simulation function
def worker(num,start_date,end_date):

    # TS;20170312191811;InTemp;-1;AmTemp;22;Load;63;Status;OK;board_id;1;fridge_id;27;latitude;15.3881107;longitude;100.02325450000001

    fileName = FILENAME_PREFIX+str(num)+".txt"
    salesfileName = SALES_DATA_PREFIX+str(num)+".txt"

    global oldData

    dat = start_date.strftime("%Y%m%d%H%M%S")

    #Create the first record. To make sure that there is first record for each fridge
    salesrecord = "TS;" + str(dat) + ";fridge_id;" + str(num) + ";SalesRevenue;10000" + ";OrderPlaced;100;\n"
    writeToFile(salesfileName,salesrecord)

    #OLD Data : Time increment interval
    interval = datetime.timedelta(seconds=30)

    #OLD Data: Random day of the month where increaseinWeight is simulated
    random_day = random.choice([5,7,2,4,9])

    ts = start_date

    #Starting weight of fridge in KGs
    weight = 150 

    #counter for 
    counter=0

    while ts <= end_date:

        if oldData:
            ts = ts + interval
        else:
            ts = datetime.datetime.now()
            tm.sleep(EVENT_INTERVAL)

        dat = ts.strftime("%Y%m%d%H%M%S")

        #If generating data for current time
        if not oldData:
            simulationToRun = ''
            for key in simulations.keys():
                #print("----"+str(key)+"-----"+"Num: "+str(num))
                #print("=== values "+str(simulations[key]))
                if str(num) in simulations[key]:
                    simulationToRun=key
                    break

            if simulationToRun:
                #Run the simulation function
                #print ("=="+fileName+" ==" +str(num)+"=="+str(ts)+"===="+str(weight)+"===")
                ts, weight = globals()[simulationToRun](fileName, num, ts, weight)
            else:
                #Generate regular data
                record = "TS;"+str(dat)+";InTemp;"+str(random.randint(-2,0))+";AmTemp;22;Load;"+str(weight)\
                       +";Status;OK;board_id;"+str(num)+";fridge_id;"+str(num)+";latitude;"+str(geo_map[str(num)][0])+\
                       ";longitude;"+str(geo_map[str(num)][1])+"\n"

                #if counter % 5 == 0 :
                #   weight = weight - random.uniform(0,0.2)
 
                #counter +=1

                writeToFile(fileName, record)

        else:   # OLD Data
            #Get the day and if the day is modulo of some random
            #selected day, then call simulateWeight & generate sales record 
            day = ts.day

            if day % random_day == 0 and ts.hour == 5 and ts.minute == 6:
                #Simulate change in weight
                ts,weight = simulateChangeInWeight(fileName,num,ts,weight)

            else:
                record = "TS;" + str(dat) + ";InTemp;" + str(random.randint(-5, 0)) + ";AmTemp;22;Load;" + \
                     str(weight) + ";Status;OK;board_id;" + str(num) + ";fridge_id;" + str(num) + \
                     ";latitude;" + str(geo_map[str(num)][0]) +  ";longitude;" + str(geo_map[str(num)][1]) + "\n"

                if counter % 30 == 0 :
                   weight = weight - random.uniform(0,0.2)

                counter +=1

                writeToFile(fileName, record)


## Write to file
def writeToFile(fileName, record):
    file = open(fileName, 'a+')
    file.write(record)
    file.close()


# Open config file every 10 seconds and
# read the configurations
def simulate_events():

    data = []
    while(1):
        global simulations
        global config_file
        fb = open(config_file, 'r')

        simulations={}
        for line in fb:
            data=line.split("=")
            if data[0] == "simulateIncreaseInTemperature" :
                simulations["simulateIncreaseInTemperature"] = data[1].rstrip().split(',')
            elif data[0] == "simulateChangeInWeight":
                simulations["simulateChangeInWeight"] = data[1].rstrip().split(',')
            elif data[0] == "simulateFridgeMovement":
                simulations["simulateFridgeMovement"] = data[1].rstrip().split(',')
            elif data[0] == "simulateFridgePowerOff":
                simulations["simulateFridgePowerOff"] = data[1].rstrip().split(',')

        fb.close()
        tm.sleep(1)


def main():

    ##Get the number of fridges
    if len(sys.argv) < 2:
        exit("Usage: ./loadgen <number_of_fridges> [oldData]")

    number_of_fridges=sys.argv[1]

    global oldData
    #Check if we want to generate data for older
    if 'oldData' in sys.argv:
        oldData = sys.argv[2]

    threads = []

    # The purpose of this thread is to open a file
    # and read the parameters.
    t = threading.Thread(target=simulate_events)
    threads.append(t)
    t.start()

    if oldData:
        start_date = datetime_object = datetime.datetime.strptime('Jan 1 2017  1:00AM', '%b %d %Y %I:%M%p')
        #end_date = datetime_object = datetime.datetime.strptime('Mar 21 2017  1:36AM', '%b %d %Y %I:%M%p')
        end_date = datetime.datetime.now()

    else:
        start_date = datetime.datetime.now()
        end_date = datetime_object = datetime.datetime.strptime('Jan 2 2020  1:36AM', '%b %d %Y %I:%M%p')

    for i in range(1,int(number_of_fridges)+1):
        print("Starting Fridge :"+str(i)+" ....")
        t=threading.Thread(target=worker, args=(i,start_date,end_date))
        threads.append(t)
        t.start()

    #While there are active threads
    while threading.active_count() > 0:
       tm.sleep(5)

#Program start from here ..!!
if __name__ == '__main__':
	main()

