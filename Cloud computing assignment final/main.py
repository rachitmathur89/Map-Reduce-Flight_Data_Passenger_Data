import mapper
import numpy as np
import time
import threading

outfile = open('output.txt', 'w')


#providing total number of thread count and process counts/jobs counts
threadCount = 10
processCount = 10
all_flights_from_airport = {}
fli_id_based_flights = {}

flight_file = "AComp_Passenger_data_no_error_DateTime.csv"
airport_file ="Top30_airports_LatLong.csv"
dataLen, flightdata, airport_data = mapper.mapper(flight_file, airport_file) #putting the data from and to the defined mapper function

#creating a list of thread processes
threading_arr = []

# Get the (processID)th data of the (threadingID)th thread
def getData(threadingID, processID):
    thread_data = []   #created a list of threading data
    for idx in np.arange(processCount):
        thread_data.append(flightdata[threadCount * processCount * (processID - 1)
                                      + processCount * (threadingID  - 1) + idx])
    return thread_data

# Created a reducer function for multi threading processes
# Calculate reduce and sum up of mapreduce algorithm
def reduceAndSum():

    flight_from_airport = {}
    flights_with_flightID = {}
    for threadingID in all_flights_from_airport:
        for idx in all_flights_from_airport[threadingID]:
            if not idx in flight_from_airport:
                flight_from_airport[idx] = all_flights_from_airport[threadingID][idx]
            else:
                flight_from_airport[idx] += all_flights_from_airport[threadingID][idx]
        for idx in fli_id_based_flights[threadingID]:
            if not idx in flights_with_flightID:
                flights_with_flightID[idx] = []
            for i in np.arange(len(fli_id_based_flights[threadingID][idx])):

                flights_with_flightID[idx].append(fli_id_based_flights[threadingID][idx][i])


    # Total number of flights from each airport
    print("\n Total number of flights from every airport:\n ")
    outfile.write("\n Total number of flights from every airport:")
    for idx in flight_from_airport:
        print("\tAirport(" + idx + ") : "
              + str(flight_from_airport[idx]))
        outfile.write("\n \tAirport(" + idx + ") : "
              + str(flight_from_airport[idx]))



    # Calculate the number of passengers on each flight from one airport to another
    print("\n Total list of flights based on flightID:\n ")
    outfile.write("\n Total list of flights based on flightID:")
    for flightID in flights_with_flightID:
        print("\n \n \tFlight_ID: " + flightID)
        outfile.write("\n \n \tFlight_ID: " + flightID)
        print("\tNumber of passengers: " + str(len(flights_with_flightID[flightID])))
        outfile.write("\n \tNumber of passengers: " + str(len(flights_with_flightID[flightID])))
        print("\tFrom airport(" + flights_with_flightID[flightID][0][1] + ") to airport("
              + flights_with_flightID[flightID][0][2] + ")")
        outfile.write("\n \tFrom airport(" + flights_with_flightID[flightID][0][1] + ") to airport("
              + flights_with_flightID[flightID][0][2] + ")")
        


# Processing the data for every thread
def process(threadingID):
    processID = 1;
    flight_from_airport = {}
    flights_with_flightID = {}
    while (True):
        flightdata = getData(threadingID, processID);

        for row in flightdata:
            if not row[2] in flight_from_airport:
                flight_from_airport[row[2]] = 1
            else:
                flight_from_airport[row[2]] += 1


        # Flights based on the Flight_ID

        # for rows in flightdata:
            flight_data = []
            flight_data.append(row[0])
            flight_data.append(row[2])
            flight_data.append(row[3])
            flight_data.append(row[4])
            flight_data.append(row[5])

            if not row[1] in flights_with_flightID:
                flights_with_flightID[row[1]] = []
            flights_with_flightID[row[1]].append(flight_data)

        processID += 1
        if processID == dataLen // (threadCount * processCount):
            break
    all_flights_from_airport[threadingID] = flight_from_airport
    fli_id_based_flights[threadingID] = flights_with_flightID
    
#Task 1 calculating the toal number of flights from each airport, using reducer and multi threading processes. Taking count on total filghts from each airports.
    if(len(all_flights_from_airport) == threadCount and len(fli_id_based_flights) == threadCount):
        for idx in all_flights_from_airport:
            print("\nThread " + str(idx) + ":")
            outfile.write("\nThread " + str(idx) + ":")
            print("\n \tThe number of flights from every airport:\n")
            outfile.write("\n \tThe number of flights from every airport:\n ")
            for flight in all_flights_from_airport[idx]:
                print("\t\tAirport(" + flight + ") : "
                      + str(all_flights_from_airport[idx][flight]))
                outfile.write("\n \t\tAirport(" + flight + ") : "
                      + str(all_flights_from_airport[idx][flight]))

            print("\n \tList of flights based on flightID:\n ")
            outfile.write("\n \tList of flights based on flightID:\n ")
            for flightID in fli_id_based_flights[idx]:
                print("\t\tFlight_ID: " + flightID)
                outfile.write("\n \t\tFlight_ID: " + flightID)
                print("\t\tNumber of passengers: " + str(len(fli_id_based_flights[idx][flightID])))
                outfile.write("\n \t\tNumber of passengers: " + str(len(fli_id_based_flights[idx][flightID])))
                print("\n \t\tFrom airport(" + fli_id_based_flights[idx][flightID][0][1] + ") to airport("
                      + fli_id_based_flights[idx][flightID][0][2] + ")")
                outfile.write("\t\tFrom airport(" + fli_id_based_flights[idx][flightID][0][1] + ") to airport("
                      + fli_id_based_flights[idx][flightID][0][2] + ")")
                
                
        reduceAndSum()

def main():

    for idx in np.arange(threadCount):
        # thread = multiprocessing.Process(target=process, args=(idx + 1,))
        # thread.start()
        thread = threading.Thread(target=process, args=(idx + 1,))
        thread.start()
        threading_arr.append(thread)

    print ("File written successfully in results.txt")
    # //system.exit(1)

if __name__ == '__main__':
    start = time.time()
    main()
    for idx in np.arange(len(threading_arr)):
        threading_arr[idx].join()
    end = time.time()
    print("Process time: " + str(end - start))
    print("Process finished!")