import json


#Adding the flight data csv with columns names to get the values in the mapper

colnames=['Pass_id','f_id','from','dest','dept_time','tot_time']
flights_data='C:/Users/Rachit Mathur/Desktop/Untitled Folder/Cloud/AComp_Passenger_data_no_error_DateTime.csv'

#creating a list of flights and flight_id's

to_flights = []


for flights in open(flights_data):
    flights=flights.strip()
    flights=flights.split(',')
    from_flights=('%s,%s'%(flights[2],flights[1]))
    to_flights.append(from_flights)

#saving the output in a txt file:
    

words=list()

#Sorting the flights and splitting from the repeated flights from the csv

for line in to_flights:
    #taking the first word
    word=line.strip().split()
    words.append(word)
words.sort()

for line in words:
    print(line)


#creating a dictionary by providing key and values to destination and source flights.

dict1={}
for word in words:
    for worr in word:
        a,b=worr.split(',')
        if a in dict1:
            if b in dict1[a]:
                continue
            else:
                c=dict1.get(a)
                new=[]
                if type(c)==list:
                    for aa in c:
                        new.append(aa)
                else:
                    new.append(c)
                new.append(b)
                dict1[a]=new
        else:
            dict1[a]=[b]
            
print(dict1)

#Created a dictionary in which the total numober of flight with their key and values in count are listed
        
for key in dict1:
    b=dict1.get(key)
    count=0
    for keyy in b:
        count+=1
    dict1[key]=count
with open('task1.txt', 'w') as file:
    file.write(json.dumps(dict1))
