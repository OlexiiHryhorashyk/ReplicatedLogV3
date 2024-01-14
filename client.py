import requests
import time
message = ""
url = 'http://localhost:8000/'
url1 = 'http://localhost:8080/'
url2 = 'http://localhost:8090/'


while message != "stop":
    message = input("Send message:")
    if message == "list master" or message == "list":
        try:
            x = requests.get(url)
            print(x.text)
        except requests.exceptions.ConnectionError as error:
            print(error)
            print("No connection to the master server!")
    elif message == "list sub1" or message == "list1":
        try:
            x = requests.get(url1)
            print(x.text)
        except requests.exceptions.ConnectionError as error:
            print("No connection to the subsequent server №1!")
    elif message == "list sub2" or message == "list2":
        try:
            x = requests.get(url2)
            print(x.text)
        except requests.exceptions.ConnectionError:
            print("No connection to the subsequent server №2!")
    elif message == "health":
        try:
            x = requests.get(url+"health")
            print(x.text)
        except requests.exceptions.ConnectionError:
            print("No connection to master! Health status is not available.")
    elif message == "stop":
        break
    else:
        try:
            start = time.time()
            message = {"message": message}
            x = requests.post(url, json=message)
            print(x.text)
            end = time.time()
            print("Execution time:", round(end - start, 2), "seconds!")
        except requests.exceptions.ConnectionError as error:
            print(error)
            print("No connection to the server!")
