# HighLoadDistributedSystems
Repo for Highly Loaded Distributed Systems labs

## Team members (KA-03мп)

- Климчук Ярослав
- Кочмар Катерина

### Lab1 - Docker

#### Commands
##### Main commands that we need:

- Start docker on Ubuntu - ```sudo sevice docker start```
- Compose and run multi-container Docker applications - ```sudo docker-compose up```
- Compose, run and scale multi-container Docker applications - ```sudo docker-compose up --scale {server name}=3```

#### Task 1

- cd Lab1_Docker/Task1 - go into right directory
- sudo docker-compose up - compose and run Docker application

#### Task 2

- cd Lab1_Docker/Task2 - go into right directory
- sudo docker-compose up - compose and run Docker application

#### Task 3

- cd Lab1_Docker/Task3 - go into right directory
- sudo docker-compose up - compose and run Docker application

#### Task 4

- cd Lab1_Docker/Task4 - go into right directory
- sudo docker-compose up --scale lite_server=3 --scale json_server=3 - compose and run Docker application

#### Task 5

- Run commands that are listed above
- Open Postman and run send GET-queries: http://localhost:8080/lite_server/, http://localhost:8080/json_server/profile, http://localhost:8080/json_server/posts, http://localhost:8080/json_server/comments
- Save the results into file Task5/query_results.txt
- cd Lab1_Docker/Task5 - go into right directory
- cat query_results.txt - check file content

## Lab 2 - Jmeter

- task1.jmx - task1.csv as a result
- task2.jmx - task2.csv as a result
- task3.jmx - task3.csv as a result
- task4.jmx - task4-1.csv as a result

## Lab 3 - Mongo

- docker-compose up -d - start docker containers in detached mode
- sudo sh init_rs.sh - execute bash script
- (wait several seconds)
- docker-compose exec router01 sh -c “mongo < .scripts/init-router.js” - start bash script with mongo inside docker-container
- run all generate_data.ipynb - run it to generate additional data
- cp mongo_lab/rides.csv mongo_lab/data/
- sudo sh import_and_query_data.sh
- docker-compose down -v --rmi all --remove-orphans - stop docker containers

## Lab 4 - Hadoop

- Hadoop_screenshots.pdf - protocol
- ProcessUnits.java - fixed Java code
- result.txt - results of requests (step 10)
- sample.txt - input data for requests 

## Lab 5 - Spark

1. Correctly install spark (https://phoenixnap.com/kb/install-spark-on-ubuntu)
2. start-master.sh --webui-port 8001 - start spark locally on 8001 port
3. apt install python3-pip - install it to work with python requirements efficiently
4. pip3 install -r requirements.txt - install all requirements via pip
5. download London.csv
6. spark-submit data_generation.py - generate data via spark
7. spark-submit main.py - execute main file via spark
