docker run --rm --name aparchespark -p 4040:4040  -v C:\cienciadatos:/home  -it apache/spark-py /opt/spark/bin/pyspark

docker exec -it aparchespark bash

docker exec -it aparchespark spark-submit /home/app.py

docker-compose exec spark-master-1 spark-submit --master spark://172.23.0.2:7077 /tmp/app.py

http://localhost:4040/jobs/


docker-compose exec -it spark-master-1 


step 
up environment
 docker-compose -f spark.yml up

enter container master, check ip adress for master
spark-submit --master spark://172.18.0.2:7077 /tmp/app.py