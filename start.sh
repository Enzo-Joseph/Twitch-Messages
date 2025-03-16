# Description: Start the project

# Build the project
cd kappa
docker compose up -d
cd ../producer
docker compose up -d

# Start the producer
powershell -Command "Start-Process cmd -ArgumentList '/c title Producer && docker exec -ti producer bash -c \"cd app && python3 producer_twitch.py\"'"

# Start Spark Streaming
powershell -Command "Start-Process cmd -ArgumentList '/c title Spark Streaming && docker exec -ti spark-master bash -c \"cd ../spark-apps && python3 cassandra_create_tables.py && bash start_spark_streaming.sh\"'"

# Open Cassandra
powershell -Command "Start-Process cmd -ArgumentList '/c title Cassandra && docker exec -ti cassandra1 bash -c \"cqlsh\"'"
