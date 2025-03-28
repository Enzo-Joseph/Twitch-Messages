
# Start the producer
powershell -Command "Start-Process cmd -ArgumentList '/k title Producer && docker exec -ti producer bash -c \"cd app && python3 producer_twitch.py\"'"

# Start Spark Streaming
powershell -Command "Start-Process cmd -ArgumentList '/k title Spark Streaming && docker exec -ti spark-master bash -c \"cd ../spark-apps && python3 cassandra_create_tables.py && bash start_spark_streaming.sh\"'"

# Open Cassandra
powershell -Command "Start-Process cmd -ArgumentList '/k title Cassandra && docker exec -ti cassandra1 bash -c \"cqlsh\"'"

# # Terminal 1
# docker exec -ti producer bash
# cd app
# python producer_twitch.py

# # Terminal 2
# docker exec -ti spark-master bash
# cd ../spark-apps
# python3 cassandra_create_tables.py
# bash start_spark_streaming.sh

# # Terminal 3
# docker exec -ti cassandra1 bash
# cqlsh
