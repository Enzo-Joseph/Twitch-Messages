powershell -Command "Start-Process cmd -ArgumentList '/c title Cassandra && docker exec -ti cassandra1 bash -c \"cqlsh\"'"
