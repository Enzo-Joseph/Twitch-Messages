import json
import time
import urllib.request
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic


def main():
    """_summary_
    
    Returns:
        _type_: _description_
    """    
    API_KEY = "06edcfd23152230a410161600667b1147d287331"
    url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

    # topic = sys.argv[1]
    topic = 'velib-stations'
    
    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')
    
    admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    server_topics = admin.list_topics()

    topic = "velib-stations-nantes"
    num_partition = 1

    print(server_topics)
    # création du topic si celui-ci n'est pas déjà créé
    if topic not in server_topics:
        try:
            print("create new topic :", topic)

            topic1 = NewTopic(name=topic,
                             num_partitions=num_partition,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            print("error")
            pass
    else:
        print(topic,"est déjà créé")

    producer = KafkaProducer(bootstrap_servers="kafka:29092")

    while True:
        response = urllib.request.urlopen(url)
        stations = json.loads(response.read().decode())

        nantes_stations = [station for station in stations if station["contract_name"] == "nantes"]
        # for station in stations:
        #     if station["contract_name"] == "nantes":
        #         nantes_stations.append(station)
        stations = nantes_stations
        
        print(len(stations))
        for station in stations:
            producer.send(topic, json.dumps(station).encode())
            
        print("{} Produced {} station records".format(datetime.fromtimestamp(time.time()), len(stations)))
        time.sleep(60)

if __name__ == "__main__":
    main()
