from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def main():

    admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    server_topics = admin.list_topics()

    topic = "topic1"
    num_partition = 1

    print(server_topics)
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
    else :
        print("topic :",topic,"déjà créé")


if __name__ == "__main__":
    main()
