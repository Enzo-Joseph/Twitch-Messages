import requests
import json
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic
from twitchio.ext import commands
import os


SERVER ="irc.chat.twitch.tv"
PORT = 6667
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
OAUTH_TOKEN = os.getenv("TWITCH_OAUTH_TOKEN")
BOT_NICKNAME = "bot"


def get_live_stream_channels(game_id=None, language=None, first=20):
    url = "https://api.twitch.tv/helix/streams"
    headers = {
        "Client-Id": CLIENT_ID,
        "Authorization": f"Bearer {OAUTH_TOKEN.removeprefix('oauth:')}"
    }
    params = {"first": first}  # Default: Get top 20 live streams

    if game_id:
        params["game_id"] = game_id  # Filter by game
    if language:
        params["language"] = language  # Filter by language (e.g., 'en')

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    channel_names = [channel["user_name"] for channel in data["data"]]

    return channel_names
    

class Bot(commands.Bot):

    def __init__(self, producer):
        # Initialise our Bot with our access token, prefix and a list of channels to join on boot...
        channels = get_live_stream_channels(game_id=509658, language="en", first=5)
        # super().__init__(token=OAUTH_TOKEN, prefix='?', initial_channels=['wankilstudio'])
        super().__init__(token=OAUTH_TOKEN, prefix='?', initial_channels=channels)
        self.producer = producer

    async def event_ready(self):
        # We are logged in and ready to chat and use commands...
        print(f'Bot is ready!')

    async def event_message(self, message):
        # Print messages to console
        # print("{} {} {} : {}".format(message.timestamp.strftime("%Y-%m-%d %H:%M:%S"), message.channel.name, message.author.name, message.content))

        # Handle commands
        self.producer.send("twitch_messages", json.dumps({
            "timestamp": message.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": message.channel.name,
            "author": message.author.name,
            "content": message.content
        }).encode())
        self.producer.flush()


if __name__=="__main__":
    # admin = KafkaAdminClient(bootstrap_servers='kafka:29092', api_version=(0, 10, 1))
    # server_topics = admin.list_topics()

    # topic = "twitch-messages"
    # num_partition = 1

    # print(server_topics)
    # # création du topic si celui-ci n'est pas déjà créé
    # if topic not in server_topics:
    #     try:
    #         print("create new topic :", topic)

    #         topic1 = NewTopic(name=topic,
    #                          num_partitions=num_partition,
    #                          replication_factor=1)
    #         admin.create_topics([topic1])
    #     except Exception:
    #         print("error")
    #         pass
    # else:
    #     print(topic,"est déjà créé")
    producer = KafkaProducer(bootstrap_servers="kafka:29092", api_version=(0, 10, 1))
    bot = Bot(producer)
    bot.run()