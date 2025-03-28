import requests
import json
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic
from twitchio.ext import commands
import os
from dotenv import load_dotenv
load_dotenv()

SERVER ="irc.chat.twitch.tv"
PORT = 6667
CLIENT_ID = os.getenv("CLIENT_ID")
OAUTH_TOKEN = os.getenv("OAUTH_TOKEN")
BOT_NICKNAME = "bot"

def get_live_stream_channels(game_id=None, language=None, first=20):
    url = "https://api.twitch.tv/helix/streams"
    headers = {
        "Client-Id": CLIENT_ID,
        "Authorization": f"Bearer {OAUTH_TOKEN}"
    }
    params = {"first": first}  # Default: Get top 20 live streams

    if game_id:
        params["game_id"] = game_id  # Filter by game
    if language:
        params["language"] = language  # Filter by language (e.g., 'en')

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    channel_names = [channel["user_name"] for channel in data["data"]]

    print(channel_names)

    return channel_names
    

class Bot(commands.Bot):

    def __init__(self, producer):
        # Initialise our Bot with our access token, prefix and a list of channels to join on boot...
        channels = get_live_stream_channels(game_id=509658, language="en", first=10)
        super().__init__(token=OAUTH_TOKEN, prefix='?', initial_channels=channels)
        self.producer = producer

    async def event_ready(self):
        # We are logged in and ready to chat and use commands...
        print(f'Bot is ready!')

    async def event_message(self, message):
        # Print messages to console
        print("{} {} {} : {}".format(message.timestamp.strftime("%Y-%m-%d %H:%M:%S"), message.channel.name, message.author.name, message.content))

        # Handle commands
        self.producer.send("twitch_messages", json.dumps({
            "timestamp": message.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": message.channel.name,
            "author": message.author.name,
            "content": message.content
        }).encode())
        self.producer.flush()


if __name__=="__main__":
    producer = KafkaProducer(bootstrap_servers="kafka:29092", api_version=(0, 10, 1))
    bot = Bot(producer)
    bot.run()