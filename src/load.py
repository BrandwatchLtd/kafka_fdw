from os.path import dirname, join
from time import sleep
import sys
from kafka import KafkaClient, SimpleProducer


def main():
    say("Loader started...")

    sleep(2)

    say("Starting message load...")

    KafkaHandler().create_topic().load_messages()

    say("Loaded all messages...")

    while True:
        sleep(60)

def say(message):
    print message
    sys.stdout.flush()


class KafkaHandler(object):
    topic = 'test'

    def __init__(self):
        self.client = KafkaClient('kafka:9092')
        self.producer = SimpleProducer(self.client)

    def create_topic(self):
        self.client.ensure_topic_exists(self.topic)
        return self

    def load_messages(self):
        with open('/data.json', 'r') as handle:
            self.producer.send_messages(self.topic, *handle.readlines())


if __name__ == '__main__':
    main()