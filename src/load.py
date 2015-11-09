from os.path import dirname, join
from time import sleep
import sys
from kafka import KafkaClient, SimpleProducer


def main():
    print("Loader started...")

    sleep(2)

    print("Starting message load...")

    KafkaHandler().create_topic().load_messages()

    print("Loaded all messages...")

    while True:
        sleep(60)


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
            self.producer.send_messages(self.topic, *handle.read().splitlines())


if __name__ == '__main__':
    main()
