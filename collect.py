import os
import sys
import shutil
import logging

import pika
import pyinotify

# pylint: disable=invalid-name,no-self-use,logging-format-interpolation
RABBITMQ_ADDR = os.getenv('RABBITMQ_SERVICE_HOST')
QUEUE_NAME = 'processor'


def mkdirs():
    dirs = ['/opt/data/processor/queue', '/opt/data/collector/files']
    for d in dirs:
        try:
            os.makedirs(d)
        except OSError:
            msg = 'Skipping make dir {}'.format(d)
            logging.info(msg)


class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        filename = os.path.basename(event.pathname)
        destination = os.path.join('/opt/data/processor/queue/', filename)
        shutil.copy(event.pathname, destination)
        msg = 'Copied file {} to {}'.format(filename, destination)
        logging.info(msg)
        channel.basic_publish(exchange='',
                              routing_key=QUEUE_NAME,
                              body=destination)
        msg = 'Sent file {} to process'.format(destination)
        logging.info(msg)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    mkdirs()

    logging.info('Attempting connection to rabbitmq {}'.format(RABBITMQ_ADDR))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBITMQ_ADDR))
    logging.info('Connected to {}'.format(RABBITMQ_ADDR))
    channel = connection.channel()
    # Make sure queue is there
    channel.queue_declare(queue=QUEUE_NAME)
    logging.info('queue {} declared'.format(QUEUE_NAME))

    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE  # watched events
    notifier = pyinotify.Notifier(wm, EventHandler())
    wm.add_watch('/opt/data/collector/files', mask, rec=True)
    notifier.loop()
    channel.close()
    connection.close()
