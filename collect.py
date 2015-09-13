import os
import shutil
import logging

import pika
import pyinotify

# pylint: disable=invalid-name,no-self-use
RABBITMQ_ADDR = 'localhost'
QUEUE_NAME = 'processor'


class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        filename = os.path.basename(event.pathname)
        destination = os.path.join('/opt/processor/queue/', filename)
        shutil.copy(event.pathname, destination)
        msg = 'Copied file {} to {}'.format(filename, destination)
        logging.info(msg)
        channel.basic_publish(exchange='',
                              routing_key=QUEUE_NAME,
                              body=destination)
        logging.info('Sent file {} to process'.format(destination))


if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/collector.log', level=logging.DEBUG)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBITMQ_ADDR)
    )
    logging.info('Connected to {}'.format(RABBITMQ_ADDR))
    channel = connection.channel()
    # Make sure queue is there
    channel.queue_declare(queue=QUEUE_NAME)
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE  # watched events
    notifier = pyinotify.Notifier(wm, EventHandler())
    wm.add_watch('/opt/collector/files', mask, rec=True)
    notifier.loop()
    channel.close()
    connection.close()
