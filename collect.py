import os
import sys
import time
import sched
import logging
from contextlib import contextmanager

import pika
import requests
from bs4 import BeautifulSoup
from etcdc.client import Client


# pylint: disable=invalid-name,no-self-use,logging-format-interpolation
RABBITMQ_ADDR = os.getenv('RABBITMQ_SERVICE_HOST', 'localhost')
QUEUE_NAME = 'processor'
PROCESSOR_FILES_PATH = '/opt/data/processor/queue'


class AuditsDownloader(object):

    def __init__(self, dst_path):
        self.dst_path = dst_path
        self.session = requests.Session()
        self.url = None

    def set_url(self, addr):
        self.url = 'http://{}/logs'.format(addr)

    @property
    def filenames(self):
        if not self.url:
            return []
        logs = self.session.get(self.url)
        soup = BeautifulSoup(logs.content, 'html.parser')
        files = []
        for anchor_tag in soup.select('a[href^=/logs/hdfs-audit.log]'):
            files.append(anchor_tag.text.strip())
        logging.debug('Files: {}'.format(files))
        return files

    def download(self, callback=None):
        for filename in self.filenames:
            dst_filepath = os.path.join(self.dst_path, filename)
            if not os.path.exists(dst_filepath):
                resp = self.session.get(os.path.join(self.url, filename))
                try:
                    resp.raise_for_status()
                except requests.exceptions.HTTPError as err:
                    msg = 'Skipping file {} due to error: {} ({})'.format(
                        filename, err.message, resp.status_code)
                    logging.warn(msg)
                    continue
                with open(dst_filepath, 'wb') as fdescriptor:
                    for chunk in resp.iter_content(1024):
                        fdescriptor.write(chunk)
                if callback:
                    callback(dst_filepath)


class Collector(object):

    def __init__(self, dst_path):
        self.downloader = AuditsDownloader(dst_path)
        self.addr = None
        self.is_downloading = False
        self.etcd_client = Client()

    def add_to_queue(self, filepath):
        channel.basic_publish(exchange='',
                              routing_key=QUEUE_NAME,
                              body=filepath)
        logging.info('Placed in queue file {}'.format(filepath))

    def set_url(self):
        if not self.addr:
            try:
                self.addr = self.etcd_client.get('/data/collector/addr').value
                self.downloader.set_url(self.addr)
            except KeyError:
                logging.info('No hdfs address set yet')

    @contextmanager
    def downloading(self):
        self.is_downloading = True
        yield
        self.is_downloading = False

    @property
    def delay(self):
        try:
            return int(self.etcd_client.get('/data/collector/delay').value)
        except KeyError:
            return 60

    def run(self):
        self.set_url()
        with self.downloading():
            self.downloader.download(callback=self.add_to_queue)

    def run_periodically(self):
        self.run()
        scheduler = sched.scheduler(time.time, time.sleep)
        while True:
            if not self.is_downloading:  # Add to queue
                scheduler.enter(self.delay, 1, self.run, [])
                scheduler.run()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    logging.info('Attempting connection to rabbitmq {}'.format(RABBITMQ_ADDR))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBITMQ_ADDR))
    logging.info('Connected to {}'.format(RABBITMQ_ADDR))
    channel = connection.channel()
    # Make sure queue is there
    channel.queue_declare(queue=QUEUE_NAME)
    logging.info('queue {} declared'.format(QUEUE_NAME))

    # Start loop runner
    collector = Collector(PROCESSOR_FILES_PATH)
    collector.run_periodically()

    channel.close()
    connection.close()
