import os
import sys
import time
import sched
import logging
from contextlib import contextmanager
from urllib.parse import urlparse

import pika
import requests
import rethinkdb
from bs4 import BeautifulSoup
from etcdc.client import Client


# pylint: disable=invalid-name,no-self-use,logging-format-interpolation

ETCD_SERVICE_HOST = os.getenv('ETCD0_SERVICE_HOST', 'localhost')
RABBITMQ_SERVICE_HOST = os.getenv('RABBITMQ_SERVICE_HOST', 'localhost')
RETHINKDB_SERVICE_HOST = os.getenv('RETHINKDB_SERVICE_HOST', 'localhost')
QUEUE_NAME = 'processor'
PROCESSOR_FILES_PATH = '/opt/data/processor/queue'


class AuditsDownloader(object):

    def __init__(self, dst_path):
        self.dst_path = dst_path
        self.session = requests.Session()
        # http://<hostname_or_ip_addr>:<port>/<path_to_logs>
        self.url = None

    @property
    def filenames(self):
        if not self.url:
            return []
        logs = self.session.get(self.url)
        soup = BeautifulSoup(logs.content, 'html.parser')
        files = []
        url_path = urlparse(self.url).path
        pattern = 'hdfs-'
        selector = 'a[href^={}]'.format(os.path.join(url_path, pattern))
        for anchor_tag in soup.select(selector):
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
        self.is_downloading = False
        self.etcd_client = Client(address=ETCD_SERVICE_HOST, port='2379')
        self.rconn = rethinkdb.connect(RETHINKDB_SERVICE_HOST, '28015')
        with self.blocking_channel() as channel:
            # Make sure queue is there
            channel.queue_declare(queue=QUEUE_NAME)
            logging.info('queue {} declared'.format(QUEUE_NAME))


    @contextmanager
    def blocking_channel(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_SERVICE_HOST))
        channel = connection.channel()

        yield channel

        channel.close()
        connection.close()

    def add_to_queue(self, filepath):
        with self.blocking_channel() as channel:
            channel.basic_publish(exchange='',
                                  routing_key=QUEUE_NAME,
                                  body=filepath)
        rethinkdb.db('data').table('collected').insert({'filepath': filepath}).run(self.rconn)
        logging.info('Placed in queue file {}'.format(filepath))

    def set_url(self):
        try:
            url = self.etcd_client.get('/data/collector/url').value
            if url.lower() not in ['none', 'null']:
                self.downloader.url = url
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
    logging.debug('etcd host: {}'.format(ETCD_SERVICE_HOST))
    logging.debug('rabbitmq host: {}'.format(RABBITMQ_SERVICE_HOST))
    logging.debug('rethinkdb host: {}'.format(RETHINKDB_SERVICE_HOST))

    # Start loop runner
    collector = Collector(PROCESSOR_FILES_PATH)
    collector.run_periodically()

