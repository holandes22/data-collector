import os
import sys
import time
import sched
import logging

import pika
import requests
from bs4 import BeautifulSoup
from etcdc.client import Client


# pylint: disable=invalid-name,no-self-use,logging-format-interpolation
RABBITMQ_ADDR = os.getenv('RABBITMQ_SERVICE_HOST', 'localhost')
QUEUE_NAME = 'processor'
PROCESSOR_FILES_PATH = '/opt/data/processor/queue'


class AuditsDownloader(object):

    def __init__(self, dst_path, hooks=None):
        self.dst_path = dst_path
        self.session = requests.Session()
        self.url = None
        if hooks:
            self.hooks = hooks
        else:
            self.hooks = {}

    def set_url(self, addr):
        self.url = 'http://{}/logs'.format(addr)

    @property
    def filenames(self):
        if not self.url:
            return []
        logs = self.session.get(self.url)
        soup = BeautifulSoup(logs.content, 'html.parser')
        files = []
        for anchor_tag in soup.select('a[href^=/logs/hdfs-audit.log.]'):
            files.append(anchor_tag.text.strip())
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

    def add_to_queue(self, filepath):
        channel.basic_publish(exchange='',
                              routing_key=QUEUE_NAME,
                              body=filepath)
        msg = 'Placed in queue file {}'.format(filepath)
        logging.info(msg)

    def set_url(self):
        if not self.addr:
            client = Client()
            try:
                self.addr = client.get('/hdfs/addr').value
                self.downloader.set_url(self.addr)
            except KeyError:
                logging.info('No hdfs address set yet')

    def run(self):
        self.set_url()
        #TODO: Avoid starting if currently downloading
        self.downloader.download(callback=self.add_to_queue)

    def run_periodically(self, delay):
        self.run()
        scheduler = sched.scheduler(time.time, time.sleep)
        while True:
            scheduler.enter(delay, 1, self.run, [])
            scheduler.run()


def mkdirs():
    for d in [PROCESSOR_FILES_PATH]:
        try:
            os.makedirs(d)
        except OSError:
            pass


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

    # Start loop runner
    collector = Collector(PROCESSOR_FILES_PATH)
    collector.run_periodically(5)

    channel.close()
    connection.close()
