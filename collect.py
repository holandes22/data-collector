import os
import shutil
import logging

import pyinotify

# pylint: disable=invalid-name,no-self-use


class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        filename = os.path.basename(event.pathname)
        destination = os.path.join('/opt/processor/queue/', filename)
        shutil.copy(event.pathname, destination)
        msg = 'Copied file {} to {}'.format(filename, destination)
        logging.info(msg)


if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/collector.log', level=logging.DEBUG)
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE  # watched events
    notifier = pyinotify.Notifier(wm, EventHandler())
    wm.add_watch('/opt/collector/files', mask, rec=True)
    notifier.loop()
