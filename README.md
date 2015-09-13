# data-collector

sudo docker build -t data-collector .
sudo docker run -d -it --rm -v /host/path/to/opt/:/opt data-collector

folder struct of mounted volume should be:

opt/
    /collector
        /files
    /processor
        /queue
        /processed

To check it works, place a file under opt/collector/files and it should be copied to opt/processor/queue

You can inspect logs:

sudo docker exec -i -t 3d00312236bb bas
less /var/log/collector.log
