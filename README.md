# data-collector

sudo docker build -t data-collector .
sudo docker run --net=host -d -it -v /host/path/to/opt/:/opt data-collector

`net=host` option in run, will give access to localhost, where rabbitmq is expected to be running. If we link at some pint
to the docker container of rabbitmq we can disregard this and connect directly to that


folder struct of mounted volume should be:

opt/
    /collector
        /files
    /processor
        /queue
        /processed

To check it works, place a file under opt/collector/files and it should be copied to opt/processor/queue

You can inspect logs:

sudo docker exec -i -t <id> bash
less /var/log/collector.log
