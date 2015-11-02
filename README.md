# data-collector

sudo docker build -t data-collector .

folder struct of mounted volume should be:

data/
    /collector
        /files
    /processor
        /queue
        /processed
    /log
    /run


You can inspect logs:

sudo docker exec -i -t <id> bash
less data/log/collector.log


For testing, we can use this image:

docker run --net=host -v /path/to/audits:/var/log/hadoop-hdfs/audits -t -d caioquirino/docker-cloudera-quickstart
