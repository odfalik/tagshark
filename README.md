source venv/bin/activate

sudo systemctl start elasticsearch
sudo systemctl start kibana

localhost
    :4040 Spark
    :9200 ES
    :5601 Kibana
    