source venv/bin/activate

sudo systemctl restart elasticsearch
sudo systemctl restart kibana

localhost
    :4040 Spark
    :9200 ES
    :5601 Kibana
    