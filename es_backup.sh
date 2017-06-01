DATE=`date +%Y%m%d`

elasticdump --input http://128.9.35.71:9200/effect-$DATE --output /data/lockheed/upload/mapping-$DATE.json --type=mapping

elasticdump --input http://128.9.35.71:9200/effect-$DATE --output=$ | gzip > /data/lockheed/upload/data-$DATE.json.gz