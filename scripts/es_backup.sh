DATE=`date +%Y%m%d`

elasticdump --input http://128.9.35.71:9200/effect-data-latest --output /data/lockheed/upload/mapping-$DATE.json --type=mapping

# elasticdump --limit 70 --input http://128.9.35.71:9200/effect-data-latest --output=$ | gzip > /data/lockheed/upload/data-$DATE.json.gz
elasticdump --limit 70 --input http://128.9.35.71:9200/effect-data-latest --output=$ > data-$DATE.json
gzip data-$DATE.json data-$DATE.json.gz
mv data-$DATE.json.gz /data/lockheed/upload/
rm data-$DATE.json

elasticdump --input http://128.9.35.71:9200/effect-gt --output /data/lockheed/upload/mapping-gt-$DATE.json --type=mapping
elasticdump --input http://128.9.35.71:9200/effect-gt --output=$ | gzip > /data/lockheed/upload/data-gt-$DATE.json.gz

elasticdump --input http://128.9.35.71:9200/effect-gt-hourly --output /data/lockheed/upload/mapping-gt-hourly-$DATE.json --type=mapping
elasticdump --input http://128.9.35.71:9200/effect-gt-hourly --output=$ | gzip > /data/lockheed/upload/data-gt-hourly-$DATE.json.gz
