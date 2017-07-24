spark-submit \
   --master local[1] \
    --deploy-mode client \
    --py-files python-lib.zip \
    ruhrCerberAPI.py \
    --output "ruhr_cerber" \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "ruhr" \
    --password mhaWupyXVQcnkLEWXPrLBXgcnzgVBaEa \
    --date $1
