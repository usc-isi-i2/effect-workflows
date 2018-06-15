spark-submit \
   --master local[1] \
    --deploy-mode client \
    --py-files python-lib.zip \
    darknetAPIToJl.py \
    --output "asu_darknet" \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "asu" \
    --password Nyd7NYLGt8DC \
    --date $1