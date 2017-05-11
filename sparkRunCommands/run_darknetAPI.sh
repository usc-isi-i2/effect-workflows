spark-submit \
   --master local[1] \
    --deploy-mode client \
    --py-files python-lib.zip \
    darknetAPIToJl.py \
    --output "asu_darknet" \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "asu" \
    --password 7a417415-d5f8-4883-82b6-b55c3a0e3d3a \
    --date $1