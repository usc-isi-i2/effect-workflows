spark-submit \
   --master local[1] \
    --deploy-mode client \
    --py-files python-lib.zip \
    isiNews.py \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "isi" \
    --date $1
