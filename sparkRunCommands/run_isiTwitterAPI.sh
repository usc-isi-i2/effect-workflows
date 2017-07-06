spark-submit \
   --master local[1] \
    --deploy-mode client \
    --py-files python-lib.zip \
    isiTwitter.py \
    --output "isi_twitter" \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "isi" \
    --date $1
    --userData False