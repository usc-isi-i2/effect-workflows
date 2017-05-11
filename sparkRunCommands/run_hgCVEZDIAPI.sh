spark-submit \
   --master local[1] \
    --deploy-mode client \
    --py-files python-lib.zip \
    hgCVEZDIAPI.py \
    --output "hg_cve1" \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "hyperiongray" \
    --password KSIDOOIWHJu8ewhui8923y8gYGuYGASYUHjksahuihIHU \
    --date $1