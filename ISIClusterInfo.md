Physical Server Information is [here](EffectCluster.md)

## Production Cluster
Cloudera Mgr: http://cloudmgr01.isi.edu:7180/cmf/login

OOZIE: http://cloudmgr03.isi.edu:8888/oozie/

SENSE: http://cloudweb01.isi.edu/app/sense

* Coordinators run everyday to download data from various APIs into the hive table `CDR` - Coordiantor-ASU, Coordinator-Ruhr, Coordinator-HG, Coordinator-ISI-News

* Coordinator for Karma runs at 6:10am PST to get the data increment, apply karma, reduce and frame the entire dataset and produce a daily effect index - **`effect-<date>`**

* An email is sent when the Karma workflow completes. (Configured in workflow `KarmaPublishES`)

* SLA is setup to send an email if the workflow does not complete by 12:51pm PST (Configured in Settings of workflow `KarmaPublishES`). 
  Need to contact Tim at Lockheed (Siedlecki, Timothy <timothy.siedlecki@lmco.com>) and let him know why it din't complete and if it can complete by 3pm PST. He can delay the run if it can still complete by 3pm PST.
  If it will not complete, Tim needs to use the previous day's index to run the forecasting models. 
  Make sure the backup is done for the index that Tim uses as the backup happens at 4pm PST
  
* Email is sent if any of the ES nodes are down or any index is red. The monitoring is done hourly. 
   * Machine: research@cloudmgr01
   * Script: `/local/research/checkESHealth.py` - Set emails here
   * Script is run using a cron job (`sudo crontab -e`)

* At 4pm PST, the effect-<date> index is backed up in cloudmgr01:`/data/lockheed/upload/data-<date>.json.gz`. If the backup does not produce the correct data file, and email will be sent and you will need to generate the backup again.
   * Machine: cloudweb01
   * Script: `/local/research/es_backup.sh` to backup and `/local/research/verifyESBackup.py` to verify the backups get generated correctly. Set emails in verifyESBackup.py script. Scripts configured using `sudo crontab -e`
   * Change and run `/local/research/es_backup_specific.sh` to generate the backup for a specific day. It generates the backup in current folder. Move it to the lockheed folder when done.
   
 * At 3pm PST, cooridnator API-Audit-Cooridnator computes statistics about the data it received from all APIs. The statistics are emailed. 
   Emails can be configured in this file - http://cloudmgr03.isi.edu:8888/filebrowser/view=/user/effect/workflow/hive-scripts/send_email.py#p1

## Cloudweb01 - Nginx and sftp Server
* This server hosts nginx and is also an sftp server from where lockheed gets the ES backups. 
* Any data that Lockheed wants to share with us is also shared using sftp and comes in the `/data/lockheed/upload` folder.
* nginx confifuration is in `/etc/nginx/conf.d/elasticsearch.conf`

## Effect Dashboard
* http://cloudweb01.isi.edu/graph/
* This runs on cloudsrch01 - Check if the process `main_dashboard.py` is running
  ```
  cd  /data/github/effect-dashboard/dashboard
  source dashboardenv/bin/activate
  nohup python main_dashboard.py &
  ```
  
## Effect Ablation Server
 * This runs on cloudsrch01 - Check if the process `main_ablation.py` is running
    ```
    cd  /data/github/effect-ablation-server
    source venv/bin/activate
    nohup python main_ablation.py &
    ```
    

## Development Cluster
Clouder Mgr: http://cldtestmgr01.isi.edu:7180/cmf/login

SENSE: http://128.9.35.104:5601/app/sense