# effect-workflows
DIG workflow processing for the EFFECT project.

## Installation

1. Download and install conda - https://www.continuum.io/downloads
2. Install conda env - `conda install -c conda conda-env`
3. Create the environment `conda env create`. This will create a virtual environment named effect-env (The name is defined in environment.yml)
4. Switch to the environment using `source activate effect-env`
5. Run the scripts
6. To exit the environment, do `source deactivate`
7. To execute the code on a cluster, you would need to attach the environment as a zip file. To export the environment, you can do `conda create -m -p /home/user1/effect-env --copy --clone effect-env`. This copies the effect-env into the location specified by the `-p` parameter.
   Then zip the environment

   ```
   cd /home/user1
   zip -r effect-env.zip effect-env
   spark-submit --archives effect-env.zip ......
   ```
<B>NOTE: You should build the environment on the same hardware/os you're going to run the job</B>


## Running script to convert PostgreSQL to CDR
1. Follow above instructions to create conda environment - Steps 1-3
2. Switch to the effect-env: `source activate effect-env`
3. Execute: 

  ```
  python postgresToCDR.py --host<postgreSQL hostname> --user <db username> --password <db password> \
                          --database <databasename> --table <tablename> --output <output filename>`
  ```


## Extras

* To remove the environment run `conda env remove -n effect-env`
* To see all environments run `conda env list`
