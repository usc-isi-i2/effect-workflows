# effect-workflows
DIG workflow processing for the EFFECT project.

## Installation

1. Download and install conda - https://www.continuum.io/downloads
2. Install conda env - `conda install -c conda conda-env`
3. Create the environment `conda env create`. This will create a virtual environment named effect-env (The name is defined in environment.yml)
4. Switch to the environment using `source activate effect-env`

<B>NOTE: You should build the environment on the same hardware/os you're going to run the job</B>


## Running script to convert PostgreSQL to CDR
1. Follow above instructions to create conda environment - Steps 1-3
2. Switch to the effect-env: `source activate effect-env`
3. Execute: 

  ```
  python postgresToCDR.py --host <postgreSQL hostname> --user <db username> --password <db password> \
                          --database <databasename> --table <tablename> \
                          --output <output filename> --team <Name of team providing data>`
  ```


## Running script to convert CSV,JSON,XML,CDR data into a format that should be used for Karma Modeling
1. Follow above instrucrions to create conda environment - Steps 1-3
2. Switch to the effect-env: `source activate effect-env`
3. Execute:

  ```
  python generateDataForKarmaModeling.py --input <input filename> --output <output filename> \
        --format <input format-csv/json/xml/cdr> --source <a name for the source> \
        --separator <column separator for CSV files>
  ```

  Example Invocations:
  ```
  python generateDataForKarmaModeling.py --input ~/github/effect/effect-data/nvd/sample/nvdcve-2.0-2003.xml \
            --output nvd.jl --format xml --source nvd


  python generateDataForKarmaModeling.py --input ~/github/effect/effect-data/hackmageddon/sample/hackmageddon_20160730.csv \
            --output hackmageddon.jl --format csv --source hackmageddon


  python generateDataForKarmaModeling.py --input ~/github/effect/effect-data/hackmageddon/sample/hackmageddon_20160730.jl \
            --output hackmageddon.jl --format json --source hackmageddon
  ```

## Loading data in HIVE

1. Login to AWS and create a tunnel - `ssh -L 8888:localhost:8888 hadoop@ec2-52-42-169-124.us-west-2.compute.amazonaws.com`
2. Access Hue on http://localhost:8888
3. See hiveQueries.sql for examples

## Running the workflow

To build the python libraries required by the workflows,

1. Edit make.sh and update the path to `dig-workflows`
2. Run `./make.sh`. This will create `lib\python-lib.zip` that can be attached with the `--py-files` option to the spark workflow
3. Copy the `python-lib.zip` file to AWS - `scp lib/python-lib.zip hadoop@ec2-52-42-169-124.us-west-2.compute.amazonaws.com:/home/hadoop/effect-workflows/lib`
4. zip your karma home folder into `karma.zip` and copt to AWS - `scp karma.zip hadoop@ec2-52-42-169-124.us-west-2.compute.amazonaws.com:/home/hadoop/effect-workflows/`
5. Build a shaded karma-spark jar -

   ```
   cd karma-spark
   mvn clean install -P shaded -Denv=hive
   scp lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar hadoop@ec2-52-42-169-124.us-west-2.compute.amazonaws.com:/home/hadoop/effect-workflows/lib
   ```

6. Login to AWS and run the workflow

```
ssh -L 8888:localhost:8888 hadoop@ec2-52-42-169-124.us-west-2.compute.amazonaws.com
spark-submit --deploy-mode client  \
    --jars "/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --conf "spark.driver.extraClassPath=/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    --archives /home/hadoop/effect-workflows/karma.zip
    /home/hadoop/effect-workflows/effectWorkflow.py \
    cdr hdfs://ip-172-31-19-102/user/effect/data/cdr-framed sequence 10
```
This will load data from HIVE table CDR, apply karma models to it and save the output to HDFS.

To load the data to ES, 

1. Create an index, say effect-2 with mappings from file - https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/es/es-mappings.json

2. Run spark workflow to load data from hdfs to this effect-2 index
    ```
    spark-submit --deploy-mode client  \
        --executor-memory 5g \
        --driver-memory 5g \
        --jars "/home/hadoop/effect-workflows/jars/elasticsearch-hadoop-2.4.0.jar" \
        --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
        /home/hadoop/effect-workflows/effectWorkflow-es.py \
        --host 172.31.19.102 \
        --port 9200 \
        --index effect-2 \
        --doctype attack \
        --input hdfs://ip-172-31-19-102/user/effect/data/cdr-framed/attack
    ```
    This shows how to add in the attack frame. This needs to executed for all the available frames.
    
3. Change the alias 'effect' in ES to point to this new index - effect-2
    ```
    POST _aliases
    {
      "actions": [
        {
          "add": {
            "index": "effect-2",
            "alias": "effect"
          },
          "remove": {
            "index": "effect-1",
            "alias": "effect"
          }
        }
      ]
    }
    ```

## Running the Extractor Workflow

1. Follow the Installation Instructions to install conda and conda-env if you dont hav ethem installed
2. Create the effect environement `conda env create`
3. Switch to the environment using `source activate effect-env`
4. Run `.\make-extractor.sh`. This bundles up the entire environemnt, including python that is used to run the workflow
5. If spark is not installed in the default `/usr/lib/spark/`, change paths in `run-extractor.sh`
6. Run `run-extractor.sh`


## Extras

* To remove the environment run `conda env remove -n effect-env`
* To see all environments run `conda env list`

