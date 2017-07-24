# effect-workflows
DIG workflow processing for the EFFECT project.

## Installation

1. Download and install conda - https://www.continuum.io/downloads. Example for 64 bit linux:
   a. `wget https://repo.continuum.io/archive/Anaconda2-4.4.0-Linux-x86_64.sh`
   b. `bash Anaconda2-4.4.0-Linux-x86_64.sh`
   c. `source ~/.bashrc`
2. Install conda env - `conda install -c conda conda-env`
3. Clone this repo: `git clone https://github.com/usc-isi-i2/effect-workflows.git`
4. `cd effect-workflows`
5. Create user `effect` on hdfs. Add folders `\user\effect\data` and '\user\effect\workflow` and give all users write permission to those folders
6. Run the install script `.\install.sh`
7. To copy CDR data from existing machine, follow instructions in `copyCDR.txt`
8. Import all workflows in oozie\*.json into you oozie using Hue and schedule the coordinators

<B>NOTE: You should build the environment on the same hardware/os you're going to run the job</B>


## Running script to convert CSV,JSON,XML,CDR data into a format that should be used for Karma Modeling
1. Switch to the effect-env: `source activate effect-env`
2. Execute:

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

1. See hiveQueries.sql for examples
2. See copyCDR.txt to copy all data from one hive install to another

## Running the workflow

1. The `install.sh` script will build all jars and files required to run the workflow
2. `cp sparkRunCommands\run_effectWorkflow.sh .\`
3. `.\run_effectWorkflow.sh`
This will load data from HIVE table CDR, apply karma models to it and save the output to HDFS.

To load the data to ES, 

1. `cp sparkRunCommands\run_effectWorkflow-es.sh .\`
3. `.\run_effectWorkflow-es.sh`


## Extras

* To remove the environment run `conda env remove -n effect-env`
* To see all environments run `conda env list`

** Run OOZIE workflow from command line - takes in job.properties and workflow.xml

