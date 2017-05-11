WORKFLOWSBASE=/mnt/github/dig-workflows

CONDA_PY_DEST=effect-env/lib/python2.7/site-packages
#conda env create -f environment.yml
rm -rf effect-env
rm effect-env.zip
rm python-lib.zip

conda create -m -p $(pwd)/effect-env/ --copy --clone effect-env
cp cdrLoader.py $CONDA_PY_DEST/
mkdir $CONDA_PY_DEST/digWorkflow
cp $WORKFLOWSBASE/pySpark-workflows/digWorkflow/* $CONDA_PY_DEST/digWorkflow/
CUR_DIR="$PWD"
cd $CONDA_PY_DEST
zip -r python-lib.zip *
mv python-lib.zip $CUR_DIR/
cd $CUR_DIR
zip -r effect-env.zip effect-env

