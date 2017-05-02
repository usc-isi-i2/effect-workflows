WORKFLOWSBASE=/mnt/github/dig-workflows

CONDA_PY_DEST=effect-env/lib/python2.7/site-packages
rm -rf effect-env
conda create -m -p $(pwd)/effect-env/ --copy --clone effect-env
cp cdrLoader.py $CONDA_PY_DEST/
mkdir $CONDA_PY_DEST/digWorkflow
cp $WORKFLOWSBASE/pySpark-workflows/digWorkflow/* $CONDA_PY_DEST/digWorkflow/ 
zip -r effect-env.zip effect-env
