conda env create -f environment.yml
rm -rf effect-env
conda create -m -p $(pwd)/effect-env/ --copy --clone effect-env
zip -r effect-env.zip effect-env