# Experiments

## Sunspot Setup

Create a conda environment with the necessary dependencies.
```bash
module load frameworks/2024.1
conda config --set allow_non_channel_urls True
conda env create --file environment.yml
conda activate hppss24
```

Certain data files need to be downloaded for the experiments to run.
```bash
mkdir data/moldesign
curl -o data/moldesign/QM9-search.tsv --create-dirs https://raw.githubusercontent.com/ExaWorks/molecular-design-parsl-demo/main/data/QM9-search.tsv
```

Scripts can be run on an interactive node.
```bash
qsub -l select=1 -l walltime=02:00:00 -A CSC249ADCD08_CNDA -q workq -I
```

## Running Experiments

Experiments can be run using the scripts in `scripts/`.
Configuration files for each experiment/application are provided in `configs/`.

## Analysis

Experiments will write output data to `runs/`.
Jupyter Notebooks are provided to analyze the run data.
```bash
jupyter-lab
```
