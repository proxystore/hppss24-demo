

# Sunspot Setup

```
qsub -l select=1 -l walltime=02:00:00 -A CSC249ADCD08_CNDA -q workq -I
```

```bash
$ module load frameworks/2024.1
$ conda config --set allow_non_channel_urls True
$ conda env create --file environment.yml
$ conda activate hppss24
```
