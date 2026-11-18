# hydra_gen

Same pipeline as `../hydra`, published by generation: each run writes both reports into
`output/gen/<generation>/` (the caller passes `--param generation=<new name>`), then the last step
switches `output/manifest.json` with `../shared/publish_generation.py`. No change to the engine.

    export GEN=g$(date +%s%N); hdrctl run . --param generation=$GEN

Readers must resolve the manifest once and read both reports from the generation it names.
