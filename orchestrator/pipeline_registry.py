from services.pipelines.pkmc.pipeline import pkmc_pipeline
from services.pipelines.pk05.pipeline import pk05_pipeline


PIPELINES = {
    "pkmc": pkmc_pipeline,
    "pk05": pk05_pipeline,
}