from threading import Thread
from orchestrator.pipeline_registry import PIPELINES
from orchestrator.workers_registry import WORKERS


# -- PIPELINES - WILL RUN ONLY WHEN CALLED --
class PipelinesOrchestrator:
    def run_pipeline(self, name):
        PIPELINES[name]() 
        return {"status": f"service {name} executed successfully (sync)"}

    def run_pipeline_async(self, name):
        thread = Thread(target=PIPELINES[name], daemon=True)
        thread.start()
        return {"status": f"service {name} executed successfully (async)"}
    

# -- WORKERS - WILL RUN EVERYTIME --
class WorkersOrchestrator:
    def __init__(self):
        self.running_workers = {}

    def start_worker(self, name):
        if name in self.running_workers:
            return "workers already running"

        workers = WORKERS[name]
        thread = Thread(target=workers, daemon=True)

        self.running_workers[name] = {
            "workers": workers,
            "thread": thread
        }

        thread.start()
        return "workers started"

    def stop_workers(self, name):
        if name not in self.running_workers:
            return "workers not running"

        workers_data = self.running_workers[name]
        workers = workers_data["workers"]
        workers.stop()
        return "workers stopped"