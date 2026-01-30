from pathlib import Path
import os


class ResolvePaths:
    def __init__(self):
        self.os_user = os.getenv("USERNAME")

    def resolve_path(self, path_template: str = None) -> Path:
        return Path(path_template.format(username=self.os_user)).resolve()
