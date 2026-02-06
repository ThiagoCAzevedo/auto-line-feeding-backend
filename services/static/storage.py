from os import listdir
from os.path import isfile, join
from dotenv import load_dotenv
import os


load_dotenv("config/.env")


class ListExcelFiles:
    def _list_files(self):
        return [f for f in listdir(os.getenv("EXCEL_PATH")) if isfile(join(os.getenv("EXCEL_PATH"), f))]


class UploadFiles:
    def _upload_files(self, file):
        content = file.file.read()
        with open(f"{os.getenv('EXCEL_PATH')}/{file.filename}", "wb") as f:
            f.write(content)

        return {"filename": file.filename, "size": len(content)}


class DeleteFiles:
    def _delete_files(self, filename):
        os.remove(os.path.join(os.getenv("EXCEL_PATH"), filename))
        return {"message": f"Arquivo '{filename}' removido com sucesso."}