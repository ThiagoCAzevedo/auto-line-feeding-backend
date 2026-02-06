from fastapi import APIRouter, File, UploadFile
from services.static.storage import ListExcelFiles, UploadFiles, DeleteFiles


router = APIRouter()


@router.get("/list/", summary="Get Files In Excel Folder")
def list_files():
    return ListExcelFiles()._list_files()

@router.post("/upload/", summary="Upload File in Excel Folder")
def upload_files(file: UploadFile = File(...)):
    return UploadFiles()._upload_files(file)

@router.delete("/delete/{filename}", summary="Delete File in Excel Folder")
def delete_files(filename):
    return DeleteFiles()._delete_files(filename)
