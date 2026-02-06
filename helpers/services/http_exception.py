from fastapi import HTTPException


class HTTP_Exceptions:
    @staticmethod
    def http_502(msg: str, e: Exception) -> HTTPException:
        return HTTPException(status_code=502, detail=f"{msg}: {e}")

    @staticmethod
    def http_500(msg: str, e: Exception) -> HTTPException:
        return HTTPException(status_code=500, detail=f"{msg}: {e}")