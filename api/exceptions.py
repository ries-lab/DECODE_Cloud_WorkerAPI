from fastapi.responses import JSONResponse


def register_exception_handlers(app):
    @app.exception_handler(NotADirectoryError)
    async def not_a_directory_exception_handler(request, exc):
        return JSONResponse(status_code=404, content={"detail": "Path is not a directory"})

    @app.exception_handler(IsADirectoryError)
    async def is_a_directory_exception_handler(request, exc):
        return JSONResponse(status_code=404, content={"detail": str(exc)})
