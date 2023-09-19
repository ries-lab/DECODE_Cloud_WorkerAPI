import os
import shutil
import enum
import abc
import io
import zipfile
from collections import namedtuple
from fastapi.responses import FileResponse, StreamingResponse
from pathlib import Path, PurePosixPath

import humanize
import boto3

import api.settings as settings


class FileTypes(enum.Enum):
    file = "file"
    directory = "directory"


FileInfo = namedtuple("File", ["path", "type", "size"])


class FileSystem(abc.ABC):
    def __init__(self, root_path: str):
        self.root_path = root_path

    def init(self):
        raise NotImplementedError()

    def list_directory(self, path: str = "", dirs: bool = True, recursive: bool = False):
        normalized_path = path if path.endswith("/") else path + "/"
        if not self.isdir(normalized_path):
            raise NotADirectoryError(path)
        return self._directory_contents(normalized_path, dirs=dirs, recursive=recursive)

    def _directory_contents(self, path: str, dirs: bool = True, recursive: bool = False):
        raise NotImplementedError()

    def get_file_info(self, path: str):
        raise NotImplementedError()

    def create_file(self, path: str, file):
        raise NotImplementedError()

    def rename(self, path: str, new_name: str):
        if not self.exists(path):
            raise FileNotFoundError(path)
        if self.isdir(path):
            raise IsADirectoryError('Cannot rename a directory')
        return self._rename_file(path, new_name)

    def _rename_file(self, path: str, new_name: str):
        raise NotImplementedError()

    def delete(self, path: str, reinit_if_root: bool = True):
        if not self.exists(path):
            return
        if self.isdir(path):
            self._delete_directory(path)
            if (path == "/" or path == "") and reinit_if_root:
                self.init()
        else:
            self._delete_file(path)

    def _delete_file(self, path: str):
        raise NotImplementedError()

    def _delete_directory(self, path: str):
        raise NotImplementedError()

    def exists(self, path: str):
        raise NotImplementedError()

    def isdir(self, path: str):
        raise NotImplementedError()

    def full_path_uri(self, path: str):
        raise NotImplementedError()

    def full_path(self, path: str):
        # For some reason, PurePosixPath returns the root path if one of the components has root path
        full = str(PurePosixPath(self.root_path, path[1:] if path.startswith("/") else path))
        return full if not path.endswith("/") else full + "/"

    def download(self, path: str):
        raise NotImplementedError()


class LocalFilesystem(FileSystem):
    """ A filesystem that uses the local filesystem. """

    def init(self):
        os.makedirs(self.root_path, exist_ok=True)

    def _directory_contents(self, path: str, dirs: bool = True, recursive: bool = False):
        if not recursive:
            files = os.listdir(self.full_path(path))
        else:
            files = [os.path.relpath(str(f), self.full_path(path)) for f in Path(self.full_path(path)).rglob("*")]
        if not dirs:
            files = [f for f in files if not os.path.isdir(os.path.join(self.full_path(path), f))]
        for file in files:
            yield self.get_file_info((path if path != '/' else '') + file)

    def get_file_info(self, path: str):
        """ Get file info. """
        metadata = os.stat(self.full_path(path))
        isdir = self.isdir(path)
        return FileInfo(
            path=path + '/' if isdir else path,
            type=FileTypes.directory if isdir else FileTypes.file,
            size=humanize.naturalsize(metadata.st_size) if not isdir else ''
        )

    def create_file(self, path, file):
        dir_path = os.path.split(path)[0]
        if not self.exists(dir_path):
            os.makedirs(self.full_path(dir_path))
        with open(self.full_path(path), 'wb') as f:
            shutil.copyfileobj(file, f)

    def delete(self, path: str, reinit_if_root: bool = True):
        if not self.exists(path):
            return
        super().delete(path, reinit_if_root)
        # Delete empty directories
        path = path[:-1] if path.endswith('/') else path
        dir_path = '/'.join(path.split('/')[:-1])
        if dir_path != '' and not os.listdir(self.full_path(dir_path)):
            self.delete(dir_path)

    def _rename_file(self, path, new_path):
        os.rename(self.full_path(path), self.full_path(new_path))

    def _delete_file(self, path):
        os.remove(self.full_path(path))

    def _delete_directory(self, path):
        shutil.rmtree(self.full_path(path))

    def exists(self, path):
        """ Check if a path exists. """
        return os.path.exists(self.full_path(path))

    def isdir(self, path):
        """ Check if a path is a directory. """
        return os.path.isdir(self.full_path(path))

    def full_path_uri(self, path):
        return self.full_path(path)
    
    def download(self, path):
        if not self.exists(path):
            return None
        if self.isdir(path):
            zip_io = io.BytesIO()
            with zipfile.ZipFile(zip_io, mode="w", compression=zipfile.ZIP_DEFLATED) as temp_zip:
                for fpath in self.list_directory(path, dirs=False, recursive=True):
                    fpath = str(fpath.path)
                    temp_zip.write(self.full_path(fpath), os.path.relpath(fpath, path))
            return StreamingResponse(
                iter([zip_io.getvalue()]),
                media_type="application/x-zip-compressed",
                headers={"Content-Disposition": f"attachment; filename={path[:-1]}.zip"},
            )
        else:
            return FileResponse(self.full_path(path))


class S3Filesystem(FileSystem):
    """ A filesystem that uses S3. """
    def __init__(self, root_path: str, s3_client, bucket):
        super().__init__(root_path)
        self.s3_client = s3_client
        self.bucket = bucket

    def init(self):
        self.s3_client.put_object(Bucket=self.bucket, Key=self.root_path + '/')

    def _directory_contents(self, path: str, dirs: bool = True, recursive: bool = False):
        # Get contents of S3 directory
        full_path = self.full_path(path)
        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.bucket, 'Prefix': full_path}
        operation_parameters['Delimiter'] = '/'
        page_iterator = paginator.paginate(**operation_parameters)
    
        for page in page_iterator:
            for key in page.get('Contents', []):
                if key['Key'] == full_path:
                    continue
                yield FileInfo(
                    path=key['Key'][len(str(self.root_path))+1:],
                    type=FileTypes.file,
                    size=humanize.naturalsize(key['Size'])
                )
            for key in page.get('CommonPrefixes', []):
                dir_path = key['Prefix'][len(str(self.root_path))+1:]
                if dirs:
                    yield FileInfo(path=dir_path, type=FileTypes.directory, size='')
                if recursive:
                    for ret in self._directory_contents(dir_path, dirs=dirs, recursive=recursive):
                        yield ret

    def get_file_info(self, path: str):
        metadata = self.s3_client.head_object(Bucket=self.bucket, Key=self.full_path(path))
        return FileInfo(
            path=path,
            type=FileTypes.file,
            size=humanize.naturalsize(metadata['ContentLength'])
        )

    def create_file(self, path, file):
        # Upload file to S3 efficiently
        self.s3_client.upload_fileobj(file, self.bucket, self.full_path(path))

    def _rename_file(self, path, new_path):
        # Rename file on S3
        self.s3_client.copy_object(Bucket=self.bucket, Key=self.full_path(new_path),
                                   CopySource={'Bucket': self.bucket, 'Key': self.full_path(path)})
        self.s3_client.delete_object(Bucket=self.bucket, Key=self.full_path(path))

    def _delete_file(self, path):
        # Delete a file from S3
        self.s3_client.delete_object(Bucket=self.bucket, Key=self.full_path(path))

    def _delete_directory(self, path):
        # Delete entire folder from S3
        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.bucket, 'Prefix': self.full_path(path)}
        page_iterator = paginator.paginate(**operation_parameters)
        delete_keys = {'Objects': []}
        for page in page_iterator:
            for key in page.get('Contents'):
                delete_keys['Objects'].append({'Key': key['Key']})
        self.s3_client.delete_objects(Bucket=self.bucket, Delete=delete_keys)

    def exists(self, path):
        # Check if there is any S3 object with the given path as prefix
        objects = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.full_path(path), MaxKeys=1)
        return 'Contents' in objects

    def isdir(self, path):
        return self.exists(path) if path.endswith('/') else False

    def full_path_uri(self, path):
        return 's3://' + self.bucket + '/' + self.full_path(path)
    
    def download(self, path):
        if not self.exists(path):
            return None
        _get_file_content = lambda path: self.s3_client.get_object(Bucket=self.bucket, Key=self.full_path(path))["Body"]
        if self.isdir(path):
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for fpath in self.list_directory(path, dirs=False, recursive=True):
                    fpath = str(fpath.path)
                    zipf.writestr(os.path.relpath(fpath, path), _get_file_content(fpath).read())
            zip_buffer.seek(0)
            headers = {
                "Content-Disposition": f"attachment; filename={path[:-1]}.zip",
                "Content-Type": "application/zip",
            }
            return StreamingResponse(io.BytesIO(zip_buffer.read()), headers=headers)
        else:
            return StreamingResponse(content=_get_file_content(path).iter_chunks())


def get_filesystem_with_root(root_path: str):
    """ Get the filesystem to use. """
    if settings.filesystem == 's3':
        s3_client = boto3.client('s3')
        return S3Filesystem(root_path, s3_client, settings.s3_bucket)
    elif settings.filesystem == 'local':
        return LocalFilesystem(root_path)
    else:
        raise ValueError('Invalid filesystem setting')


def get_user_filesystem(user_id: str):
    """ Get the filesystem to use for a user. """
    return get_filesystem_with_root(str(Path(settings.user_data_root_path) / user_id))
