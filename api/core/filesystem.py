import os
import shutil
import enum
import abc
from collections import namedtuple
from pathlib import PurePosixPath

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

    def list_directory(self, path: str = ""):
        normalized_path = path if path.endswith("/") else path + "/"
        if not self.isdir(normalized_path):
            raise NotADirectoryError(path)
        return self._directory_contents(normalized_path)

    def _directory_contents(self, path: str):
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

    def full_path(self, path: str):
        # For some reason, PurePosixPath returns the root path if one of the components has root path
        full = str(PurePosixPath(self.root_path, path[1:] if path.startswith("/") else path))
        return full if not path.endswith("/") else full + "/"


class LocalFilesystem(FileSystem):
    """ A filesystem that uses the local filesystem. """

    def init(self):
        os.makedirs(self.root_path, exist_ok=True)

    def _directory_contents(self, path: str):
        files = os.listdir(self.full_path(path))
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


class S3Filesystem(FileSystem):
    """ A filesystem that uses S3. """
    def __init__(self, root_path: str, s3_client, bucket):
        super().__init__(root_path)
        self.s3_client = s3_client
        self.bucket = bucket

    def init(self):
        self.s3_client.put_object(Bucket=self.bucket, Key=self.root_path + '/')

    def _directory_contents(self, path: str):
        # Get contents of S3 directory
        full_path = self.full_path(path)
        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.bucket, 'Prefix': full_path, 'Delimiter': '/'}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for key in page.get('CommonPrefixes', []):
                yield FileInfo(
                    path=key['Prefix'][len(self.root_path)+1:],
                    type=FileTypes.directory,
                    size=''
                )
            for key in page.get('Contents', []):
                if key['Key'] == full_path:
                    continue
                yield FileInfo(
                    path=key['Key'][len(self.root_path)+1:],
                    type=FileTypes.file,
                    size=humanize.naturalsize(key['Size'])
                )

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


def get_filesystem(user_id: str):
    """ Get the filesystem to use. """
    if settings.filesystem == 's3':
        s3_client = boto3.client('s3')
        return S3Filesystem(settings.user_data_root_path + user_id, s3_client, 'decode-test')
    elif settings.filesystem == 'local':
        return LocalFilesystem(settings.user_data_root_path + user_id)
    else:
        raise ValueError('Invalid filesystem setting')
