import os
import shutil
import enum

import humanize


class FileTypes(enum.Enum):
    file = "file"
    directory = "directory"


class LocalFilesystem:
    """ A filesystem that uses the local filesystem. """

    def __init__(self, root_path):
        self.root_path = root_path

    def list_directory(self, path=''):
        """ List the contents of a directory. """
        if not self.exists(path):
            raise FileNotFoundError(path)
        if not self.isdir(path):
            raise NotADirectoryError(path)
        for file in os.listdir(os.path.join(self.root_path, path)):
            yield self.get_file_info(os.path.join(path, file))

    def get_file_info(self, path):
        metadata = os.stat(os.path.join(self.root_path, path))
        dir_path, filename = os.path.split(path)
        isdir = self.isdir(path)
        return {
            'name': filename,
            'parent': dir_path + '/',
            'type': FileTypes.directory.value if isdir else FileTypes.file.value,
            'size': humanize.naturalsize(metadata.st_size) if not isdir else '',
            'date_created': metadata.st_ctime,
        }

    def isdir(self, path=''):
        """ Check if a path is a directory. """
        return os.path.isdir(os.path.join(self.root_path, path))

    def create_file(self, path, file):
        dir_path = os.path.split(path)[0]
        if not self.exists(dir_path):
            self.create_directory(dir_path)
        try:
            with open(os.path.join(self.root_path, path), 'wb') as f:
                shutil.copyfileobj(file, f)
        finally:
            file.close()

    def create_directory(self, path=''):
        """ Create a directory. """
        os.makedirs(os.path.join(self.root_path, path), exist_ok=True)

    def exists(self, path=''):
        """ Check if a path exists. """
        return os.path.exists(os.path.join(self.root_path, path))

    def rename(self, path, new_name):
        """ Rename a file or directory. """
        if not self.exists(path):
            raise FileNotFoundError(path)
        new_path = os.path.join(os.path.split(path)[0], new_name)
        os.rename(os.path.join(self.root_path, path), os.path.join(self.root_path, new_path))
        return new_path

    def delete(self, path):
        """ Delete a file or directory. """
        if not self.exists(path):
            raise FileNotFoundError(path)
        if self.isdir(path):
            shutil.rmtree(os.path.join(self.root_path, path))
        else:
            os.remove(os.path.join(self.root_path, path))
