import pytest
import boto3
from io import BytesIO
from .conftest import root_file1_name, root_file2_name, subdir_name, subdir_file1_name, \
    root_file1_contents, root_file2_contents, subdir_file1_contents


from api.core.filesystem import S3Filesystem, FileTypes, FileInfo, FileSystem, LocalFilesystem

user_dir = 'test_user_dir'


@pytest.fixture(scope='class', params=['local', pytest.param('s3', marks=pytest.mark.aws)])
def filesystem_uninit(request):
    if request.param == 'local':
        return LocalFilesystem(user_dir)
    else:
        s3_client = boto3.client('s3')
        return S3Filesystem(user_dir, s3_client, 'decode-test')


@pytest.fixture(scope="class")
def filesystem(filesystem_uninit):
    filesystem_uninit.init()
    yield filesystem_uninit
    filesystem_uninit.delete('/', reinit_if_root=False)


class TestFilesystemBase:
    @pytest.fixture(scope='class')
    def filesystem_base(self):
        return FileSystem(user_dir)

    def test_list_directory_file(self, filesystem_base, monkeypatch):
        monkeypatch.setattr(filesystem_base, 'isdir', lambda path: False)
        with pytest.raises(NotADirectoryError):
            filesystem_base.list_directory(root_file1_name)

    def test_rename_nonexistent_raises(self, filesystem_base, monkeypatch):
        monkeypatch.setattr(filesystem_base, 'exists', lambda path: False)
        with pytest.raises(FileNotFoundError):
            filesystem_base.rename(root_file1_name, 'new_name')

    def test_rename_directory_fails(self, filesystem_base, monkeypatch):
        monkeypatch.setattr(filesystem_base, 'exists', lambda path: True)
        monkeypatch.setattr(filesystem_base, 'isdir', lambda path: True)
        with pytest.raises(IsADirectoryError):
            filesystem_base.rename(root_file1_name, 'new_name')

    def test_delete_idempotent(self, filesystem_base, monkeypatch):
        monkeypatch.setattr(filesystem_base, 'exists', lambda path: False)
        filesystem_base.delete(root_file1_name)


class TestFilesystem:
    def test_init(self, filesystem_uninit):
        filesystem_uninit.init()
        assert filesystem_uninit.exists('/')

    def test_list_directory_empty(self, filesystem):
        files = list(filesystem.list_directory('/'))
        assert files == []

    def test_list_directory_root_empty_string(self, filesystem):
        files = list(filesystem.list_directory(''))
        assert files == []

    def test_list_directory(self, filesystem, multiple_files):
        files = list(filesystem.list_directory('/'))
        assert len(files) == 3
        assert FileInfo(subdir_name, FileTypes.directory, '') in files
        assert FileInfo(root_file2_name, FileTypes.file, '{} Bytes'.format(len(root_file2_contents))) in files
        assert FileInfo(root_file1_name, FileTypes.file, '{} Bytes'.format(len(root_file1_contents))) in files

    def test_list_directory_subdir(self, filesystem, multiple_files):
        files = list(filesystem.list_directory(subdir_name))
        assert len(files) == 1
        assert files[0] == FileInfo(subdir_file1_name, FileTypes.file,
                                    '{} Bytes'.format(len(subdir_file1_contents)))

    def test_get_file_info(self, filesystem, multiple_files):
        info = filesystem.get_file_info(root_file1_name)
        assert info == FileInfo(root_file1_name, FileTypes.file, '{} Bytes'.format(len(root_file1_contents)))

    def test_create_file(self, filesystem, cleanup_files):
        filesystem.create_file(root_file1_name, BytesIO(bytes(root_file1_contents, 'utf-8')))
        cleanup_files.append(root_file1_name)
        assert filesystem.exists(root_file1_name)
        
    def test_create_file_subdir(self, filesystem, cleanup_files):
        filesystem.create_file(subdir_file1_name, BytesIO(bytes(subdir_file1_contents, 'utf-8')))
        cleanup_files.append(subdir_file1_name)
        assert filesystem.exists(subdir_file1_name)
        
    def test_rename(self, filesystem, single_file, cleanup_files):
        filesystem.rename(root_file1_name, root_file2_name)
        cleanup_files.append(root_file2_name)
        assert filesystem.exists(root_file2_name)
        assert not filesystem.exists(root_file1_name)

    def test_exists_true_for_existing_file(self, filesystem, single_file):
        assert filesystem.exists(root_file1_name)

    def test_exists_true_for_existing_directory(self, filesystem, multiple_files):
        assert filesystem.exists(subdir_name)

    def test_exists_false_for_nonexistent_object(self, filesystem):
        assert not filesystem.exists(root_file1_name)

    def test_exists_root(self, filesystem):
        assert filesystem.exists('/')

    def test_isdir_true_for_existing_directory(self, filesystem, multiple_files):
        assert filesystem.isdir(subdir_name)

    def test_isdir_false_for_existing_file(self, filesystem, single_file):
        assert not filesystem.isdir(root_file1_name)

    def test_isdir_false_for_nonexistent_object(self, filesystem):
        assert not filesystem.isdir(root_file1_name)

    def test_isdir_root(self, filesystem):
        assert filesystem.isdir('/')

    def test_delete_file(self, filesystem, single_file):
        filesystem.delete(root_file1_name)
        assert not filesystem.exists(root_file1_name)

    def test_delete_directory(self, filesystem, multiple_files):
        filesystem.delete(subdir_name)
        assert not filesystem.exists(subdir_name)

    def test_empty_directories_are_deleted(self, filesystem, multiple_files):
        filesystem.delete(subdir_file1_name)
        assert not filesystem.exists(subdir_name)
