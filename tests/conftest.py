import pytest
from io import BytesIO

root_file1_name = 'dfile.txt'
root_file2_name = 'cfile.txt'
subdir_name = 'test_dir/'
subdir_file1_name = 'test_dir/test_file.txt'
root_file1_contents = 'file contents'
root_file2_contents = 'file2 contents'
subdir_file1_contents = 'subdir file contents'


@pytest.fixture
def multiple_files(filesystem):
    filesystem.create_file(root_file1_name, BytesIO(bytes(root_file1_contents, 'utf-8')))
    filesystem.create_file(root_file2_name, BytesIO(bytes(root_file2_contents, 'utf-8')))
    filesystem.create_file(subdir_file1_name, BytesIO(bytes(subdir_file1_contents, 'utf-8')))
    yield
    filesystem.delete(root_file1_name)
    filesystem.delete(root_file2_name)
    filesystem.delete(subdir_file1_name)


@pytest.fixture
def single_file(filesystem):
    filesystem.create_file(root_file1_name, BytesIO(bytes(root_file1_contents, 'utf-8')))
    yield
    filesystem.delete(root_file1_name)


@pytest.fixture
def cleanup_files(filesystem):
    to_cleanup = []
    yield to_cleanup
    for file in to_cleanup:
        filesystem.delete(file)
