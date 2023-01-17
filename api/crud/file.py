from api.core.filesystem import get_filesystem


def list_user_files(user_id: str, path: str = ''):
    filesystem = get_filesystem(user_id)
    return filesystem.list_directory(path)


def upload_file(user_id: str, path: str, file):
    filesystem = get_filesystem(user_id)
    filesystem.create_file(path, file)
    return filesystem.get_file_info(path)


def rename_file(user_id: str, path: str, new_path: str):
    filesystem = get_filesystem(user_id)
    filesystem.rename(path, new_path)
    return filesystem.get_file_info(new_path)


def delete_user_file(user_id: str, path: str):
    filesystem = get_filesystem(user_id)
    return filesystem.delete(path)
