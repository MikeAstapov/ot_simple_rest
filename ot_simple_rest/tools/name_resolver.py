import os
from typing import Optional


class FileNameResolver:
    """
    Responsible for resolving naming conflicts
    """

    def __init__(self, file_directory: str):
        self.file_directory = file_directory

    def resolve_filename_with_suffix(self, filename: str) -> Optional[str]:
        if not os.path.exists(self.file_directory):
            raise FileNotFoundError(f'Target directory {self.file_directory} not found')
        full_filename = os.path.join(self.file_directory, filename)
        if not os.path.exists(full_filename):
            return filename
        else:
            i = 1
            name = '.'.join(full_filename.split('.')[0:-1])
            if name == '':
                name = full_filename.split('.')[-1]
                extension = ''
            else:
                extension = '.' + full_filename.split('.')[-1]
            while True:
                tmp_name = name + f'_{i}' + extension
                if not os.path.exists(tmp_name):
                    return os.path.basename(tmp_name)
                i += 1

    def resolve_filename_no_duplicate(self, filename: str) -> Optional[str]:
        full_filename = os.path.join(self.file_directory, filename)
        if not os.path.exists(full_filename):
            return filename
        else:
            return None

    def resolve_filename_rewrite(self, filename: str) -> Optional[str]:
        return filename

