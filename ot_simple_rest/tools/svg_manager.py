import os
from typing import Optional

WRITE_OK = 0
WRITE_FAIL = 1

DELETE_OK = 0
DELETE_FAIL = 1


class SvgNameResolver:

    def __init__(self, file_directory: str):
        self.file_directory = file_directory

    def resolve_filename(self, filename: str) -> Optional[str]:
        full_filename = os.path.join(self.file_directory, filename)
        if not os.path.exists(full_filename):
            return filename
        else:
            i = 1
            name = '.'.join(full_filename.split('.')[0:-1])
            extension = '.' + full_filename.split('.')[-1]
            while True:
                tmp_name = name + f'_{i}' + extension
                if not os.path.exists(tmp_name):
                    return tmp_name
                i += 1


class SVGManager:

    def __init__(self, file_directory: str):
        self.file_directory = file_directory
        self.name_resolver = SvgNameResolver(file_directory)

    def write(self, filename: str, file: bytes) -> str:
        new_filename = self.name_resolver.resolve_filename(filename)
        if new_filename is not None:
            full_filename = os.path.join(self.file_directory, new_filename)
            f = open(full_filename, "wb")
            f.write(file)
        return new_filename

    def delete(self, filename: str):
        if os.path.exists(os.path.join(self.file_directory, filename)):
            os.remove(os.path.join(self.file_directory, filename))
            return DELETE_OK
        else:
            return DELETE_FAIL
