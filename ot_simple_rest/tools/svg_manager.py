import os
from tools.name_resolver import FileNameResolver


class SVGManager:
    """
    Responsible for filesystem manipulation relating to SVG files
    """

    def __init__(self, file_directory: str):
        self.file_directory = file_directory
        self.name_resolver = FileNameResolver(file_directory)

    def write(self, filename: str, file: bytes) -> str:
        """
        Write given file to disk
        """
        new_filename = self.name_resolver.resolve_filename_with_suffix(filename)
        if new_filename is not None:
            full_filename = os.path.join(self.file_directory, new_filename)
            f = open(full_filename, "wb")
            f.write(file)
        return new_filename

    def delete(self, filename: str):
        """
        Delete file from disk
        """
        if os.path.exists(os.path.join(self.file_directory, filename)):
            os.remove(os.path.join(self.file_directory, filename))
            return True
        else:
            return False
