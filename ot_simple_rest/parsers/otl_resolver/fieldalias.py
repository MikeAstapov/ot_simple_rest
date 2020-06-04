import re


class FieldAlias:

    def __init__(self, file_path=None):
        self.file_path = file_path
        self.lines = None

    def get_aliases(self, field):
        if self.lines is None:
            with open(self.file_path) as fr:
                self.lines = fr.read().split('\n')
        aliases = []
        for line in self.lines:
            if re.search(field, line, re.IGNORECASE):
                aliases.append(line.split(',')[0])
        return aliases
