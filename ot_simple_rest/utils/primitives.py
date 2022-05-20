
class RestUser:

    def __init__(self, name: str, _id: int):
        self.name = name
        self.id = _id

    def __str__(self):
        return f'user name={self.name} with ID={self.id}'


class EverythingEqual:

    def __eq__(self, other):
        return True

    def __lt__(self, other):
        return False

    def __ne__(self, other):
        return False

    def __gt__(self, other):
        return False

    def __str__(self):
        return '*'

    def __repr__(self):
        return self.__str__()
    
    def find(self, *args):
        return -1
