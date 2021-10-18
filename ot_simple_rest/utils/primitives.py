
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
