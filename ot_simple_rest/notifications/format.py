
class Notification:

    def __init__(self, code=None, value=None):
        self.code = code
        self.value = value

    def as_dict(self):
        res = {}
        for key, value in self.__dict__.items():
            res.update({key: value})
        return res

