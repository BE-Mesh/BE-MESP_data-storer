from .utilities.singleton import Singleton


class Storage(metaclass=Singleton):
    def __init__(self):

        self.database_path = ''