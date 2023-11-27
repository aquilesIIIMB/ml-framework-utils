from interface import Interface

# pylint: disable=super-init-not-called
class RepositoryInterface(Interface):
    def __init__(self, config):
        pass

    def write(self, dataframe, target_name):
        pass

    def read(self, query_template, args=None):
        pass

    def commit(self):
        pass

    def close(self):
        pass
