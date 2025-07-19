import pickle

class Functions:
    @staticmethod
    def read_pickle(file):
        with open(file, 'rb') as f:
            return pickle.load(f)