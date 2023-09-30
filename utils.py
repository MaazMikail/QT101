import pickle
import lzma
import dill as pickle

def save_pickle(path, obj):
    with lzma.open(path, 'wb') as f:
        pickle.dump(obj, f)

def load_pickle(obj_path):
    with lzma.open(obj_path, 'rb') as f:
        file = pickle.load(f)
    return file