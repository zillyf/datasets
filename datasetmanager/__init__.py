__version__ = '1.1'


def hello_world():
    print("Hello World, Dataset Manager!")

def create_dataset(name):
    import os

def add_image(filename):
    # check MD5 write MD5
    return image_hash

def add_image_meta(namespace, json):
    # create json
    pass

def get_item():
    pass


from torch.utils.data import Dataset

class DSMDataItem():
    def __init__(self, sha, image_filename, transform):
        self.filename = image_filename
        self.sha = sha
        self.transform = transform
    
    @property
    def image(self):
        from PIL import Image
        # open method used to open different extension image file
        im = Image.open(self.filename)
        # TODO: implement transform
        return im


class DSMImageDataset(Dataset):
    """Load Image Dataset from Folder"""

    def calc_sha(self, filename):
        import hashlib
        with open(filename,"rb") as f:
            bytes = f.read() # read entire file as bytes
            readable_hash = hashlib.sha256(bytes).hexdigest();
        return readable_hash

    def write_namespace(self, key_name):
        import pickle
        with open(self.root_dir+self.dataset_meta+key_name+'.pk', 'wb') as f:
            pickle.dump(self.__dict__[key_name],f)
        return
    
    def exists_namespace(self, key_name):
        import os.path
        if os.path.isfile(self.root_dir+self.dataset_meta+key_name+'.pk'):
            return True
        else:
            return False
    
    def create_namespace(self, key_name):
        self.__dict__[key_name]=[ [] for _ in range(len(self.shalist)) ]
        self.write_namespace(key_name)
        self.namespaces.append(key_name)

    def write_shalist(self):
        self.write_namespace('shalist')
    
    def write_filelist(self):
        self.write_namespace('filelist')

    def import_file(self, filename):
        sha = self.calc_sha(filename)
        if sha not in self.shalist:
            self.shalist.append(sha)
            assert filename not in self.filelist
            self.filelist.append(filename)

    def init_namespace(self, namespace):
        import pickle
        with open(self.root_dir+self.dataset_meta+namespace+'.pk', 'rb') as f:
            self.__dict__[namespace]=pickle.load(f)
            self.namespaces.append(namespace)
            
    def init_namespaces(self):
        import os
        for namespace_pk in os.listdir(self.root_dir+self.dataset_meta):
            if namespace_pk.endswith(".pk"):
                namespace=namespace_pk.strip('.pk')
                if namespace not in self.namespaces:
                    print(namespace)
                    self.init_namespace(namespace)
        
    def import_folder(self, folder, pattern):
        import glob
        filelist=glob.glob(folder+'/'+pattern)
        for filename in filelist:
            self.import_file(filename)
        self.write_shalist()
        self.write_filelist()

    def init_datafolder(self):
        import os.path
        if os.path.isdir(self.root_dir+self.dataset_meta):
            pass
        else:
            os.mkdir(self.root_dir+self.dataset_meta)

    def init_shalist(self):
        self.shalist=[]
        if self.exists_namespace('shalist'):
            pass
        else:
            self.create_namespace('shalist')
        return
 

    def init_filelist(self):
        self.filelist=[]
        if self.exists_namespace('filelist'):
            pass
        else:
            self.create_namespace('filelist')
    
    def __init__(self, root_dir, transform=None):
        self.root_dir = root_dir
        self.dataset_meta='dataset_meta/'
        self.init_datafolder()
        self.transform = transform
        # Ensure existance of mandatory namespaces
        self.namespaces=[]
        self.init_shalist()
        self.init_filelist()
        # Load namespaces
        self.init_namespaces()
            
        assert len(self.shalist)==len(self.filelist)
        
    def __len__(self):
        return len(self.shalist)

    def __getitem__(self, idx):
        if isinstance(idx, int):
            assert idx>=0
            assert idx<len(self.shalist)

            data_item = DSMDataItem(self.shalist[idx], self.filelist[idx], self.transform)
            for namespace in self.namespaces:
                data_item.__dict__[namespace]=self.__dict__[namespace][idx]
            return data_item

        if isinstance(idx, str):
            int_idx=self.shalist.index(idx)
            return self.__getitem__(int_idx)
