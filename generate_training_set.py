# A module to synthesize labels and images into a training set

import datetime
import glob
import os
import sys
import tarfile

import h5py
from PIL import Image
import numpy as np
import pandas as pd

from utils import format_batchnum

# Collect all labels (metadata already in master_labels)
master_labels = pd.read_csv('Results/master_results.csv').set_index("OBJID")
my_labels = pd.read_csv('Results/ramorgan2/my_labels.csv').set_index("OBJID")
unsure_labels = pd.concat([pd.read_csv(x) for x in glob.glob('Results/UnsureResults/*.csv')]).set_index("OBJID")

### Overwrite unsure and overlapping labels
my_labels['ACTION'] = my_labels['LABEL'].values
my_labels['USER'] = "ramorgan2"
master_labels.update(my_labels)
unsure_labels['USER'] = "ramorgan2"
master_labels.update(unsure_labels)
del unsure_labels, my_labels

### drop unnecessary columns
master_labels.drop(['Unnamed: 0', 'METADATA_STAMP'], axis=1, inplace=True)

### Collect all batch numbers
batch_objid_map = pd.read_csv('batch_objid_map.csv')
batch_objid_map.drop_duplicates(inplace=True)
batch_objid_map.set_index('OBJID', inplace=True)
master_labels = master_labels.join(batch_objid_map, on='OBJID', how='inner')

# Prepare data storage objects
class_map = {'Good': 0,
             'Marginal': 1,
             'Other': 2,
             'BadSubtraction': 3,
             'DarkSpotInTemplateCenter': 4,
             'NoisyTemplate': 5,
             'PsfInTemplate': 6}

class DataObject():
    def __init__(self):
        self.objid_list = []
        self.images = []
        return

data = {v: DataObject() for v in class_map.values()}

counter = 0.0
total = len(set(master_labels['BATCH'].values))
# Collect all images
batch_labels = master_labels.groupby("BATCH")
for batch, labels in batch_labels:
    # Output progress
    counter += 1.0
    progress = counter / total * 100
    sys.stdout.write("\rProgress: %.2f %%" %progress)
    sys.stdout.flush()

    # Read tarball
    batch_filename = glob.glob('ImageBank/Batches/batch--' + format_batchnum(batch) + '*.gz')[0]
    t = tarfile.open(batch_filename, 'r:gz')

    # Determine OBJIDs of interest
    batch_objids = [int(x.split('/')[-1][4:-4]) for x in t.getnames() if x.find('srch') != -1]
    valid_objids = list(set(batch_objids).intersection(set(labels.index)))

    # Sort data
    for objid in valid_objids:
        # Skip any lingering UNSURE images
        if labels.loc[objid]['ACTION'] not in class_map.keys():
            continue

        # Isolate images
        prefix = batch_filename.split('/')[-1][:-7] + '/'
        srch = np.array(Image.open(t.extractfile(prefix + "srch" + str(objid) + ".gif")))
        temp = np.array(Image.open(t.extractfile(prefix + "temp" + str(objid) + ".gif")))
        diff = np.array(Image.open(t.extractfile(prefix + "diff" + str(objid) + ".gif")))

        # Organize using the data storage objects
        data[class_map[labels.loc[objid]['ACTION']]].objid_list.append(objid)
        data[class_map[labels.loc[objid]['ACTION']]].images.append(np.array([srch, temp, diff]))

    t.close()

# Now attach the metadata
for v in data.values():
    v.metadata = master_labels.loc[v.objid_list]

# Create a directory for the training set
training_set_name = "TS__" + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
os.mkdir("TrainingSets/" + training_set_name)

# Save data
for k, v in data.items():
    # Save metadata
    v.metadata.to_csv("TrainingSets/" + training_set_name + '/Class_' + str(k) + '_metadata.csv')

    # Save OBJIDs
    listfile = open("TrainingSets/" + training_set_name + '/Class_' + str(k) + '_objids.txt', 'w+')
    listfile.writelines([str(x) + '\n' for x in v.objid_list])
    listfile.close()

    # Save images
    hf = h5py.File("TrainingSets/" + training_set_name + '/Class_' + str(k) + '_images.h5', 'w')
    hf.create_dataset("Class_" + str(k), data=np.array(v.images))
    hf.close()

# Compress the training set
os.chdir('TrainingSets')
os.system('tar -czf ' + training_set_name + '.tar.gz ' + training_set_name)
os.system('rm -rf ' + training_set_name)
