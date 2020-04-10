#!/usr/bin/env python3

# Unpack and organize newly-uploaded files

import glob
import numpy as np
import os
import pandas as pd
import sys

from utils import get_chunks, format_batchnum

os.chdir('/afs/hep.wisc.edu/bechtol-group/ArtifactSpy')

# Read list of already processed metadata files
list_file = open('already_sorted.txt', 'r')
already_sorted_timestamps = [x.strip() for x in list_file.readlines()]
list_file.close()

# Read list of all metadata files
metadata_files = glob.glob('Results/Metadata/*.csv')
metadata_timestamps = [x.split('/')[-1].split('.')[0] for x in metadata_files]

# Get list of timestamps needing to be sorted
remaining_timestamps = list(set(metadata_timestamps) - set(already_sorted_timestamps))

# Get list of all existing batches
batches = glob.glob('ImageBank/Batches/batch--*')
batch_nums = [int(x.split('--')[1]) for x in batches]

next_batch_num = np.max(batch_nums) + 1

# Build up a map of batch numbers to objids
map_data = []

for timestamp in remaining_timestamps:
    
    # Unpack the image tarball
    os.chdir('ImageBank/Tarballs')
    os.system('tar -xzf {}.tar.gz'.format(timestamp))

    # Get a list of the unpacked objids
    objids = [x.split('srch')[-1].split('.')[0] for x in glob.glob(timestamp + '/srch*.gif')]
    os.chdir('../..')

    # Split list of objids into chunks
    split_objids = get_chunks(objids, 100)
    
    # Go through the chunks and organize images
    for batch in split_objids:

        # Make the batch directory
        str_batch_num = format_batchnum(next_batch_num)
        batch_name = 'ImageBank/Batches/batch--' + str_batch_num + '--' + timestamp
        os.system('mkdir ' + batch_name)

        # Move images into the directory
        for individual_objid in batch:
            os.system('cp ImageBank/Tarballs/{2}/*{0}.gif {1}'.format(individual_objid, batch_name, timestamp))
            # Track objid to batch relationship
            map_data.append([next_batch_num, individual_objid])

        # Tar up the batch
        os.chdir('ImageBank/Batches')
        os.system('tar -czf batch--' + str_batch_num + '--' + timestamp + '.tar.gz ' + 'batch--' + str_batch_num + '--' + timestamp)
        os.system('rm -rf batch--' + str_batch_num + '--' + timestamp)
        os.chdir('../..')

        # Update the batch number
        next_batch_num += 1    

    # Append timestamp to already_sorted.txt
    list_file = open('already_sorted.txt', 'a')
    list_file.write(timestamp + '\n')
    list_file.close()    

    # Remove the unpacked directory
    os.system('rm -r ImageBank/Tarballs/' + timestamp)

# Save map of objids and batches
objid_batch_df = pd.DataFrame(data=map_data, columns=['BATCH', 'OBJID'])
if os.path.exists('batch_objid_map.csv'):
    existing_objid_batch_df = pd.read_csv('batch_objid_map.csv')
    objid_batch_df = pd.concat([existing_objid_batch_df, objid_batch_df])
objid_batch_df.to_csv('batch_objid_map.csv', index=False)

# Initialize log.txt if it doesn't exist yet
if not os.path.exists('log.txt'):
    log_file = open('log.txt', 'w+')
    log_file.write('CURRENT: batch--00001--{}.tar.gz'.format(metadata_timestamps[0]))
    log_file.close()

    # Queue up the first batch
    os.system('cp ImageBank/Batches/batch--00001--{0}.tar.gz CURRENT--batch--00001--{1}.tar.gz'.format(metadata_timestamps[0], metadata_timestamps[0]))
