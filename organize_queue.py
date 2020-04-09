#!/usr/bin/env python3

# Handle all data in the DES_DATA directory

import datetime
import glob
import os
import platform
import sys

from utils import format_batchnum, send_email, backup

os.chdir('/afs/hep.wisc.edu/bechtol-group/ArtifactSpy')

def running_low_on_data(new_next_batch_num):
    """
    return True if more data needs to be added to the bank
    """

    # Get all batch numbers
    batches = [x for x in glob.glob('ImageBank/Batches/*.tar.gz') if x.find('CURRENT') == -1]
    batch_nums = [int(x.split('batch--')[-1].split('--')[0]) for x in batches]
    
    # Find the batches that haven't been queued up yet
    remaining_batches = [x for x in batch_nums if x > int(new_next_batch_num)]
    if len(remaining_batches) > 7:
        return False
    else:
        return True


# Backup log.txt
backup('log.txt')

# Read log.txt
log_file = open('log.txt', 'r')
lines = log_file.readlines()
log_file.close()

# Find next up line
current_line = [x for x in lines if x.split(':')[0] == 'CURRENT'][0]
current_batch = current_line.split(':')[1].strip()
current_batch_num = int(current_batch.split('--')[1])
next_batch_num = current_batch_num + 1

# Remove the CURRENT directory
os.system('rm ImageBank/Batches/CURRENT--{}'.format(current_batch))

# Find the tarball for the next batch (since timestamp may be different)
next_batch_list = glob.glob('ImageBank/Batches/batch--' + format_batchnum(next_batch_num) + '--*.tar.gz')
next_batch = next_batch_list[0].split('/')[-1]

# Generate the NEXTUP directory
os.system('cp ImageBank/Batches/{0} ImageBank/Batches/CURRENT--{1}'.format(next_batch, next_batch))

# Update log.txt
log_file = open('log.txt', 'w+')
log_file.write('CURRENT: ' + next_batch)
log_file.close()

# Backup log.txt
backup('log.txt')

# Check if running low on data
if running_low_on_data(next_batch_num):
    
    #send me an email
    email_address = 'robert.morgan@wisc.edu'
    body = 'Sup Fam.\n\nI would appreciate it if you could run grab_data.py when you get a chance.'
    body += '\n\nLove,\n\nArtifactSpy'
    subject = "I need more data"
    send_email(body, subject, "ArtifactSpy@{}".format(platform.node()), email_address)


