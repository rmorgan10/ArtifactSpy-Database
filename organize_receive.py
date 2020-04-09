#!/usr/bin/env python3

# Organize classification results received from user

import datetime
import glob
import numpy as np
import os
import pandas as pd
import platform
import sys

os.chdir('/afs/hep.wisc.edu/bechtol-group/ArtifactSpy')

from utils import backup, evaluate, queue_up_unsure, send_email

# Read list of already received results
log_file = open('already_received.txt', 'r')
already_received_files = [x.strip() for x in log_file.readlines()]
log_file.close()

# Collect list of all result files
all_files = [x.split('/')[-1] for x in glob.glob('Results/UserResults/*.csv')]

# Get remaining result files
remaining_files = list(set(all_files) - set(already_received_files))

# Iterate through and analyze new files
unsure_objids = []
result_dfs = []
users, totals = [], []
for remaining_file in remaining_files:

    # Get identity of user
    hep_username = remaining_file.split('--')[0]
    users.append(hep_username)

    # Load results into a dataframe
    df = pd.read_csv('Results/UserResults/' + remaining_file)
    if df.shape[0] == 0:
        del df
        os.system('rm Results/UserResults/' + remaining_file)
        continue

    totals.append(df.shape[0])

    # Collect objids of stamps marked "Unsure" by user
    unsure_images = list(df['OBJID'].values[df['ACTION'].values == 'Unsure'])
    unsure_objids += unsure_images

    # Determine the unique tarball timestamps that link to the metadata
    metadata_timestamps = []
    for stamp_list in df['METADATA_STAMP'].values:
        metadata_timestamps += stamp_list.split(',')
    unique_metadata_timestamps = np.unique(metadata_timestamps)

    # For each timestamp, read in the metadata, then merge with objids
    for metadata_timestamp in unique_metadata_timestamps:

        md_df = pd.read_csv('Results/Metadata/{}.csv'.format(metadata_timestamp))
        merged_df = md_df.merge(df, on='OBJID').copy()
        
        # Handle the case where it's the wrong metadata stamp
        if merged_df.shape[0] == 0:
            continue

        merged_df['USER'] = hep_username
        result_dfs.append(merged_df)
    
    # Append to log_file
    log_file = open('already_received.txt', 'a')
    log_file.write(remaining_file + '\n')
    log_file.close()

# Concatenate merged dfs
result_df = pd.concat(result_dfs).reset_index(drop=True)

# Append to master resutls file
if os.path.exists('Results/master_results.csv'):
    backup('Results/master_results.csv')
    master_df = pd.read_csv('Results/master_results.csv')
    updated_master = pd.concat([master_df, result_df])
else:
    updated_master = result_df.copy()

updated_master.to_csv('Results/master_results.csv', index=False)
backup('Results/master_results.csv')

# Check for overlap between ramorgan2 and new submissions
ramorgan2_df = pd.read_csv('Results/ramorgan2/my_labels.csv')
ramorgan2_df_merged = ramorgan2_df.merge(result_df, on='OBJID')
if ramorgan2_df_merged.shape[0] != 0:
    evaluate(ramorgan2_df_merged)

# Queue up the unsure images for me
if len(unsure_objids) != 0:
    queue_up_unsure(unsure_objids)

# Send me an email with some stats
user_data = {}
for user, total in zip(users, totals):
    if user in user_data.keys():
        user_data[user] = user_data[user] + total
    else:
        user_data[user] = total

email_address = "robert.morgan@wisc.edu"
subject = "I got more data!"
body = "Yo! I just got some results!\n\n"
for user, total in user_data.items():
    body += user + ': ' + str(total) + '\n'
body += "\nUntil next time! Love,\n\nArtifactSpy"
send_email(body, subject, "ArtifactSpy@{}".format(platform.node()), email_address)
