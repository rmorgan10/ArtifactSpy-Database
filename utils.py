# Functions for ArtifactSpy Database

import datetime
from email.mime.text import MIMEText
import glob
import numpy as np
import os
import pandas as pd
import platform
import smtplib
import sys


#def chunks(lst, n):
#    """
#    A generator to break a list into chunks of size n
#    """
#    for i in range(0, len(lst), n):
#        yield lst[i:i + n]

#def get_chunks(lst, n):
#    """
#    Return a list split into chunks of size n
#    """
#    chunked_list = list(chunks(lst, n))
#    # Take the last chunk and distribute it's elements into previous chunks
#    # -- this assures there will never be a small chunk
#    equal_chunks = chunked_list[0:-1]
#    for i in range(len(chunked_list[-1])):
#        equal_chunks[i].append(chunked_list[-1][i])
#    return equal_chunks

def get_chunks(lst, n):
    """
    Break a list (lst) into n chunks.
    """
    chunk_size = len(lst) // n
    chunks = [[]] * n
    start, end = (0, chunk_size)
    
    for i in range(n):
        # sort elements in equal size chunks
        chunks[i] = lst[start:end]
        start = end + 0
        end += chunk_size
    
    # pick up all the remaining elements
    for i, element in enumerate(lst[start:]):
        chunks[i].append(element)
        
    return chunks

def format_batchnum(num):
    """
    Make sure the batch number has the right number of leading zeros
    """
    str_num = str(num)
    while len(str_num) < 5:
        str_num = '0' + str_num
    return str_num

def send_email(body, subject, sender, receiver):
    """
    Send an email
    """

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    try:
        sm = smtplib.SMTP('localhost')
        sm.sendmail(sender, receiver, msg.as_string())
    except smtplib.SMTPException:
        sys.exit("ERROR sending mail")


def backup(filename):
    """
    Attach date to filename and copy to backup directory.
    Assumes filename has one '.' character.
    """
    base_filename = filename.split('/')[-1]
    (prefix, suffix) = base_filename.split('.')
    date_string = "{:%y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())

    backup_filename = prefix + '_' + date_string + '.' + suffix
    os.system('cp {0} BACKUP/{1}'.format(filename, backup_filename))

def evaluate(df):
    """
    Compose a report by comparing ramorgan2 labels to user
    """
    message_body = "Matched Classification Summary\n\n"

    # Look at each user independently
    users = np.unique(df['USER'].values)
    for user in users:
        user_df = df[df['USER'].values == user].copy().reset_index(drop=True)

        # Get overall accuracy
        total = user_df.shape[0]
        correct = np.sum(user_df['ACTION'].values == user_df['LABEL'].values)

        # Write full report
        report = "USER\tACTION\tLABEL\tSNID\tOBJID\n"
        for index, row in user_df.iterrows():
            report += str(row['USER']) + '\t'
            report += str(row['ACTION']) + '\t'
            report += str(row['LABEL']) + '\t'
            report += str(row['SNID']) + '\t'
            report += str(row['OBJID']) + '\t'
            report += '\n'

        message_body += 'USER: ' + user + '\t\t' + str(correct) + '/' + str(total) 
        message_body += '\n' + '-' * 80 + '\n'
        message_body += report

    email_address = "robert.morgan@wisc.edu"
    message_body += "\n\nLove,\n\nArtifactSpy"
    subject = "Classification Report!"
    send_email(message_body, subject, "ArtifactSpy@{}".format(platform.node()), email_address)

def queue_up_unsure(objids):
    """
    Prepare a directory of unsure objids to be labelled
    - note that we only need the label from me since the metadata already has been collected
    """
    # Read in master list
    master_df = pd.read_csv('batch_objid_map.csv')

    # Iterate through the objid list and sort images accordingly
    for objid in objids:
        batches = master_df['BATCH'].values[master_df['OBJID'].values == objid]
        for batch in batches:

            batch_tarball = glob.glob('ImageBank/Batches/batch--{}--*.tar.gz'.format(format_batchnum(int(batch))))[0]
            batch_dir_name = batch_tarball.split('/')[-1].split('.')[0]

            # open up the tarball
            os.system('tar -xzf ' + batch_tarball)
            
            # copy the stamps for the objid to the staging directory
            os.system('cp {0}/*{1}.gif ImageBank/Unsure'.format(batch_dir_name, objid))
            
            # clean up the unpacked tarball
            os.system('rm -r ' + batch_dir_name)
        
    # After we get enough unsure objids, tar them up and send me an email
    os.chdir('ImageBank/Unsure')
    if len(glob.glob('*.gif')) >= 60:
        # Create tarball
        dir_name = 'UNSURE_{:%y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
        os.system('mkdir ' + dir_name)
        os.system('mv *.gif ' + dir_name)
        os.system('tar -czf {0}.tar.gz {1}'.format(dir_name, dir_name))
        os.system('rm -r ' + dir_name)
        
        # Compose an email
        email_address = 'robert.morgan@wisc.edu'
        subject = "UNSURE images are ready!"
        body = "Howdy Partner!\n\nI got some images for you to look at. "
        body += "Here's where those boys are:\n\n"
        body += "\t" + dir_name + '.tar.gz\n\n'
        body += "Best of luck! Love,\n\nArtifactSpy"
        send_email(body, subject, "ArtifactSpy@{}".format(platform.node()), email_address)
    
