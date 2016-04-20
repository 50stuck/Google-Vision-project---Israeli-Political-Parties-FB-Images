# [START import_libraries]
import base64
import os

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
# The url template to retrieve the discovery document for trusted testers.
DISCOVERY_URL='https://{api}.googleapis.com/$discovery/rest?version={apiVersion}'

import sqlite3

# [END import_libraries]

def getjpgsindir(tikia): #returns a list of all JPGs in a given directory
    jpgs = []
    dirlist = os.listdir(tikia)
    for item in dirlist:
        if os.path.isfile(os.path.join(tikia,item)):
            if item[-4:] == '.jpg':
                jpgs.append(os.path.join(tikia, item))
    return(jpgs)

def getdirsindir(tikia): #returns a list of all subdirectories in a given directory
    tikiot = []
    dirlist = os.listdir(tikia)
    for item in dirlist:
        if os.path.isdir(os.path.join(tikia,item)):
            tikiot.append(os.path.join(tikia,item))
    return(tikiot)

def getjpgsindirandsubdirs(basepath): #returns a list of all JPGs in a given directory and all of it subdirectories
    pathlist = [basepath]
    jpglist = []

    while len(pathlist)>0:
        for path in pathlist:
            jpgs = getjpgsindir(path)
            for jpg in jpgs:
                jpglist.append(jpg)
            subdirs = getdirsindir(path)
            for subdir in subdirs:
                pathlist.append(subdir)
            pathlist.remove(path)

    return(jpglist)

def getphotolabels(photo_file): #returns a list of all labels to a given photo on the computer, via the Google Vision API
    # [START authenticate]
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'C:\Users\Dror\my vision project-5ff74f9a411c.json' #### change path to Google App Credentials
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('vision', 'v1', credentials=credentials,
                              discoveryServiceUrl=DISCOVERY_URL)
    # [END authenticate]

    # [START construct_request] #runs a "get labels" query
    with open(photo_file, 'rb') as image:
        image_content = base64.b64encode(image.read())
        service_request = service.images().annotate(body={
            'requests': [{
                'image': {
                    'content': image_content.decode('UTF-8')
                },
                'features': [{
                    'type': 'LABEL_DETECTION',
                }]
            }]
        })
        # [END construct_request]
        # [START parse_response] #retrieves just the labels, and all the labels
        response = service_request.execute()
        alllabels=[]
        try:
            labelAnnotations = response['responses'][0]['labelAnnotations']
            for label in labelAnnotations:
                alllabels.append(label['description'])

            return alllabels
        except:
            print('problem...')
            return  []
        # [END parse_response]

def filetodb(filename, cur, db):
    #enter file to DB
    cur.execute('SELECT * FROM PICS WHERE PIC_FILE=?', (filename,))
    if cur.fetchone() is not None: #if picture already in DB
        print('picture ', filename, 'already in DB')
    else:
        print('entering picture ', filename, ' into DB')
        cur.execute('INSERT INTO PICS (PIC_FILE) VALUES (?)', (filename,))
    db.commit() #commit changes to the DB

def labeltodb(label, cur, db):
    cur.execute('SELECT * FROM GOOGLE_LABELS WHERE LABEL_NAME=?', (label,))
    if cur.fetchone() is not None: #if label already in DB
        print('label ', label, 'already in DB')
    else:
        print('entering label ', label, ' into DB')
        cur.execute('INSERT INTO GOOGLE_LABELS (LABEL_NAME) VALUES (?)', (label,))
    db.commit() #commit changes to the DB

def piclabellinktodb(photo, label, cur, db):
    print('entering ', photo, '-', label, ' link to DB')

    #get IDs
    cur.execute('SELECT * FROM PICS WHERE PIC_FILE=?', (photo,)) #get picture ID
    picid = int(list(cur.fetchone())[0])
    cur.execute('SELECT * FROM GOOGLE_LABELS WHERE LABEL_NAME=?', (label,)) #get label ID
    labelid = int(list(cur.fetchone())[0])

    cur.execute('INSERT INTO PIC_GOOGLE_LABELS (PIC_ID, LABEL_ID) VALUES (?,?)', (picid, labelid)) #enter pic_id-label_id link
    db.commit() #commit changes to the DB

def partytodb(party, cur, db):
    cur.execute('SELECT * FROM PARTIES WHERE PARTY_NAME=?', (party,))
    if cur.fetchone() is not None: #if party already in DB
        print('party ', party, 'already in DB')
    else:
        print('entering party ', party, ' into DB')
        cur.execute('INSERT INTO PARTIES (PARTY_NAME) VALUES (?)', (party,))
    db.commit() #commit changes to the DB

def picpartytodb(photo, party, cur, db):
    print('entering ', photo, '-', party, ' link to DB')

    #get IDs
    cur.execute('SELECT * FROM PICS WHERE PIC_FILE=?', (photo,)) #get picture ID
    picid = int(list(cur.fetchone())[0])
    cur.execute('SELECT * FROM PARTIES WHERE PARTY_NAME=?', (party,)) #get party ID
    partyid = int(list(cur.fetchone())[0])

    cur.execute('INSERT INTO PIC_PARTY (PIC_ID, PARTY_ID) VALUES (?,?)', (picid,partyid))
    db.commit() #commit changes to the DB

def main(basepath):
    # [START connecting to local sqlite DB, create relevant tables, etc.]
    db = sqlite3.connect('party_profile_pics_db.sqlite') ####change to DB you actually want to connect to
    cur = db.cursor()

    # [START creating the tables of the DB]
    cur.execute('CREATE TABLE IF NOT EXISTS PARTIES (PARTY_ID INTEGER PRIMARY KEY, PARTY_NAME TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS GOOGLE_LABELS (LABEL_ID INTEGER PRIMARY KEY, LABEL_NAME TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS PICS (PIC_ID INTEGER PRIMARY KEY, PIC_FILE TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS PIC_PARTY (PIC_ID INTEGER, PARTY_ID INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS PIC_GOOGLE_LABELS (PIC_ID INTEGER, LABEL_ID INTEGER)')
    # [END creating the tables of the DB]
    # [END connecting to local sqlite DB]

    # [START getting labels for every JPG in directory and subdirectories, enter them to DB]
    for jpg in getjpgsindirandsubdirs(basepath):
        jpgname = jpg[jpg.rfind('\\') + 1:] #just JPG name, without it's path
        almostparty = jpg[:jpg.rfind('\\')]
        party = almostparty[almostparty.rfind('\\') +1:]
        print('getting data for pic ', jpgname)
        photolabels = getphotolabels(jpg)

        filetodb(jpgname, cur, db) #call the "enter file to DB if needed" function
        partytodb(party, cur, db) #call the "enter party to DB if needed" function
        picpartytodb(jpgname, party, cur, db) #call the "enter file-party link

        for label in photolabels:
            labeltodb(label, cur, db) #call the "enter label to DB if needed" function
            piclabellinktodb(jpgname, label, cur, db)

            db.commit() #commit changes to the DB
    cur.close()
    # [START getting labels for every JPG in directory and subdirectories, enter them to DB]

# [START actualy running the program]
main(r'C:\Users\Dror\Documents\Data Science\Israeli Elections\partyfbpics') #### change to "Base Directory" to start from
# [END actualy running the program]