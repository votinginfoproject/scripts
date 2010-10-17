import sys
import datetime
import os.path
import glob
import sqlite3
from datastore import Datastore
import xml.etree.cElementTree as ET
from csv import DictReader, QUOTE_ALL, Error as CSVError
from datetime import datetime, tzinfo
from utils import get_files
from xml.sax.saxutils import escape,unescape

def setupdb(filepath):
  print "Creating database..."
  datastore = Datastore(filepath)
  cursor = datastore.connect()
  
  cleanup(cursor)
  datastore.commit()
  
  create_tables(cursor)
  datastore.commit()
  
  load_data(cursor)
  datastore.commit()
  
  update_data(cursor)
  datastore.commit()
  
  datastore.close()
  
def cleanup(cursor):
  cursor.execute("DROP TABLE IF EXISTS Early_Vote_Site")
  cursor.execute("DROP TABLE IF EXISTS Polling_Location")
  cursor.execute("DROP TABLE IF EXISTS Precinct")
  cursor.execute("DROP TABLE IF EXISTS Precinct_Polling")
  cursor.execute("DROP TABLE IF EXISTS Precinct_Early_Vote")
  cursor.execute("DROP TABLE IF EXISTS Precinct_Split")
  cursor.execute("DROP TABLE IF EXISTS Electoral_District")
  cursor.execute("DROP TABLE IF EXISTS Precinct_Split_District")
  cursor.execute("DROP TABLE IF EXISTS Street_Segment")
  cursor.execute("DROP TABLE IF EXISTS Election_Administration")
  cursor.execute("DROP TABLE IF EXISTS Election_Official")
  cursor.execute("DROP TABLE IF EXISTS Locality")
  cursor.execute("DROP TABLE IF EXISTS State")
  cursor.execute("DROP TABLE IF EXISTS Election")
  cursor.execute("DROP TABLE IF EXISTS VIP_Info")
  
def create_tables(cursor):
  cursor.execute("""CREATE TABLE IF NOT EXISTS Early_Vote_Site
(
id INTEGER PRIMARY KEY,
name TEXT NOT NULL,
address TEXT NOT NULL,
city TEXT,
state TEXT,
zip TEXT,
voter_services TEXT
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Polling_Location
(
id INTEGER PRIMARY KEY,
location_name TEXT,
line1 TEXT,
city TEXT,
state TEXT,
zip TEXT
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Precinct_Polling
(
polling_location_id INTEGER REFERENCES Polling_Location (id),
precinct_id INTEGER REFERENCES Precinct (id),
PRIMARY KEY(polling_location_id,precinct_id)
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Precinct
(
id INTEGER PRIMARY KEY,
name TEXT NOT NULL,
locality_id INTEGER NOT NULL,
mail_only TEXT DEFAULT 'No' NOT NULL
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Precinct_Split
(
id INTEGER PRIMARY KEY,
name TEXT NOT NULL,
precinct_id INTEGER NOT NULL REFERENCES Precinct (id)
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Electoral_District
(
id INTEGER PRIMARY KEY,
name TEXT,
type TEXT,
number INTEGER
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Precinct_Split_District
(
precinct_split_id INTEGER NOT NULL REFERENCES Precinct_Split (id),
electoral_district_id INTEGER NOT NULL REFERENCES Electoral_District (id),
PRIMARY KEY(precinct_split_id, electoral_district_id)
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Street_Segment
(
id CHAR PRIMARY KEY,
start_house_number INTEGER NOT NULL,
end_house_number INTEGER NOT NULL,
odd_even_both TEXT,
start_apartment_number INTEGER,
end_apartment_number INTEGER,
street_direction TEXT,
street_name TEXT NOT NULL,
street_suffix TEXT NOT NULL,
address_direction TEXT,
state TEXT NOT NULL,
city TEXT NOT NULL,
zip TEXT NOT NULL,
precinct_id INTEGER REFERENCES Precinct (id),
precinct_split_id INTEGER REFERENCES Precint_Split (id)
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Election_Administration
(
id INTEGER PRIMARY KEY,
name TEXT,
eo_id INTEGER,
mailing_address TEXT,
city TEXT,
state TEXT,
zip TEXT,
zip_plus TEXT,
elections_url TEXT,
registration_url TEXT,
am_i_registered_url TEXT,
absentee_url TEXT,
where_do_i_vote_url TEXT
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Election_Official
(
id INTEGER PRIMARY KEY,
name TEXT,
title TEXT,
phone TEXT,
fax TEXT,
email TEXT
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Locality
(
id INTEGER PRIMARY KEY,
name TEXT,
state_id TEXT NOT NULL,
type TEXT,
election_administration_id INTEGER REFERENCES Election_Administration (id)
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS State
(
id INTEGER PRIMARY KEY,
name TEXT,
election_administration_id INTEGER REFERENCES Election_Administration (id),
organization_url TEXT
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Precinct_Early_Vote
(
precinct_id INTEGER REFERENCES Precinct (id),
early_vote_site_id INTEGER REFERENCES Early_Vote_Site (id),
PRIMARY KEY(precinct_id, early_vote_site_id)
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS Election
(
id INTEGER PRIMARY KEY,
date TEXT,
election_type TEXT,
state_id INTEGER REFERENCES State (id),
statewide TEXT,
registration_info TEXT
)""")

  cursor.execute("""CREATE TABLE IF NOT EXISTS VIP_Info
(
id INTEGER PRIMARY KEY,
source_id INTEGER,
description TEXT,
state_id INTEGER REFERENCES State (id)
);""")

def load_data(cursor):
  file_tmpl = '/Users/jared/Documents/projects/vip/data/co/{0}.txt'
  
  possible_data = (
    'source',
    'election_administration',
    'state',
    'election',
    'election_official',
    'locality',
    'polling_location',
    'precinct',
    'precinct_split',
    'street_segment',
  )
  
  for i in possible_data:
    if os.path.exists(file_tmpl.format(i)):
      with open(file_tmpl.format(i),'r') as r:
        print "Parsing and loading data from {0}".format(i)
        reader = DictReader(r, delimiter='|')
        
        try:
          for line in reader:
            if i=='source':
              cursor.execute(
                "INSERT INTO VIP_Info(id,source_id,description,state_id) VALUES (?,?,?,?)",
                (
                  line['VIP_ID'],
                  line['ID'],
                  line['DESCRIPTION'],
                  8,
                )
              )
              
            elif i=='election_administration':
              cursor.execute(
                "INSERT OR IGNORE INTO Election_Administration(id,name,eo_id,mailing_address,city,state,zip,zip_plus,elections_url,registration_url,am_i_registered_url,absentee_url,where_do_i_vote_url) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                  line['ID'],
                  line.get('NAME', None),
                  line.get('EO_ID', None),
                  line.get('MAILING_ADDRESS', None),
                  line.get('CITY', None),
                  line.get('STATE', None),
                  line.get('ZIP', None),
                  line.get('ZIP_PLUS', None),
                  line.get('ELECTIONS_URL', None),
                  line.get('REGISTRATION_URL', None),
                  line.get('AM_I_REGISTERED_URL', None),
                  line.get('ABSENTEE_URL', None),
                  line.get('WHERE_DO_I_VOTE_URL', None),
                )
              )
            
            elif i=='state':
              cursor.execute(
                "INSERT INTO State(id,name,election_administration_id,organization_url) VALUES (?,?,?,?)",
                (
                  line['ID'],
                  line.get('NAME'),
                  line.get('ELECTION_ADMINISTRATION_ID'),
                  'http://www.sos.ms.gov/elections.aspx',
                )
              )
            
            elif i=='election':
              cursor.execute(
                "INSERT OR IGNORE INTO Election(id,date,election_type,state_id,statewide,registration_info) VALUES (?,?,?,?,?,?)",
                (
                  line['ID'],
                  datetime.strptime(line.get('DATE'), '%Y-%m-%d').strftime('%Y-%m-%d'),
                  line.get('ELECTION_TYPE', "General"),
                  line.get('STATE_ID', 8),
                  line.get('STATEWIDE', "Yes"),
                  line.get('REGISTRATION_INFO', None),
                )
              )
            
            elif i=='election_official':
              cursor.execute(
                "INSERT INTO Election_Official(id,name,title,phone,fax,email) VALUES (?,?,?,?,?,?)",
                (
                  line['ID'],
                  line.get('NAME'),
                  line.get('TITLE', None),
                  line.get('PHONE', None),
                  line.get('FAX', None),
                  line.get('EMAIL', None)
                )
              )
              
            elif i=='locality':
              cursor.execute(
                "INSERT OR IGNORE INTO Locality(id,name,state_id,type,election_administration_id) VALUES (?,?,?,?,?)",
                (
                  line['ID'],
                  line.get('NAME'),
                  line.get('STATE_ID', 8),
                  line.get('TYPE'),
                  line.get('ELECTION_ADMINISTRATION_ID'),
                )
              )
            
            elif i=='polling_location':
              cursor.execute(
                "INSERT OR IGNORE INTO Polling_Location(id,location_name,line1,city,state,zip) VALUES (?,?,?,?,?,?)",
                (
                  line['ID'],
                  line.get('LOCATION_NAME'),
                  line.get('LINE1'),
                  line.get('CITY'),
                  line.get('STATE'),
                  line.get('ZIP'),
                )
              )
              
            elif i=='precinct':
              cursor.execute(
                "INSERT OR IGNORE INTO Precinct(id,name,locality_id,mail_only) VALUES (?,?,?,?)",
                (
                  line['ID'],
                  line.get('NAME'),
                  line.get('LOCALITY_ID'),
                  line.get('MAIL_ONLY',"No"),
                )
              )
                
              if len(line.get('POLLING_LOCATION_ID',""))>0:
                cursor.execute(
                  "INSERT OR IGNORE INTO Precinct_Polling(precinct_id,polling_location_id) VALUES (?,?)",
                  (
                    line['ID'],
                    line['POLLING_LOCATION_ID'],
                  )
                )
              
              if len(line.get('EARLY_VOTE_SITE_ID',""))>0:
                cursor.execute(
                  "INSERT OR IGNORE INTO Precinct_Early_Vote(precinct_id,early_vote_site_id) VALUES (?,?)",
                  (
                    line['ID'],
                    line['EARLY_VOTE_SITE_ID'],
                  )
                )
            
            elif i=='precinct_split':
              if len(line.get('PRECINCT_ID',""))>0:
                cursor.execute(
                  "INSERT OR IGNORE INTO Precinct_Split(id,name,precinct_id) VALUES (?,?,?)",
                  (
                    line['ID'],
                    line.get('NAME'),
                    line.get('PRECINCT_ID'),
                  )
                )
                
                continue
              
              if len(line.get('ELECTORAL_DISTRICT_ID',""))>0:
                cursor.execute(
                  "INSERT OR IGNORE INTO Precinct_Split_District(precinct_split_id,electoral_district_id) VALUES (?,?)",
                  (
                    line['ID'],
                    line['ELECTORAL_DISTRICT_ID'],
                  )
                )
            
            elif i=='street_segment':
              cursor.execute(
                "INSERT INTO Street_Segment(id,start_house_number,end_house_number,odd_even_both,start_apartment_number,end_apartment_number,street_direction,street_name,street_suffix,address_direction,state,city,zip,precinct_id,precinct_split_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                  line.get('STREET_SEGMENT_ID'),
                  line.get('START_HOUSE_NUMBER', None),
                  line.get('END_HOUSE_NUMBER', None),
                  line.get('ODD_EVEN_BOTH', None),
                  line.get('START_APARTMENT_NUMBER', None),
                  line.get('END_APARTMENT_NUMBER', None),
                  line.get('STREET_DIRECTION', None),
                  line.get('STREET_NAME', None),
                  line.get('STREET_SUFFIX', None),
                  line.get('ADDRESS_DIRECTION', None),
                  line.get('STATE', None),
                  line.get('CITY', None),
                  line.get('ZIP', None),
                  line.get('PRECINCT_ID', None),
                  line.get('PRECINCT_SPLIT_ID', None),
                )
              )
              
        except CSVError, e:
          sys.exit("file {0}, line {1}: {2}".format(file_tmpl.format(i), reader.line_num, e))
        
        except sqlite3.IntegrityError, e:
          sys.exit("file {0}, line {1}: {2}".format(file_tmpl.format(i), reader.line_num, e))

def update_data(cursor):
  print "Updating street segments..."
  cursor.execute("UPDATE Street_Segment SET start_house_number=1, end_house_number=9999999 WHERE start_house_number=0 AND end_house_number=0")
  
  print "Updating locality election official data..."
  cursor.execute("UPDATE Locality SET election_administration_id=NULL WHERE election_administration_id NOT IN (SELECT id FROM Election_Administration)")
  
  print "Updating Precinct Polling data..."
  cursor.execute("DELETE FROM Precinct_Polling WHERE polling_location_id NOT IN (SELECT id FROM Polling_Location)")