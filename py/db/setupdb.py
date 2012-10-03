import sys, re
import datetime
import os.path
import glob
import sqlite3
from datastore import Datastore
import xml.etree.cElementTree as ET
from csv import DictReader, QUOTE_MINIMAL, Error as CSVError
from datetime import datetime, tzinfo
from xml.sax.saxutils import escape,unescape

def setupdb(filepath, config):
  print "Creating database..."
  datastore = Datastore(filepath)
  cursor = datastore.connect()
  
  cleanup(cursor)
  datastore.commit()
  
  create_tables(cursor)
  datastore.commit()
    
  ansi = Datastore('/tmp/ansi.db')
  c = ansi.connect()
  cleanup(c)
  load_ansi(c, 'national.txt')
  ansi.commit()
  ansi.close()
  
  load_data(cursor, config)
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
  cursor.execute("DROP TABLE IF EXISTS Ansi")
  cursor.execute("DROP TABLE IF EXISTS Ansi_State")
  
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

  cursor.execute("""CREATE TABLE IF NOT EXISTS Split_Polling
(
polling_location_id INTEGER REFERENCES Polling_Location (id),
split_id INTEGER REFERENCES Precinct_Split (id),
PRIMARY KEY(polling_location_id,split_id)
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
id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
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
original_id INTEGER,
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

def load_ansi(cursor,filename):
  """Loads data from file into the database"""
  filename = os.path.abspath(filename)
  if os.path.exists(filename):
    with open(filename,'r') as r:
      reader = DictReader(r, delimiter=',')
      
      try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS Ansi
          (
          state_id INTEGER REFERENCES State (id),
          county_id INTEGER,
          county_name TEXT,
          PRIMARY KEY(state_id, county_id)
          )""")
          
        cursor.execute("""CREATE TABLE IF NOT EXISTS Ansi_State
          (
          id INTEGER PRIMARY KEY,
          abbreviation TEXT
          )""")
        
        for line in reader:
          cursor.execute(
            "INSERT INTO Ansi(state_id,county_id,county_name) VALUES (?,?,?)",
            (
              line['State ANSI'],
              line['County ANSI'],
              line['County Name'],
            )
          )
          
          cursor.execute(
            "INSERT OR IGNORE INTO Ansi_State(id,abbreviation) VALUES (?,?)",
            (
              line['State ANSI'],
              line['State'],
            )
          )
          
      except CSVError, e:
        sys.exit("file {0}, line {1}: {2}".format(filename, reader.line_num, e))
  else:
    sys.exit("No data found")

def get_line(r, reader, parser_type):
  """Generator that determines the parser type and yields the next line"""
  if parser_type=='csv':
    for line in reader:
      yield line 
  elif parser_type=='regex':
    for line in r:
      groups = re.match(reader,line).groupdict('')
      yield groups
      
def load_data(cursor, config):
  file_tmpl = '{0}{1}.txt'
  
  possible_data = (
    'source',
    'election_administration',
    'state',
    'election',
#    'election_official',
    'locality',
    'polling_location',
    'precinct',
    'precinct_split',
    'street_segment',
  )
  
  for i in possible_data:
    filename = file_tmpl.format(config.get('Main','data_dir'),i)
    print "Currently looking at {0}".format(i)
    if os.path.exists(filename):
      with open(filename,'r') as r:
        print "Parsing and loading data from {0}".format(i)
        sect = i.title()
        parser_type = config.get(sect,'parser_type')
        if config.has_section(sect):
          if parser_type=='csv':
            klass = dyn_class(
              config.get(sect,'parser_module'),
              config.get(sect,'parser_class')
            )
            
            reader = klass(
              r,
              delimiter=chr(config.getint(sect,'delimiter')),
              quotechar=config.get(sect, 'quotechar'),
              quoting=QUOTE_MINIMAL
            )
          elif parser_type=='regex':
            reader = re.compile(config.get(sect,'regex'))
            #reader = DictReader(r, delimiter=chr(config.getint(i.title(),'delimiter')), quotechar='"', quoting=QUOTE_MINIMAL)
        else:
          reader = DictReader(
            r,
            delimiter=chr(config.getint('Parser','delimiter')),
            quotechar='"',
            quoting=QUOTE_MINIMAL
          )
        
        try:
          for line in get_line(r, reader, parser_type):
            if i=='source':
              cursor.execute(
                "INSERT INTO VIP_Info(id,source_id,description,state_id) VALUES (?,?,?,?)",
                (
                  line['VIP_ID'],
                  line['ID'],
                  line['DESCRIPTION'],
                  line.get('STATE_ID', config.get('Main','fips')),
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
              print line
              cursor.execute(
                "INSERT INTO State(id,name,election_administration_id,organization_url) VALUES (?,?,?,?)",
                (
                  line.get('ID', config.get('Main','fips')),
                  line.get('NAME'),
                  line.get('ELECTION_ADMINISTRATION_ID'),
                  line.get('ORGANIZATION_URL',''),
                )
              )
            
            elif i=='election':
              cursor.execute(
                "INSERT OR IGNORE INTO Election(id,date,election_type,state_id,statewide,registration_info) VALUES (?,?,?,?,?,?)",
                (
                  line['ID'],
                  datetime.strptime(line.get('DATE'), config.get('Main','time_format')).strftime('%Y-%m-%d'),
                  line.get('ELECTION_TYPE', "General"),
                  line.get('STATE_ID', config.get('Main','fips')),
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
                  line.get('STATE_ID', config.get('Main','fips')),
                  line.get('TYPE'),
                  line.get('ELECTION_ADMINISTRATION_ID'),
                )
              )
            
            elif i=='polling_location':
              cursor.execute(
                "INSERT OR IGNORE INTO Polling_Location(id,location_name,line1,city,state,zip) VALUES (?,?,?,?,?,?)",
                (
                  line['ID'],
                  line.get('LOCATION_NAME',''),
                  line.get('LINE1',''),
                  line.get('CITY',''),
                  line.get('STATE',''),
                  line.get('ZIP',''),
                )
              )
              
            elif i=='precinct':
              cursor.execute(
                "INSERT OR IGNORE INTO Precinct(id,name,locality_id,mail_only) VALUES (?,?,?,?)",
                (
                  line['ID'],
                  line.get('NAME'),
                  line.get('LOCALITY_ID', config.get('Main','locality_id')),
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
                if len(line.get('NAME',''))==0: line['NAME']=line['ID']
                  
                line['ID'] = sanitize(line,'ID')
                
                cursor.execute(
                  "INSERT OR IGNORE INTO Precinct_Split(id,name,precinct_id) VALUES (?,?,?)",
                  (
                    line['ID'],
                    line.get('NAME'),
                    line['PRECINCT_ID'],
                  )
                )
              
              if len(line.get('ELECTORAL_DISTRICT_ID',""))>0:
                cursor.execute(
                  "INSERT OR IGNORE INTO Precinct_Split_District(precinct_split_id,electoral_district_id) VALUES (?,?)",
                  (
                    line['ID'],
                    line['ELECTORAL_DISTRICT_ID'],
                  )
                )
                
              if len(line.get('POLLING_LOCATION_ID',""))>0:
                cursor.execute(
                  "INSERT OR IGNORE INTO Split_Polling(split_id,polling_location_id) VALUES (?,?)",
                  (
                    line['ID'],
                    line['POLLING_LOCATION_ID'],
                  )
                )
            
            elif i=='street_segment':
              if len(line.get('PRECINCT_SPLIT_ID',""))>0:
                line['PRECINCT_SPLIT_ID'] = sanitize(line,'PRECINCT_SPLIT_ID')
              
              if line.get('STREET_DIRECTION','')=='NULL':
                line['STREET_DIRECTION'] = None
                
              cursor.execute(
                """INSERT INTO
                  Street_Segment(
                  id,
                  start_house_number,
                  end_house_number,
                  odd_even_both,
                  start_apartment_number,
                  end_apartment_number,
                  street_direction,
                  street_name,
                  street_suffix,
                  address_direction,
                  state,
                  city,
                  zip,
                  precinct_id,
                  precinct_split_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                  line.get('ID'),
                  line.get('START_HOUSE_NUMBER', None),
                  line.get('END_HOUSE_NUMBER', None),
                  line.get('ODD_EVEN_BOTH', None),
                  line.get('START_APARTMENT_NUMBER', None),
                  line.get('END_APARTMENT_NUMBER', None),
                  line.get('STREET_DIRECTION', None),
                  line.get('STREET_NAME', None),
                  line.get('STREET_SUFFIX', None),
                  line.get('ADDRESS_DIRECTION', None),
                  line.get('STATE', config.get('Main', 'state_abbreviation')),
                  line.get('CITY', None),
                  line.get('ZIP', None),
                  line.get('PRECINCT_ID', None),
                  line.get('PRECINCT_SPLIT_ID', None),
                )
              )
              
        except CSVError, e:
          sys.exit("file {0}, line {1}: {2}".format(filename, reader.line_num, e))
        
        except sqlite3.IntegrityError, e:
          sys.exit("file {0}, line {1}: {2}".format(filename, reader.line_num, e))

def update_data(cursor):
  print "Updating Precinct Split data..."
  #cursor.execute("DELETE FROM Precinct_Split WHERE precinct_id NOT IN (SELECT id FROM Precinct)")
  
  print "Updating street segments..."
  cursor.execute("UPDATE Street_Segment SET start_house_number=1, end_house_number=9999999 WHERE start_house_number=0 AND end_house_number=0")
  cursor.execute("DELETE FROM Street_Segment WHERE precinct_id NOT IN (SELECT id FROM Precinct)")
  
  print "Updating Locality election official data..."
  cursor.execute("UPDATE Locality SET election_administration_id=NULL WHERE election_administration_id NOT IN (SELECT id FROM Election_Administration)")
  
  print "Updating Polling Location data..."
  #cursor.execute("DELETE FROM Polling_Location WHERE line1 IS NULL OR line1=''")
  
  print "Updating Precinct Polling data..."
  #cursor.execute("DELETE FROM Precinct_Polling WHERE polling_location_id NOT IN (SELECT id FROM Polling_Location)")
  
  print "Updating Precinct Split data..."
  cursor.execute("DELETE FROM Precinct_Split WHERE precinct_id NOT IN (SELECT id FROM Precinct)")

def get_locality(county_id, state_fips):
  ds = Datastore('/tmp/ansi.db')
  c = ds.connect()
  
  c.execute(
    """SELECT state_id, county_id, county_name
      FROM
        Ansi
      WHERE
        state_id=? AND
        county_id=?""",
    (
      state_fips,
      county_id,
    )
  )
  
  row = dict(c.fetchone())
  id = "".join([str(row['state_id']),str(row['county_id']).rjust(3,'0')])
  name = row['county_name']
  ds.close()
  
  return (id,name,)
  
  
def sanitize(line, key):
  """Mostly for sanitizing ids at this point
  May be more useful later."""
  return ''.join(line[key].split(' '))

def dyn_class(modname,classname):
  """Returns a class of classname from module modname."""
  module = __import__(modname)
  klass = getattr(module,classname)
  return klass
