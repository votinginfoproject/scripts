import sys
import datetime
import os.path
import glob
import sqlite3
import xml.etree.cElementTree as ET
from db.datastore import Datastore
from db.setupdb import setupdb
from datetime import datetime, tzinfo
from optparse import OptionParser,make_option
from ConfigParser import SafeConfigParser
from utils import get_files
from xml.sax.saxutils import escape,unescape

def main():
  """Set it off"""
  now = datetime.now()
  usage = "%prog [options] arg"
  version = "%prog .1"
  write_mode = 'a'
  
  parser = OptionParser(usage=usage, version=version, option_list=create_options_list())

  (opts,args) = parser.parse_args()
  
  if os.path.isfile(opts.config):
    # parse config file
    config = SafeConfigParser()
    config.read(opts.config)
  else:
    sys.exit("Please specify a valid config file")
  
  database = "{0}vip_data.db".format(config.get('DataSource','db_dir'))
  
  if opts.refreshdb:
    setupdb(database, config)
  
  datastore = Datastore(database)
  cursor = datastore.connect()
  
  vip_file = "{0}vipFeed-{1}.xml".format(config.get('Main','output_dir'),config.get('Main','fips'))
  
  with open(vip_file,'w') as w:
    w.write("""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<vip_object xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://election-info-standard.googlecode.com/files/vip_spec_v3.0.xsd" schemaVersion="3.0">
""")
    
    create_header(w, cursor, now)
    create_election(w, cursor)
    create_election_admins(w, cursor)
    create_election_officials(w, cursor)
    create_localities(w, cursor, config)
    create_precincts(w, cursor, config)
    create_precinct_splits(w, cursor, config)
    create_street_segments(w, cursor, config)
    create_polling_locations(w, cursor, config)
    
    w.write('</vip_object>')

def create_header(w, cursor, now):
  print "Creating source and state elements..."
  
  cursor.execute("""SELECT
    VIP_Info.source_id AS source_id,
    State.id AS state_id,
    State.name AS state_name,
    VIP_Info.id AS vip_id,
    ? AS datetime,
    description,
    organization_url,
    election_administration_id
  FROM
    VIP_Info,
    State
  WHERE
    State.id=VIP_Info.state_id
  """,(now.strftime('%Y-%m-%dT%H:%M:%S'),))

  row = cursor.fetchone()
  
  root = ET.Element("source",id=unicode(row['source_id']))
  
  vip_id = ET.SubElement(root,"vip_id")
  vip_id.text = unicode(row['vip_id'])
  
  source_name = ET.SubElement(root,"name")
  source_name.text = row['state_name']
  
  datetime = ET.SubElement(root,"datetime")
  datetime.text = unicode(row['datetime'])
  
  if row['description'] is not None and len(row['description'])>0:
    description = ET.SubElement(root,"description")
    description.text = row['description']
  
  if row['organization_url'] is not None and len(row['organization_url'])>0:
    organization_url = ET.SubElement(root,"organization_url")
    organization_url.text = unicode(row['organization_url'])
    
  w.write(ET.tostring(root))
  
  state = ET.Element("state",id=unicode(row['state_id']))
  
  name = ET.SubElement(state,"name")
  name.text = row['state_name']
  
  if row['election_administration_id'] is not None:
    election_administration_id = ET.SubElement(state,"election_administration_id")
    election_administration_id.text = unicode(row['election_administration_id'])
  
  w.write(ET.tostring(state))

def create_election(w, cursor):
  print "Creating election elements..."
  
  cursor.execute("SELECT * FROM Election LIMIT 1")
  
  for row in cursor:
    root = ET.Element("election",id=unicode(row['id']))
    
    date = ET.SubElement(root,"date")
    date.text = row['date']
    
    election_type = ET.SubElement(root,"election_type")
    election_type.text = row['election_type']
    
    state_id = ET.SubElement(root,"state_id")
    state_id.text = unicode(row['state_id'])
    
    statewide = ET.SubElement(root,"statewide")
    statewide.text = row['statewide']
    
    registration_info = ET.SubElement(root,"registration_info")
    registration_info.text = unicode(row['registration_info'])
    
    w.write(ET.tostring(root))
  
def create_election_admins(w, cursor):
  """Writes all election administration information"""
  print "Creating election administration elements..."
  
  cursor.execute("SELECT * FROM Election_Administration")

  for row in cursor:
    root = ET.Element("election_administration",id=unicode(row['id']))
    
    name = ET.SubElement(root,"name")
    name.text = row['name']
    
    if row['eo_id'] is not None:
      eo_id = ET.SubElement(root,"eo_id")
      eo_id.text = unicode(row['eo_id'])
    
    if row['mailing_address'] is not None and len(row['mailing_address'])>0:
      mailing = ET.SubElement(root,"mailing_address")
      line1 = ET.SubElement(mailing,"line1")
      line1.text = escape(row['mailing_address'])
      city = ET.SubElement(mailing,"city")
      city.text = row['city']
      state = ET.SubElement(mailing,"state")
      state.text = row['state']
      zipcode = ET.SubElement(mailing,"zip")
      
      if row['zip_plus'] is not None and len(row['zip_plus'])>0:
        zipcode.text = "{0}-{1}".format(row['zip'],row['zip_plus'])
      else:
        zipcode.text = row['zip']
      
    if row['elections_url'] is not None and len(row['elections_url'])>0:
      elections_url = ET.SubElement(root,"elections_url")
      elections_url.text = row['elections_url']
      
    if row['registration_url'] is not None and len(row['registration_url'])>0:
      registration_url = ET.SubElement(root,"registration_url")
      registration_url.text = row['registration_url']
    
    if row['am_i_registered_url'] is not None and len(row['am_i_registered_url'])>0:
      am_i_registered_url = ET.SubElement(root,"am_i_registered_url")
      am_i_registered_url.text = row['am_i_registered_url']
    
    if row['absentee_url'] is not None and len(row['absentee_url'])>0:
      absentee_url = ET.SubElement(root,"absentee_url")
      absentee_url.text = row['absentee_url']
    
    if row['where_do_i_vote_url'] is not None and len(row['where_do_i_vote_url'])>0:
      where_do_i_vote = ET.SubElement(root,"where_do_i_vote_url")
      where_do_i_vote.text = row['where_do_i_vote_url']
    
    w.write(ET.tostring(root))

def create_election_officials(w, cursor):
  """Writes all election official information"""
  print "Creating election official elements..."
  
  cursor.execute("SELECT * FROM Election_Official")

  for row in cursor:
    root = ET.Element("election_official",id=unicode(row['id']))
    
    name = ET.SubElement(root,"name")
    name.text = row['name']
    
    if row['title'] is not None:
      title = ET.SubElement(root,"title")
      title.text = unicode(row['title'])
    
    if row['phone'] is not None:
      phone = ET.SubElement(root,"phone")
      phone.text = row['phone']
      
    if row['fax'] is not None:
      fax = ET.SubElement(root,"fax")
      fax.text = row['fax']
    
    if row['email'] is not None:
      email = ET.SubElement(root,"email")
      email.text = unicode(row['email'])
      
    w.write(ET.tostring(root))
  
def create_localities(w, cursor, config):
  print "Creating locality elements..."
    
  cursor.execute("""SELECT
    ?||id AS id,
    name,
    state_id,
    type,
    election_administration_id
  FROM Locality""",
  (config.get('Locality','locality_prefix'),))
  
  for row in cursor:
    root = ET.Element("locality",id=unicode(row['id']))
    
    name = ET.SubElement(root,"name")
    name.text = row['name']
    
    state_id = ET.SubElement(root,"state_id")
    state_id.text = unicode(row['state_id'])
    
    locality_type = ET.SubElement(root,"type")
    locality_type.text = row['type']
    
    if row['election_administration_id'] is not None:
      election_administration_id = ET.SubElement(root,"election_administration_id")
      election_administration_id.text = unicode(row['election_administration_id'])
    
    w.write(ET.tostring(root))
  
def create_precincts(w, cursor, config):
  """Writes all election administration information"""
  print "Creating precinct elements..."
  
  mem = Datastore(':memory:')
  c = mem.connect()
  c.execute("""CREATE TABLE IF NOT EXISTS Precinct_Polling
(
polling_location_id INTEGER,
precinct_id INTEGER,
PRIMARY KEY(polling_location_id, precinct_id)
)""")

  cursor.execute("""SELECT
    polling_location_id AS polling_location_id,
    precinct_id AS precinct_id
  FROM Precinct_Polling"""
  )
  
  for row in cursor:
    c.execute("INSERT INTO Precinct_Polling VALUES (?,?)",(list(row)))
  
  mem.commit()
  
  cursor.execute("""SELECT
    id AS original_id,
    ?||id AS id,
    name,
    ?||locality_id AS locality_id,
    mail_only
    FROM Precinct WHERE locality_id IS NOT NULL AND name IS NOT NULL""",
    (
      config.get('Precinct', 'precinct_prefix'),
      config.get('Locality', 'locality_prefix'),
    )
  )
  
  for row in cursor:
    root = ET.Element("precinct",id=unicode(row['id']))
    name = ET.SubElement(root,"name")
    name.text = row['name']
    
    locality_id = ET.SubElement(root,"locality_id")
    locality_id.text = unicode(row['locality_id'])
    
    if row['mail_only'] is not None and len(row['mail_only'])>0:
      mail_only = ET.SubElement(root,"mail_only")
      mail_only.text = row['mail_only']
      
      if row['mail_only']=='Yes':
        w.write(ET.tostring(root))
        continue
      else:
        c.execute("""SELECT
            ?||polling_location_id AS polling_location_id
          FROM Precinct_Polling
          WHERE precinct_id=?""",
          (
            config.get('Polling_Location', 'polling_prefix'),
            row['original_id'],
          )
        )

        for p in c:
          polling_location_id = ET.SubElement(root,"polling_location_id")
          polling_location_id.text = unicode(p['polling_location_id'])
    
    w.write(ET.tostring(root))

def create_precinct_splits(w, cursor, config):
  print "Creating precinct split elements..."
  
  mem = Datastore(':memory:')
  c = mem.connect()
  c.execute("""CREATE TABLE IF NOT EXISTS Split_Polling
(
polling_location_id INTEGER,
split_id INTEGER,
PRIMARY KEY(polling_location_id, split_id)
)""")

  cursor.execute("""SELECT
    polling_location_id AS polling_location_id,
    split_id AS precinct_split_id
  FROM Split_Polling"""
  )
  
  for row in cursor:
    c.execute("INSERT INTO Split_Polling VALUES (?,?)",(list(row)))
  
  mem.commit()
  
  cursor.execute("""SELECT
    ps.id AS original_id,
    ?||ps.id AS id,
    ps.name,
    mail_only,
    ?||precinct_id AS precinct_id
    FROM
      Precinct_Split ps,
      Precinct p
    WHERE
      p.id=ps.precinct_id""",
    (
      config.get('Precinct_Split','split_prefix'),
      config.get('Precinct','precinct_prefix'),
    )
  )
  
  for row in cursor:
    root = ET.Element("precinct_split",id=unicode(row['id']))
    
    name = ET.SubElement(root,"name")
    name.text = row['name']
    
    precinct_id = ET.SubElement(root,"precinct_id")
    precinct_id.text = unicode(row['precinct_id'])
    
    if row['mail_only'] is not None and len(row['mail_only'])>0:      
      if row['mail_only']=='Yes':
        w.write(ET.tostring(root))
        continue
        
      else:
        c.execute("""SELECT
            ?||polling_location_id AS polling_location_id
          FROM Split_Polling
          WHERE split_id=?""",
          (
            config.get('Polling_Location', 'polling_prefix'),
            row['original_id'],
          )
        )

        for p in c:
          polling_location_id = ET.SubElement(root,"polling_location_id")
          polling_location_id.text = unicode(p['polling_location_id'])
    
    w.write(ET.tostring(root))
    
def create_street_segments(w, cursor, config):
  print "Creating street segment elements..."
  
  cursor.execute("""SELECT
      ?||s.id as id,
      start_house_number,
      end_house_number,
      CASE
        WHEN odd_even_both='O' THEN 'Odd'
        WHEN odd_even_both='E' THEN 'Even'
        WHEN odd_even_both='A' THEN 'Both'
        ELSE odd_even_both
      END AS odd_even_both,
      start_apartment_number,
      end_apartment_number,
      street_direction,
      street_name,
      street_suffix,
      address_direction,
      state,
      city,
      zip,
      ?||s.precinct_id AS precinct_id,
      ?||s.precinct_split_id AS precinct_split_id
    FROM
      Street_Segment AS s
    WHERE
      s.street_name IS NOT NULL""",
    (
      config.get('Street_Segment','street_prefix'),    
      config.get('Precinct','precinct_prefix'),
      config.get('Precinct_Split','split_prefix'),
    )
  )
  
  for row in cursor:
    root = ET.Element("street_segment",id=unicode(row['id']))
    
    if len(unicode(row['start_house_number']))>0 and len(unicode(row['end_house_number']))>0:
      
      if row['start_house_number']>row['end_house_number']:
        start = unicode(row['end_house_number'])
        end = unicode(row['start_house_number'])
      else:
        start = unicode(row['start_house_number'])
        end = unicode(row['end_house_number'])
      
      start_house_number = ET.SubElement(root,"start_house_number")
      start_house_number.text = start
      
      end_house_number = ET.SubElement(root,"end_house_number")  
      end_house_number.text = end
    
    if len(row['odd_even_both'])>0:
      odd_even_both = ET.SubElement(root,"odd_even_both")
      odd_even_both.text = unicode(row['odd_even_both'])
    
    if row['start_apartment_number'] is not None and row['end_apartment_number'] is not None and len(unicode(row['start_apartment_number']))>0 and len(unicode(row['end_apartment_number']))>0:
      
      start_apartment_number = ET.SubElement(root,"start_apartment_number")
      start_apartment_number.text = unicode(row['start_apartment_number'])
      
      end_apartment_number = ET.SubElement(root,"end_apartment_number")
      end_apartment_number.text = unicode(row['end_apartment_number'])
    
    non_house_address = ET.SubElement(root,"non_house_address")
    
    if row['street_direction'] is not None and len(row['street_direction'])>0:
      street_direction = ET.SubElement(non_house_address,'street_direction')
      street_direction.text = row['street_direction']
    
    street_name = ET.SubElement(non_house_address,"street_name")
    street_name.text = row['street_name']
    
    if row['street_suffix'] is not None and len(row['street_suffix'])>0:
      street_suffix = ET.SubElement(non_house_address,"street_suffix")
      street_suffix.text = row['street_suffix']
    
    if row['address_direction'] is not None and len(row['address_direction'])>0:
      address_direction = ET.SubElement(non_house_address,"address_direction")
      address_direction.text = row['address_direction']
    
    city = ET.SubElement(non_house_address,"city")
    city.text = row['city']
    
    state = ET.SubElement(non_house_address,"state")
    state.text = row['state']
    
    zip = ET.SubElement(non_house_address,"zip")
    zip.text = row['zip']

    if row['precinct_id'] is not None:
      precinct_id = ET.SubElement(root,"precinct_id")
      precinct_id.text = unicode(row['precinct_id'])
      
    if row['precinct_split_id'] is not None:
      precinct_split_id = ET.SubElement(root,"precinct_split_id")
      precinct_split_id.text = unicode(row['precinct_split_id'])

    w.write(ET.tostring(root))
  
def create_polling_locations(w, cursor, config):
  print "Creating polling place elements..."

  cursor.execute("""SELECT
    ?||id AS id,
    location_name,
    TRIM(line1) AS line1,
    city,
    ? AS state,
    zip
  FROM Polling_Location
  WHERE
    line1 IS NOT NULL AND
    line1!=?""",
    (
      config.get('Polling_Location','polling_prefix'),
      config.get('Main', 'state_abbreviation'),
      '',
    )
  )  
#   cursor.execute("""SELECT
#     ?||id AS id,
#     location_name,
#     TRIM(line1) AS line1,
#     city,
#     CASE
#       WHEN state=? THEN ?
#       WHEN state IS NULL THEN ?
#       WHEN state='' THEN ?
#     END AS state,
#     zip
#   FROM Polling_Location
#   WHERE
#     line1 IS NOT NULL AND
#     line1!=?""",
#     (
#       config.get('Polling_Location','polling_prefix'),
#       config.get('Main', 'state_name'),
#       config.get('Main', 'state_abbreviation'),
#       config.get('Main', 'state_abbreviation'),
#       config.get('Main', 'state_abbreviation'),
#       '',
#     )
#   )
  
  for row in cursor:
    root = ET.Element("polling_location",id=unicode(row['id']))
    
    address = ET.SubElement(root,"address")
    
    if row['location_name'] is not None:
      location_name = ET.SubElement(address,"location_name")
      location_name.text = escape(row['location_name'])
    
    line1 = ET.SubElement(address,"line1")
    line1.text = escape(row['line1'])
    city = ET.SubElement(address,"city")
    city.text = row['city']
    state = ET.SubElement(address,"state")
    state.text = row['state']
    zipcode = ET.SubElement(address,"zip")
    zipcode.text = row['zip']
    
    w.write(ET.tostring(root))
  
def config_section_as_dict(config_items):
  """Changes the section list of tuples to a dictionary"""
  data_dict = {}
  
  for key,val in config_items:
    data_dict[key] = val
  
  return data_dict

def create_options_list():
  """Creates the optionsparser options list"""
  option_list = [
    make_option("-v", "--verbose", dest="verbose",
      default=False, action="store_true",
      help="Write status messages to stdout"),

    make_option("--config", type="string",
      dest="config", default="config.ini",
      help="Specifies a config file",
      metavar="CONFIG_FILE"),
    
    make_option("--refreshdb", dest="refreshdb",
      default=False, action="store_true",
      help="Reloads database"),
  ]
  
  return option_list

if __name__ == "__main__":
  main()