import sys
import datetime
import os.path
import glob
import sqlite3
import xml.etree.cElementTree as ET
from db.datastore import Datastore
from db.setupdb import setupdb
from csv import DictReader,Error as CSVError
from datetime import datetime, tzinfo
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
  
    setupdb('/tmp/vip_data_co.db')
  
  vip_file = "{0}vipFeed-{1}.xml".format(config.get('Main','output_dir'),config.get('Header','state_fips'))
  
  with open(vip_file,'w') as w:
    w.write('<?xml version="1.0" standalone="yes"?>\n<vip_object xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://election-info-standard.googlecode.com/files/vip_spec_v2.2.xsd" schemaVersion="2.2">\n')
    
    create_header(w,now)
    create_election(w)
    create_election_admins(w)
    create_election_officials(w)
    create_localities(w)
    create_precincts(w)
    create_precinct_splits(w)
    create_street_segments(w)
    create_polling_locations(w)
    
    w.write('</vip_object>')

def create_header(w,now):
  print "Creating source and state elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  cursor.execute("SELECT VIP_Info.source_id AS source_id, State.name AS state_name, VIP_Info.id AS vip_id, ? AS datetime, description, organization_url, election_administration_id FROM VIP_Info, State WHERE State.id=VIP_Info.state_id",(now.strftime('%Y-%m-%dT%H:%M:%S'),))

  row = cursor.fetchone()

  w.write("""  <source id="{source_id}">
    <name>State of {state_name}</name>
    <vip_id>{vip_id}</vip_id>
    <datetime>{datetime}</datetime>
    <description>{description}</description>
    <organization_url>{organization_url}</organization_url>
  </source>
  <state id="{vip_id}">
    <name>{state_name}</name>
    <election_administration_id>{election_administration_id}</election_administration_id>
  </state>
""".format(**row))

def create_election(w):
  print "Creating election elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
  cursor.execute("SELECT * FROM Election")
  
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
    
  datastore.close()
  
def create_election_admins(w):
  """Writes all election administration information"""
  print "Creating election administration elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
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
  
  datastore.close()

def create_election_officials(w):
  """Writes all election official information"""
  print "Creating election official elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
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
  
  datastore.close()
  
def create_localities(w):
  print "Creating locality elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
  cursor.execute("SELECT * FROM Locality")
  
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
    
  datastore.close()
  
def create_precincts(w):
  """Writes all election administration information"""
  print "Creating precinct elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
  mem = Datastore(':memory:')
  c = mem.connect()
  c.execute("""CREATE TABLE IF NOT EXISTS Precinct_Polling
(
polling_location_id INTEGER,
precinct_id INTEGER,
PRIMARY KEY(polling_location_id, precinct_id)
)""")

  cursor.execute("SELECT * FROM Precinct_Polling")
  
  for row in cursor:
    c.execute("INSERT INTO Precinct_Polling VALUES (?,?)",(list(row)))
  
  mem.commit()
  
  cursor.execute("SELECT * FROM Precinct WHERE locality_id IS NOT NULL AND name IS NOT NULL")
  
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
        c.execute("SELECT polling_location_id FROM Precinct_Polling WHERE precinct_id=?",(row['id'],))

        for p in c:
          polling_location_id = ET.SubElement(root,"polling_location_id")
          polling_location_id.text = unicode(p['polling_location_id'])
    
    w.write(ET.tostring(root))
  
  datastore.close()

def create_precinct_splits(w):
  print "Creating precinct split elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
  cursor.execute("SELECT * FROM Precinct_Split")
  
  for row in cursor:
    root = ET.Element("precinct_split",id=unicode(row['id']))
    
    name = ET.SubElement(root,"name")
    name.text = row['name']
    
    precinct_id = ET.SubElement(root,"precinct_id")
    precinct_id.text = unicode(row['precinct_id'])
    
    w.write(ET.tostring(root))
    
  datastore.close()

def create_street_segments(w):
  print "Creating street segment elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
  cursor.execute("SELECT * FROM Street_Segment, Precinct WHERE Street_Segment.street_name IS NOT NULL AND Street_Segment.precinct_id=Precinct.id")
  
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
    
    if len(row['street_direction'])>0:
      street_direction = ET.SubElement(non_house_address,'street_direction')
      street_direction.text = row['street_direction']
    
    street_name = ET.SubElement(non_house_address,"street_name")
    street_name.text = row['street_name']
    
    if len(row['street_suffix'])>0:
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
  
  datastore.close()
  
def create_polling_locations(w):
  print "Creating polling place elements..."
  datastore = Datastore('/tmp/vip_data_co.db')
  cursor = datastore.connect()
  
  cursor.execute("SELECT * FROM Polling_Location WHERE line1 IS NOT NULL")
  
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
  
  datastore.close()

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
  ]
  
  return option_list

if __name__ == "__main__":
  main()