using System;
using System.Configuration;
using System.Collections.Specialized;
using System.Linq;
using System.ComponentModel;
using System.Data;
using System.Text;
using System.Xml;
using Oracle.DataAccess.Client;

namespace Builder {
    class FeedWriter {
        private string _sOutputFileNameFormat = "{0}vipFeed-{1}-{2}.xml";
        private string _sFileTimeFormat = "yyyy-MM-ddTHH-mm-ss";
        //private DateTime _oScriptStart;
        private XmlWriter _oXmlWriter;

        private XmlWriter CreateXmlWriter(NameValueCollection oXmlConfig, String sOutputFile) {
            XmlWriterSettings xmlSettings = new XmlWriterSettings();
            xmlSettings.Indent = Boolean.Parse(oXmlConfig.Get("Indent"));
            xmlSettings.IndentChars = oXmlConfig.Get("IndentChars");
            xmlSettings.ConformanceLevel = oXmlConfig.Get("ConformanceLevel") == "Fragment" ? ConformanceLevel.Fragment : ConformanceLevel.Document;

            return XmlWriter.Create(sOutputFile, xmlSettings);
        }

        private string FormatFileName(string sFilePath, string sFIPS, DateTime oScriptStart) {
            return String.Format(_sOutputFileNameFormat,sFilePath,sFIPS,oScriptStart.ToString(_sFileTimeFormat));
        }

        public void WriteStartElement(string sElementName) {
            _oXmlWriter.WriteStartElement(sElementName);
        }

        public void WriteEndElement() {
            _oXmlWriter.WriteEndElement();
        }

        public void WriteHeader(NameValueCollection oVipConfig) {
            _oXmlWriter.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");
            _oXmlWriter.WriteAttributeString("xsi", "noNamespaceSchemaLocation", null, oVipConfig.Get("SchemaURL"));
            _oXmlWriter.WriteAttributeString("schemaVersion", oVipConfig.Get("SchemaVer"));

            _oXmlWriter.WriteStartElement("state");
            _oXmlWriter.WriteAttributeString("id", oVipConfig.Get("StateFIPS"));
            _oXmlWriter.WriteElementString("name", oVipConfig.Get("StateName"));
            _oXmlWriter.WriteEndElement();

            _oXmlWriter.WriteStartElement("source");
            _oXmlWriter.WriteAttributeString("id", "1");
            _oXmlWriter.WriteElementString("vip_id", oVipConfig.Get("StateFIPS"));
            _oXmlWriter.WriteElementString("datetime", oVipConfig.Get("ScriptStart"));
            _oXmlWriter.WriteElementString("description", oVipConfig.Get("Description"));
            _oXmlWriter.WriteElementString("organization_url", oVipConfig.Get("OrganizationURL"));
            _oXmlWriter.WriteEndElement();
        }

        public void WriteElementFromConfig(String sElementName, NameValueCollection oConfig) {
            _oXmlWriter.WriteStartElement(sElementName);

            try {
                _oXmlWriter.WriteAttributeString("id", oConfig.Get("id"));
                // clear the id so it's not used in the loop
                oConfig.Remove("id");
            } catch (ArgumentOutOfRangeException) {
                // skip it
            }

            foreach (String s in oConfig.AllKeys) {
                _oXmlWriter.WriteElementString(s, oConfig[s]);
            }

            _oXmlWriter.WriteEndElement();
        }

        public void WritePollingPlaces(OracleCommand oCmd) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while (oDataReader.Read()) {
                _oXmlWriter.WriteStartElement("polling_location");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                _oXmlWriter.WriteStartElement("address");

                if (oDataReader["location_name"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("location_name", oDataReader["location_name"].ToString());
                }

                _oXmlWriter.WriteElementString("line1", oDataReader["line1"].ToString());
                _oXmlWriter.WriteElementString("city", oDataReader["city"].ToString());
                _oXmlWriter.WriteElementString("state", oDataReader["state"].ToString());
                _oXmlWriter.WriteElementString("zip", oDataReader["zip"].ToString());
                _oXmlWriter.WriteEndElement();

                if (oDataReader["directions"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("directions", oDataReader["directions"].ToString());
                }

                if (oDataReader["polling_hours"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("polling_hours", oDataReader["polling_hours"].ToString());
                }

                _oXmlWriter.WriteEndElement();
            }
        }

        public void WriteStreetSegments(OracleCommand oCmd) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while (oDataReader.Read()) {
                _oXmlWriter.WriteStartElement("street_segment");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());

                _oXmlWriter.WriteElementString("start_house_number", oDataReader["start_house_number"].ToString());
                _oXmlWriter.WriteElementString("end_house_number", oDataReader["end_house_number"].ToString());

                if (oDataReader["odd_even_both"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("odd_even_both", oDataReader["odd_even_both"].ToString());
                }

                _oXmlWriter.WriteStartElement("non_house_address");

                if (oDataReader["street_direction"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("street_direction", oDataReader["street_direction"].ToString());
                }

                _oXmlWriter.WriteElementString("street_name", oDataReader["street_name"].ToString());

                if (oDataReader["street_suffix"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("street_suffix", oDataReader["street_suffix"].ToString());
                }

                if (oDataReader["address_direction"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("address_direction", oDataReader["address_direction"].ToString());
                }

                _oXmlWriter.WriteElementString("state", oDataReader["state"].ToString());
                _oXmlWriter.WriteElementString("city", oDataReader["city"].ToString());

                if (oDataReader["zip"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("zip", oDataReader["zip"].ToString());
                }

                _oXmlWriter.WriteEndElement(); // end non_house_address

                _oXmlWriter.WriteElementString("precinct_id", oDataReader["precinct_id"].ToString());

                _oXmlWriter.WriteEndElement(); // end street_segment
            }
        }

        public void WritePrecincts(OracleCommand oCmd) {
            OracleCommand oCmdPolling = new OracleCommand();
            oCmdPolling.Connection = oCmd.Connection;
            oCmdPolling.CommandType = CommandType.Text;

            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();
            OracleDataReader oPollingReader;

            while (oDataReader.Read()) {
                _oXmlWriter.WriteStartElement("precinct");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                _oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                _oXmlWriter.WriteElementString("locality_id", oDataReader["locality_id"].ToString());

                // based on relationship between polling place and precinct
                oCmdPolling.CommandText = "";
                oPollingReader = oCmdPolling.ExecuteReader();

                // if there is only one polling place per precinct, this may be unnecessary
                while (oPollingReader.Read()) {
                    _oXmlWriter.WriteElementString("polling_location_id", oDataReader["polling_location_id"].ToString());
                }
                
                _oXmlWriter.WriteEndElement();
            }
        }

        public void WriteLocalities(OracleCommand oCmd) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while (oDataReader.Read()) {
                _oXmlWriter.WriteStartElement("locality");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                _oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                _oXmlWriter.WriteElementString("state_id", oDataReader["state_id"].ToString());
                _oXmlWriter.WriteElementString("type", oDataReader["type"].ToString());
                _oXmlWriter.WriteEndElement();
            }
        }

        public FeedWriter(DateTime oScriptStart, string sFilePath, string sFIPS, NameValueCollection oWriterSettings) {
            _oXmlWriter = CreateXmlWriter(oWriterSettings,FormatFileName(sFilePath,sFIPS,oScriptStart));
        }
    }

    class DataConnection {
        // private members
        private NameValueCollection _oDbConfig;

        private string FormatDbConnString() {
            String oradb = "Data Source=(DESCRIPTION="
                + "(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1})))"
                + "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={2})));"
                + "User Id={3};Password={4};";

            return String.Format(oradb,
                _oDbConfig.Get("Host"),
                _oDbConfig.Get("Port"),
                _oDbConfig.Get("Service"),
                _oDbConfig.Get("User"),
                _oDbConfig.Get("Pass"));
        }

        public DataConnection(NameValueCollection oDbConfig) {
            _oDbConfig = oDbConfig;
        }

        public OracleConnection DbConnect() {
            return new OracleConnection(FormatDbConnString());
        }
    }

    class Program {

        public static void Main(String[] args) {
            DateTime oScriptStart = DateTime.UtcNow;
            /*NameValueCollection oAppSettings = ConfigurationManager.AppSettings;
            NameValueCollection oDbSettings = ConfigurationManager.GetSection("dbSettings") as NameValueCollection;
            NameValueCollection oVipSettings = ConfigurationManager.GetSection("vipSettings") as NameValueCollection;
            NameValueCollection oXmlSettings = ConfigurationManager.GetSection("xmlWriterSettings") as NameValueCollection;
            NameValueCollection oElectionAdminSettings = ConfigurationManager.GetSection("electionOfficial") as NameValueCollection;*/
            
            NameValueCollection oDbSettings = new NameValueCollection();
            NameValueCollection oVipSettings = new NameValueCollection();
            NameValueCollection oXmlSettings = new NameValueCollection();
            NameValueCollection oElectionAdminSettings = new NameValueCollection();

            oDbSettings.Add("Host", "localhost");
            oDbSettings.Add("Port", "1521");
            oDbSettings.Add("Service", "VIPFEED");
            oDbSettings.Add("User", "DBSNMP");
            oDbSettings.Add("Pass", "m4yN4rd46");

            oVipSettings.Add("FilePath", "C:\\Users\\Jared\\Documents\\projects\\vip\\feeds\\");
            oVipSettings.Add("SchemaURL", "http://election-info-standard.googlecode.com/files/vip_spec_v2.2a.xsd");
            oVipSettings.Add("SchemaVer", "2.2");
            oVipSettings.Add("StateName", "State");
            oVipSettings.Add("StateFIPS", "20");
            oVipSettings.Add("Description", "The Secretary of State is the chief state election official for the state. This feed provides information on registration, advance voting, and the location of polling locations for registered voters in the state.");
            oVipSettings.Add("OrganizationURL", "http://sos.gov");

            oXmlSettings.Add("Indent", "true");
            oXmlSettings.Add("IndentChars", "  ");
            oXmlSettings.Add("ConformanceLevel", "Fragment");

            DataConnection oDataConn = new DataConnection(oDbSettings);
            FeedWriter oFeedWriter = new FeedWriter(
                oScriptStart,
                oVipSettings.Get("FilePath"),
                oVipSettings.Get("StateFIPS"),
                oXmlSettings
            );

            oFeedWriter.WriteStartElement("vip_object");

            oFeedWriter.WriteHeader(oVipSettings);

            // this may not be necessary if the election_officials are stored in the db
            //oFeedWriter.WriteElementFromConfig("election_official", oElectionAdminSettings);
            
            try {
                using (OracleConnection oConn = oDataConn.DbConnect()) {
                    oConn.Open();

                    OracleCommand oCmd = new OracleCommand();
                    oCmd.Connection = oConn;

                    oFeedWriter.WritePollingPlaces(oCmd);
                    oFeedWriter.WriteStreetSegments(oCmd);
                    oFeedWriter.WritePrecincts(oCmd);
                    oFeedWriter.WriteLocalities(oCmd);
                }

            } catch (OracleException ex) {
                switch (ex.Number) {
                    case 1:
                        Console.WriteLine("Error attempting to insert duplicate data.");
                        break;
                    case 12545:
                        Console.WriteLine("The database is unavailable.");
                        break;
                    default:
                        Console.WriteLine("Database error: " + ex.Message.ToString());
                        break;
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message.ToString());
            }

            oFeedWriter.WriteEndElement();
        }
    }
}
