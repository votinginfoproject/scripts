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
        private string _sOutputFileNameFormat = "{0}vipFeed-{1}.xml";
        private XmlWriter _oXmlWriter;

        private XmlWriter CreateXmlWriter(NameValueCollection oXmlConfig, String sOutputFile) {
            XmlWriterSettings xmlSettings = new XmlWriterSettings();
            xmlSettings.Indent = Boolean.Parse(oXmlConfig.Get("Indent"));
            xmlSettings.IndentChars = oXmlConfig.Get("IndentChars");
            xmlSettings.ConformanceLevel = oXmlConfig.Get("ConformanceLevel") == "Fragment" ? ConformanceLevel.Fragment : ConformanceLevel.Document;

            return XmlWriter.Create(sOutputFile, xmlSettings);
        }

        private string FormatFileName(string sFilePath, string sFIPS) {
            return String.Format(_sOutputFileNameFormat,sFilePath,sFIPS);
        }

        public void WriteStartElement(string sElementName) {
            _oXmlWriter.WriteStartElement(sElementName);
        }

        public void WriteEndElement() {
            _oXmlWriter.WriteEndElement();
	    _oXmlWriter.Flush();
        }

        public void WriteHeader(NameValueCollection oVipConfig) {
            _oXmlWriter.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");
            _oXmlWriter.WriteAttributeString("xsi", "noNamespaceSchemaLocation", null, oVipConfig.Get("SchemaURL"));
            _oXmlWriter.WriteAttributeString("schemaVersion", oVipConfig.Get("SchemaVer"));

            WriteStartElement("state");
            _oXmlWriter.WriteAttributeString("id", oVipConfig.Get("StateFIPS"));
            _oXmlWriter.WriteElementString("name", oVipConfig.Get("StateName"));
            WriteEndElement();

            WriteStartElement("source");
            _oXmlWriter.WriteAttributeString("id", "1");
            _oXmlWriter.WriteElementString("vip_id", oVipConfig.Get("StateFIPS"));
            _oXmlWriter.WriteElementString("name", oVipConfig.Get("SourceName"));
            _oXmlWriter.WriteElementString("datetime", oVipConfig.Get("ScriptStart"));
            _oXmlWriter.WriteElementString("description", oVipConfig.Get("Description"));
            _oXmlWriter.WriteElementString("organization_url", oVipConfig.Get("OrganizationURL"));
            WriteEndElement();
        }

        public void WriteElementFromConfig(String sElementName, NameValueCollection oConfig) {
            WriteStartElement(sElementName);

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

            WriteEndElement();
        }

        public void WritePollingPlaces(OracleCommand oCmd) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while (oDataReader.Read()) {
                WriteStartElement("polling_location");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                WriteStartElement("address");

                if (oDataReader["location_name"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("location_name", oDataReader["location_name"].ToString());
                }

                _oXmlWriter.WriteElementString("line1", oDataReader["line1"].ToString());
                _oXmlWriter.WriteElementString("city", oDataReader["city"].ToString());
                _oXmlWriter.WriteElementString("state", oDataReader["state"].ToString());
                _oXmlWriter.WriteElementString("zip", oDataReader["zip"].ToString());
                WriteEndElement();

                if (oDataReader["directions"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("directions", oDataReader["directions"].ToString());
                }

                if (oDataReader["polling_hours"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("polling_hours", oDataReader["polling_hours"].ToString());
                }

                WriteEndElement();
            }
        }

        public void WriteStreetSegments(OracleCommand oCmd) {
            String start_house = "";
            String end_house = "";

            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while (oDataReader.Read()) {
                WriteStartElement("street_segment");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());

                if(oDataReader["start_house_number"] == DBNull.Value || oDataReader["start_house_number"].ToString() == "") {
                    start_house = "1";
                } else {
                    start_house = oDataReader["start_house_number"].ToString();
                }

                if (oDataReader["end_house_number"] == DBNull.Value || oDataReader["end_house_number"].ToString() == "") {
                    end_house = "999999";
                } else {
                    end_house = oDataReader["end_house_number"].ToString();
                }

                _oXmlWriter.WriteElementString("start_house_number", start_house);
                _oXmlWriter.WriteElementString("end_house_number", end_house);

                if (oDataReader["odd_even_both"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("odd_even_both", oDataReader["odd_even_both"].ToString());
                }

                WriteStartElement("non_house_address");

                if (oDataReader["street_direction"] != DBNull.Value) {
                    _oXmlWriter.WriteElementString("street_direction", oDataReader["street_direction"].ToString());
                }

                _oXmlWriter.WriteElementString("street_name", oDataReader["street_name"].ToString().Replace("\"",""));

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

                WriteEndElement(); // end non_house_address

                _oXmlWriter.WriteElementString("precinct_id", oDataReader["precinct_id"].ToString());

                WriteEndElement(); // end street_segment
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
                WriteStartElement("precinct");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                _oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                _oXmlWriter.WriteElementString("locality_id", oDataReader["locality_id"].ToString());

                // based on relationship between polling place and precinct
                oCmdPolling.CommandText = "";
                oPollingReader = oCmdPolling.ExecuteReader();

                // if there is only one polling place per precinct, this may be unnecessary
                while (oPollingReader.Read()) {
                    _oXmlWriter.WriteElementString("polling_location_id", oPollingReader["polling_location_id"].ToString());
                }
                
                WriteEndElement();
            }
        }

        public void WritePrecinctsSplits(OracleCommand oCmd) {
            OracleCommand oCmdPolling = new OracleCommand();
            oCmdPolling.Connection = oCmd.Connection;
            oCmdPolling.CommandType = CommandType.Text;

            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();
            OracleDataReader oPollingReader;

            while (oDataReader.Read()) {
                WriteStartElement("precinct_split");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                _oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                _oXmlWriter.WriteElementString("precinct_id", oDataReader["precinct_id"].ToString());

                // based on relationship between polling place and precinct
                oCmdPolling.CommandText = "select * from Table where id=";
                oPollingReader = oCmdPolling.ExecuteReader();

                // if there is only one polling place per precinct, this may be unnecessary
                while (oPollingReader.Read()) {
                    _oXmlWriter.WriteElementString("polling_location_id", oPollingReader["polling_location_id"].ToString());
                }
                
                WriteEndElement();
            }
        }

        public void WriteLocalities(OracleCommand oCmd) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while (oDataReader.Read()) {
                WriteStartElement("locality");
                _oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                _oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                _oXmlWriter.WriteElementString("state_id", oDataReader["state_id"].ToString());
                _oXmlWriter.WriteElementString("type", oDataReader["type"].ToString());
                WriteEndElement();
            }
        }

        public FeedWriter(string sFilePath, string sFIPS, NameValueCollection oWriterSettings) {
            _oXmlWriter = CreateXmlWriter(oWriterSettings,FormatFileName(sFilePath,sFIPS));
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
            oDbSettings.Add("Pass", "P4SSW0RD");

            oVipSettings.Add("FilePath", "C:\\Users\\Jared\\Documents\\projects\\vip\\feeds\\");
            oVipSettings.Add("SchemaURL", "http://election-info-standard.googlecode.com/files/vip_spec_v2.2a.xsd");
            oVipSettings.Add("SchemaVer", "2.2");
            oVipSettings.Add("StateName", "State");
            oVipSettings.Add("ScriptStart", oScriptStart.ToString("yyyy-MM-ddTHH:mm:ss"));
            oVipSettings.Add("StateFIPS", "20");
            oVipSettings.Add("SourceName", "State of Somewhere");
            oVipSettings.Add("Description", "The Secretary of State is the chief state election official for the state. This feed provides information on registration, advance voting, and the location of polling locations for registered voters in the state.");
            oVipSettings.Add("OrganizationURL", "http://sos.gov");

            oXmlSettings.Add("Indent", "true");
            oXmlSettings.Add("IndentChars", "  ");
            oXmlSettings.Add("ConformanceLevel", "Fragment");

            DataConnection oDataConn = new DataConnection(oDbSettings);
            FeedWriter oFeedWriter = new FeedWriter(
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
		    oFeedWriter.WritePrecinctSplits(oCmd);
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
