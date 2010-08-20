using System;
using System.Configuration;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Text;
using System.Xml;
using Oracle.DataAccess.Client;

namespace Vip {
    class Builder {
        // private members
        private DateTime _oScriptStart;
        private String _sOutputFileName;

        private OracleConnection DbConnect () {
            return new OracleConnection (FormatDbConnString(GetConfigSection("dbSettings")));
        }

        private string FormatDbConnString (NameValueCollection oDbConfig) {
            String oradb = "Data Source=(DESCRIPTION="
                + "(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1})))"
                + "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ORCL)));"
                + "User Id={2};Password={3};";

            return String.Format(oradb,
                oDbConfig.Get("Host"),
                oDbConfig.Get("Port"),
                oDbConfig.Get("User"),
                oDbConfig.Get("Pass"));
        }

        private NameValueCollection GetConfigSection (string sSection) {
            return ConfigurationSettings.GetConfig(sSection) as NameValueCollection;
        }

        private XmlWriter CreateXmlWriter (NameValueCollection oXmlConfig, String sOutputFile) {
            XmlWriterSettings xmlSettings = new XmlWriterSettings();
            xmlSettings.Indent = oXmlConfig.Get("Indent");
            xmlSettings.IndentChars = oXmlConfig.Get("IndentChars");
            xmlSettings.ConformanceLevel = oXmlConfig.Get("ConformanceLevel")=="Fragment" ? ConformanceLevel.Fragment : ConformanceLevel.Document;

            return XmlWriter.Create(sOutputFile, xmlSettings);
        }

        private void WriteHeader (NameValueCollection oVipConfig, XmlWriter oXml) {
            oXml.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");
            oXml.WriteAttributeString("xsi", "noNamespaceSchemaLocation", null, oVipConfig.Get("SchemaURL"));
            oXml.WriteAttributeString("schemaVersion", oVipConfig.Get("SchemaVer"));
            
            oXml.WriteStartElement("state");
            oXml.WriteAttributeString("id", oVipConfig.Get("StateFIPS"));
            oXml.WriteElementString("name", oVipConfig.Get("StateName"));
            oXml.WriteEndElement();

            oXml.WriteStartElement("source");
            oXml.WriteAttributeString("id", "1");
            oXml.WriteElementString("vip_id", oVipConfig.Get("StateFIPS"));
            oXml.WriteElementString("datetime", oVipConfig.Get("ScriptStart"));
            oXml.WriteElementString("description", oVipConfig.Get("Description"));
            oXml.WriteElementString("organization_url", oVipConfig.Get("OrganizationURL"));
            oXml.WriteEndElement();
        }

        private void WriteElementFromConfig (String sElementName, NameValueCollection oConfig, XmlWriter oXml) {
            oXml.WriteStartElement(sElementName);

            if (oConfig.ContainsKey("id")) {
                oXml.WriteAttributeString("id", oConfig.Get("id"));
                // clear the id so it's not used in the loop
                oConfig.Remove("id");
            }
            
            foreach (String s in oConfig.AllKeys) {
                oXml.WriteElementString(s, oConfig[s]);
            }

            oXml.WriteEndElement();
        }

        private void WriteStreetSegments (OracleCommand oCmd, XmlWriter oXmlWriter) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while(oDataReader.Read()) {
                oXmlWriter.WriteStartElement("street_segment");
                oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());

                oXmlWriter.WriteElementString("start_house_number", oDataReader["start_house_number"].ToString());
                oXmlWriter.WriteElementString("end_house_number", oDataReader["end_house_number"].ToString());

                if(oDataReader["odd_even_both"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("odd_even_both", oDataReader["odd_even_both"].ToString());
                }

                oXmlWriter.WriteStartElement("non_house_address");

                if(oDataReader["street_direction"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("street_direction", oDataReader["street_direction"].ToString());
                }

                oXmlWriter.WriteElementString("street_name", oDataReader["street_name"].ToString());

                if(oDataReader["street_suffix"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("street_suffix", oDataReader["street_suffix"].ToString());
                }

                if(oDataReader["address_direction"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("address_direction", oDataReader["address_direction"].ToString());
                }

                oXmlWriter.WriteElementString("state", oDataReader["state"].ToString());
                oXmlWriter.WriteElementString("city", oDataReader["city"].ToString());

                if(oDataReader["zip"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("zip", oDataReader["zip"].ToString());
                }

                oXmlWriter.WriteEndElement(); // end non_house_address

                oXmlWriter.WriteElementString("precinct_id", oDataReader["precinct_id"].ToString());

                oXmlWriter.WriteEndElement(); // end street_segment
            }
        }

        private void WritePrecincts (OracleCommand oCmd, XmlWriter oXmlWriter) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while(oDataReader.Read()) {
                oXmlWriter.WriteStartElement("precinct");
                oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                oXmlWriter.WriteElementString("locality_id", oDataReader["locality_id"].ToString());
                oXmlWriter.WriteElementString("polling_location_id", oDataReader["polling_location_id"].ToString());
                oXmlWriter.WriteEndElement();
            }
        }

        private void WriteLocalities (OracleCommand oCmd, XmlWriter oXmlWriter) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while(oDataReader.Read()) {
                oXmlWriter.WriteStartElement("locality");
                oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                oXmlWriter.WriteElementString("name", oDataReader["name"].ToString());
                oXmlWriter.WriteElementString("state_id", oDataReader["state_id"].ToString());
                oXmlWriter.WriteElementString("type", oDataReader["type"].ToString());
                oXmlWriter.WriteEndElement();
            }
        }

        private void WritePollingPlaces (OracleCommand oCmd, XmlWriter oXmlWriter) {
            oCmd.CommandText = "select * from Table";
            oCmd.CommandType = CommandType.Text;

            OracleDataReader oDataReader = oCmd.ExecuteReader();

            while(oDataReader.Read()) {
                oXmlWriter.WriteStartElement("polling_location");
                oXmlWriter.WriteAttributeString("id", oDataReader["id"].ToString());
                oXmlWriter.WriteStartElement("address");

                if(oDataReader["location_name"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("location_name", oDataReader["name"].ToString());
                }

                oXmlWriter.WriteElementString("line1", oDataReader["address"].ToString());
                oXmlWriter.WriteElementString("city", oDataReader["city"].ToString());
                oXmlWriter.WriteElementString("state", oDataReader["state"].ToString());
                oXmlWriter.WriteElementString("zip", oDataReader["zip"].ToString());
                oXmlWriter.WriteEndElement();

                if(oDataReader["directions"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("directions", oDataReader["directions"].ToString());
                }

                if(oDataReader["polling_hours"]!=DBNull.Value) {
                    oXmlWriter.WriteElementString("polling_hours", oDataReader["polling_hours"].ToString());
                }

                oXmlWriter.WriteEndElement();
            }
        }

        public static void Main (String[] args) {
            _oScriptStart = DateTime.UtcNow;

            NameValueCollection oVipConfig = GetConfigSection("vipSettings");

            // add the scriptStart time to the config and pass it into the header function
            oVipConfig.Add("ScriptStart", _oScriptStart.ToString("yyyy-MM-ddTHH:mm:ss"));

            _sOutputFileName = String.Format("{0}vipFeed-{1}-{2}.xml",
                                             oVipConfig.Get("FilePath"),
                                             oVipConfig.Get("StateFIPS"),
                                             _oScriptStart.ToString("yyyy-MM-ddTHH-mm-ss"));

            XmlWriter oXmlWriter = CreateXmlWriter(GetConfigSection("xmlWriterSettings"),
                                                   _sOutputFileName);

            oXmlWriter.WriteStartElement("vip_object");

            WriteHeader(GetConfigSection("vipSettings"), oXmlWriter);

            // this may not be necessary if the election_officials are stored in the db
            WriteElementFromConfig("election_official",
                                   GetConfigSection("electionOfficial1"),
                                   oXmlWriter);
            WriteElementFromConfig("election_official",
                                   GetConfigSection("electionOfficial2"),
                                   oXmlWriter);

            try {
                using (OracleConnection oConn = DbConnect()) {
                    oConn.Open();

                    OracleCommand oCmd = new OracleCommand();
                    oCmd.Connection = oConn;

                    WritePollingPlaces(oCmd, oXmlWriter);
                    WriteStreetSegments(oCmd, oXmlWriter);
                    WritePrecincts(oCmd, oXmlWriter);
                    WriteLocalities(oCmd, oXmlWriter);
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
            }
            catch (Exception ex) {
                Console.WriteLine(ex.Message.ToString());
            }

            oXmlWriter.WriteEndElement();
        }
    }
}