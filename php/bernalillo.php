<?php

//Directory to place files in. end with slash
$localDirectory="../../../files/bernalillo/";
//Log file. Name as you wish.
$logFname=$localDirectory . "log.txt";
//Accessible web directory with files in it. end with slash
$webDirectory="http://localhost/vip/bernalillo/";
$precinctsFile="precincts.csv";
$locationsFile="polling_locations_numeric.csv";
$segmentsFile="street_segment.csv";


/**
 * User should adjust these values to automatic upload to FTP
 */
$autoFTPupload=False;
$ftp_server="";
$ftp_user="";
$ftp_pass="";
$ftp_directory="";
//$autoFTPupload=True;
$ftp_server="mindlessphilosopher.net";
$ftp_user="mindless";
$ftp_pass="";
$ftp_directory="misc/";


/**
 * User should adjust these values for his or her geography
 */
 
$stateName="New Mexico";
$countyName="Bernalillo";
//State fips code
$stateFIPS=35;
//County fips code
$countyFIPS=35001;
//Organization URL: the county's election's website
$organizationURL="http://www.cocovote.us/";
/**
 * End user adjustments
 */ 

/**
 * Run Script
 */ 
//Script takes a few minutes to run, so I eliminate the time limit
set_time_limit(0);
//Gets the datetime for use in filename and source object
$dateArray=getdate();
//File name. Do NOT adjust. File name is specified 
$fnameND="vipFeed-$countyFIPS.xml";
$fnameZipND=$fnameND . ".zip";
$fname=$localDirectory . $fnameND;
$fnameZip=$localDirectory . $fnameZipND;
//File handles
$logHandle = fopen($logFname, "w+");
$xmlHandle = fopen($fname, "w");

//download data
//download_data("http://talkingpointsmemo.com/images/","bernanke-askance-large.jpg","");
download_data($webDirectory,$precinctsFile,$localDirectory);
download_data($webDirectory,$locationsFile,$localDirectory);
download_data($webDirectory,$segmentsFile,$localDirectory);

//Add contents to feed.
$contents=addHeader($dateArray);fwrite($xmlHandle, $contents);
$contents=addPrecincts(file_get_contents($localDirectory . $precinctsFile));fwrite($xmlHandle, $contents);
$contents=addLocations(file_get_contents($localDirectory . $locationsFile));fwrite($xmlHandle, $contents);
$contents=addSegments(file_get_contents($localDirectory . $segmentsFile),$xmlHandle);fwrite($xmlHandle, $contents);
$contents=addFooter();fwrite($xmlHandle, $contents);

zip_file($fname,$fnameZip);

if($autoFTPupload) {
  post_file($ftp_directory . $fnameZipND,$fnameZip,$ftp_server,$ftp_user,$ftp_pass);
}

addLogMessage("DONE!");

//Close handles
fclose($logHandle);
fclose($xmlHandle);

/**
 * Download the data. Put in local directory
 * See decriptions of inputs above 
 */
function download_data($webDirectory,$dataFileName,$localDirectory) {
addLogMessage("Start download of " . $dataFileName);
  if(!@copy($webDirectory . $dataFileName,$localDirectory . $dataFileName)) {
      $errors= error_get_last();
      addLogMessage("COPY ERROR: " . $errors['type']);
      addLogMessage("<br />\n" . $errors['message']);
  }
addLogMessage("End download of " . $dataFileName);
} 


/**
 * Input: the output from the current getdate() 
 * Returns the header for the file (XML header, source object, state, locality)
 */ 
function addHeader($dateArray) {
  global $stateFIPS;
  global $countyFIPS;
  global $stateName;
  global $countyName;
  global $webDirectory;
  global $organizationURL;
  $str="";
  $str.="<?xml version=\"1.0\" standalone=\"yes\" ?" . ">\n";
  $str.="<vip_object xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" 
  xsi:noNamespaceSchemaLocation=\"http://election-info-standard.googlecode.com/files/election%20spec%20v2.2a.xsd\" schemaVersion=\"2.2\">\n";
  $str.="<state id=\"$stateFIPS\"><name>$stateName</name></state>\n";
  $str.="<source id=\"0\">\n";
  $str.="<name>" . $countyName . " County, ". $stateName . "</name>\n";
  $str.="<vip_id>$countyFIPS</vip_id>\n";		
  $str.="<datetime>" . $dateArray['year'] . "-" . str_pad($dateArray['mon'],2,"0",STR_PAD_LEFT) . "-" . str_pad($dateArray['mday'],2,"0",STR_PAD_LEFT) . "T" . str_pad($dateArray['hours'],2,"0",STR_PAD_LEFT) . 
    ":" . str_pad($dateArray['minutes'],2,"0",STR_PAD_LEFT) . ":" . str_pad($dateArray['seconds'],2,"0",STR_PAD_LEFT) . "</datetime>\n";
  //$str.="<description>" . $countyName . " has posted the official precinct and polling place list at $webDirectory  
  //This feed is the VIP Feed of that data as interpreted by VIP staffers and includes precincts, polling location, and street segments.</description>\n";
  $str.="<description>" . $countyName . " has provided the official precinct and polling place list at.  
  This feed is the VIP Feed of that data as interpreted by VIP staffers and includes precincts, polling location, and street segments.</description>\n";
  $str.="<organization_url>". $organizationURL ."</organization_url>\n";
  $str.="</source>\n";
  $str.="<locality id=\"$countyFIPS\"><name>$countyName</name><state_id>$stateFIPS</state_id><type>county</type></locality>\n";
  return $str;
}

/**
 * Input: the data (in string form) from the precincts file.
 * Returns the feed contents for precincts
 */ 
function addPrecincts($datastr) {
  global $countyFIPS;
  addLogMessage("Start Precincts");
  $precArray=parseCSV($datastr);
  $str=""; $savedID = -1;
  foreach($precArray as $prec) {
    $curID=$prec[0];
    if ((strlen($curID)>0) && strcmp($curID,"split_id")!=0) {
      if ($savedID!=$curID) {
        $precID="999" . $prec[2];
        $str.="<precinct_split id=\"" . $curID . "\"><name>". trim($prec[1]) . "</name><precinct_id>$precID</precinct_id>";
        $str.="</precinct_split>\n";
        if (strcmp($prec[4],"y")==0) {
          $str.="<precinct id=\"" . $precID . "\"><name>". trim($prec[3]) . "</name><locality_id>$countyFIPS</locality_id>";
          $str.="<polling_location_id>". trim($prec[5]) . "</polling_location_id>";
          $str.="</precinct>\n";
        }
      }
    }
    $savedID=$curID;
  }
  addLogMessage("End Precincts");
  return $str;
}

/**
 * Input: the data (in string form) from the polling locations file.
 * Returns the feed contents for polling locations
 */
function addLocations($datastr) {
  addLogMessage("Start Locations");
  $locArray=parseCSV($datastr);
  $str=""; $savedID = -1; $savedName="";
  foreach($locArray as $loc) {
    $curID=$loc[0]; $curName=$loc[2];
    if ((strlen($curID)>0) && strcmp($curID,"ID")!=0 && strcmp(trim($curName),"MAIL BALLOT PRECINCT")!=0) {
      if ($savedID!=$curID) {
        $str.="<polling_location id=\"" . $curID . "\">";
        $str.="<address>";
        $str.=" <location_name>" . xmlspecialchars($loc[2]) . "</location_name>";
        $str.=" <line1>" . xmlspecialchars($loc[3]) . "</line1>";
        $str.=" <city>" . xmlspecialchars($loc[4]) . "</city>";
        $str.=" <state>NM</state>";
        $str.=" <zip>" . xmlspecialchars($loc[5]) . "</zip>";
        $str.="</address>";
        $str.="<directions>" . xmlspecialchars(trim($loc[6])) . "</directions>";
        $str.="</polling_location>\n";
      }
    }
    $savedID=$curID; $savedName=$curName;
  }
  addLogMessage("End Locations");
  return $str;

}

/**
 * Inputs: the filename of the segments; the file handle for feed output
 * Different inputs from precinct function because the size of the segments
 * data requires it to be written in chunks
 * Returns: TRUE on successful writing to feed file handle ($xmlHandle)
 *  
 * Note: Alter $maxSeg below for testing purposes. This value should be
 * greater than the number of segments
 */
function addSegments($datastr) {
  $str="";
  $house_num_saved=0;
  $saved_street="";
  $i=0;
  $maxSeg=7000000;
  addLogMessage("Start Segments");

  $segArray=parseCSV($datastr);
  addLogMessage("File parsed");
  foreach($segArray as $seg) {
    if (($i % 10000)==0 && $i>0) {
      fwrite($xmlHandle, $str);      
      addLogMessage("Segments to " . $i . " done");
      $str="";    
    }
    if($i>$maxSeg) {
      break;
    }
    if (strlen($seg[0])>0 && strcmp($seg[0],"id")!=0) {
      $str.="<street_segment id=\"" . $seg[0] . "\">\n";
      $str.="<start_house_number>" . $seg[5] . "</start_house_number>\n";
      $str.="<end_house_number>" . $seg[6] . "</end_house_number>\n";
      $oeb=strcmp($seg[7],"E")==0 ? "Even" : (strcmp($seg[7],"O")==0 ? "Odd" : "Both");
      $str.=" <odd_even_both>". $oeb . "</odd_even_both>\n";
      $str.="<precinct_split_id>" . (int)((double)$seg[10]*10) .  "</precinct_split_id>\n";
      $str.="<precinct_id>999" . (int)(trim($seg[13])) .  "</precinct_id>\n";
      $str.="<non_house_address>\n";
      $str.=" <street_direction>" . $seg[1]. "</street_direction>\n";
      $str.=" <street_name>" . $seg[2]. "</street_name>\n";
      $str.=" <street_suffix>" . $seg[3]. "</street_suffix>\n";
      $str.=" <address_direction>" . $seg[4]. "</address_direction>\n";
      $str.=" <city>" . $seg[11]. "</city>\n";
      $str.=" <state>NM</state>\n";
      $str.=" <zip>" . trim($seg[12]) . "</zip>\n";
      $str.="</non_house_address>\n";
      $str.="</street_segment>\n"; 
    }
    $i=$i+1;
  }
  addLogMessage("Segments done");
  return $str;
}

/**
 * Zip file $fname into archive $fnameZip
 */ 
function zip_file($fname,$fnameZip) {
  addLogMessage("Start zipping file");
  $zip = new ZipArchive();
  
  if ($zip->open($fnameZip, ZIPARCHIVE::CREATE)!==TRUE) {
      addLogMessage("cannot open <$fnameZip>\n");
  }
  $zip->addFile($fname);
  $zip->close();
  addLogMessage("End zipping file");
}

/**
 * Returns: footer contents
 */ 
function addFooter() {
  return "</vip_object>";
}

/**
 * Input: (1) the filename on the server; (2) the local dir/filename of the feed;
 * (3) ftp info (server, username, password)
 * Action: post the feed file to this FTP server
 * Returns nothing 
 */
function post_file($remoteName,$fname,$ftp_server,$ftp_user,$ftp_pass) {
 
  addLogMessage("Posting File to server $ftp_server");
  
  // set up a connection or die
  $conn_id = ftp_connect($ftp_server) or die("Couldn't connect to $ftp_server"); 
  
  // try to login
  if (@ftp_login($conn_id, $ftp_user, $ftp_pass)) {
     addLogMessage("Connected as $ftp_user@$ftp_server\n");
  } else {
     addLogMessage("Couldn't connect as $ftp_user\n");
  }
  if (ftp_put($conn_id, $remoteName, $fname, FTP_BINARY)) {
   addLogMessage("successfully uploaded $fname as $remoteName\n");
  } else {
   addLogMessage("There was a problem while uploading $fname\n");
  }
  
  // close the connection
  ftp_close($conn_id); 

}



/**
 * Add log message $msg to global log
 */
function addLogMessage($msg) {
  global $logHandle;
  fwrite($logHandle,date(DATE_ATOM). " -- " . $msg . "\n");
}


   /**
     * Create a 2D array from a CSV string
     *
     * @param mixed $data 2D array
     * @param string $delimiter Field delimiter
     * @param string $enclosure Field enclosure
     * @param string $newline Line seperator
     * @return
     */
    function parseCSV($data, $delimiter = ',', $enclosure = '"', $newline = "\n"){
        $pos = $last_pos = -1;
        $end = strlen($data);
        $row = 0;
        $quote_open = false;
        $trim_quote = false;

        $return = array();

        // Create a continuous loop
        for ($i = -1;; ++$i){
            ++$pos;
            // Get the positions
            $comma_pos = strpos($data, $delimiter, $pos);
            $quote_pos = strpos($data, $enclosure, $pos);
            $newline_pos = strpos($data, $newline, $pos);

            // Which one comes first?
            $pos = min(($comma_pos === false) ? $end : $comma_pos, ($quote_pos === false) ? $end : $quote_pos, ($newline_pos === false) ? $end : $newline_pos);

            // Cache it
            $char = (isset($data[$pos])) ? $data[$pos] : null;
            $done = ($pos == $end);

            // It it a special character?
            if ($done || $char == $delimiter || $char == $newline){

                // Ignore it as we're still in a quote
                if ($quote_open && !$done){
                    continue;
                }

                $length = $pos - ++$last_pos;

                // Is the last thing a quote?
                if ($trim_quote){
                    // Well then get rid of it
                    --$length;
                }

                // Get all the contents of this column
                $return[$row][] = ($length > 0) ? str_replace($enclosure . $enclosure, $enclosure, substr($data, $last_pos, $length)) : '';

                // And we're done
                if ($done){
                    break;
                }

                // Save the last position
                $last_pos = $pos;

                // Next row?
                if ($char == $newline){
                    ++$row;
                }

                $trim_quote = false;
            }
            // Our quote?
            else if ($char == $enclosure){

                // Toggle it
                if ($quote_open == false){
                    // It's an opening quote
                    $quote_open = true;
                    $trim_quote = false;

                    // Trim this opening quote?
                    if ($last_pos + 1 == $pos){
                        ++$last_pos;
                    }

                }
                else {
                    // It's a closing quote
                    $quote_open = false;

                    // Trim the last quote?
                    $trim_quote = true;
                }

            }

        }

        return $return;
    }


function xmlspecialchars($text) {
   return str_replace('&#039;', '&apos;', htmlspecialchars($text, ENT_QUOTES));
}

?>
