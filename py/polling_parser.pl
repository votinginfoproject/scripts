#!/usr/bin/perl

=head1 [polling_parser.pl]

 description:

=cut

use File::Basename;
use Geo::StreetAddress::US;
use Text::CSV_XS;
use Tie::Handle::CSV;
#use Data::Dumper;
use JSON;
use strict;
use warnings;
use Getopt::Long;
use Cwd 'abs_path';

my $prog = $0;
my $usage = <<EOQ;
Usage for $0:

  >$prog [-test -help -verbose -address -file]

EOQ

my $date = get_date();

my $help;
my $test;
my $debug;
my $verbose = 1;
my $address = '';
my $file = '';

my $bsub;
my $log;
my $stdout;
my $stdin;
my $run;
my $dry_run;

my $ok = GetOptions(
                    'test'      => \$test,
                    'debug=i'   => \$debug,
                    'verbose=i' => \$verbose,
                    'help'      => \$help,
                    'log'       => \$log,
                    'address=s' => \$address,
                    'file=s'    => \$file,
                    
                    'run'       => \$run,
                    'dry_run'   => \$dry_run,
                   );

# output
my $json;
my $hashref;

if ($help || !$ok || !$file) {
  print $usage;
  exit;
}

sub trim {
  my $string = shift;
  $string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub get_date {
  my ($day, $mon, $year) = (localtime)[3..5];
  return my $date= sprintf "%04d-%02d-%02d", $year+1900, $mon+1, $day;
}


if ($file) {
  # get file minutia
  my $path =  abs_path($file) or die "File Not Found: $!";
  my $base_dir = dirname($path) or die "Directory Not Found: $!";
  my $header;
  my @output_headers = qw(id location_name line1 city state zip);
  my $polling_headers = join("|", @output_headers);
  my $polling_locations = {};
  
  # set up the CSV reader
  my $fh = Tie::Handle::CSV->new(
    $file,
    header => [qw(region_assigned site_type id name line1 city pp_tel owner contact_name vendor_phone fax owner_line1 owner_line2 owner_line3 date_surveyed status date_in_service pay_indicator type_of_facility accessible comment)],
    key_case => 'lower',
    sep_char => '~'
  );  
  
  # output file                                                                                                                                                                                                 
  open my $wh, ">", "$base_dir/polling_location.txt" or die "$base_dir/polling_location.txt: $!";
  my $csv = Text::CSV_XS->new ({
    eol => $/,
    sep_char => "|"
  });
  
  $header = $fh->header;
  
  
  # read from file
  while (my $csv_line = <$fh>) {
    my @output_line;
    
    # because the reader eats the header, line number starts at '2'                                                                                                                                             
    if ($. == 1) {
      print $wh "$polling_headers" . "\n";
    }
        
    if($csv_line->{'site_type'} eq "P") {
      push(@output_line, (
        $csv_line->{'id'},
        $csv_line->{'name'},
        $csv_line->{'line1'},
        $csv_line->{'city'},
        "AK",
        "",
      ));
      
      $csv->print($wh, \@output_line);
    }
  }
  
  close $wh or die "$base_dir/polling_location.txt: $!";
 
}

my $run_time = time() - $^T;

print "Job took $run_time seconds\n";

#if ($address) {
#    $hashref = Geo::StreetAddress::US->parse_address($address);
#    $hashref = Geo::StreetAddress::US->parse_location($address) || { error => 'Could not geocode' };
#    $json = encode_json $hashref;
#    print "$json";
#}
