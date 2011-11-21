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

sub output_localities {
  my $localities = shift;
  my $base_dir = shift;
  my @output_headers = qw(id name type);
  my $headers = join("|",@output_headers);
  
  # output file
  open my $wh, ">", "$base_dir/locality.txt" or die "$base_dir/locality.txt: $!";
  my $csv = Text::CSV_XS->new ({
    eol => $/,
    sep_char => "|"
  });
  
  print $wh "$headers" . "\n";
  
  foreach my $key (keys %{$localities}) {
    my @output_line;
    push(@output_line,($localities->{$key}->{'id'}, $localities->{$key}->{'name'}, "County"));
    $csv->print($wh, \@output_line);
  }
  
  close $wh or die "$base_dir/locality.txt: $!";
  
  return;
}

sub output_precincts {
  my $precincts = shift;
  my $base_dir = shift;
  my @output_headers = qw(id name locality_id polling_location_id);
  my $headers = join("|",@output_headers);
  
  # output file
  open my $wh, ">", "$base_dir/precinct.txt" or die "$base_dir/precinct.txt: $!";
  my $csv = Text::CSV_XS->new ({
    eol => $/,
    sep_char => "|"
  });
  
  print $wh "$headers" . "\n";
  
  foreach my $id (keys %{$precincts}) {
    my @output_line;
    push(@output_line,($id, $precincts->{$id}->{name}, $precincts->{$id}->{locality_id}, $precincts->{$id}->{polling_location_id}));
    $csv->print($wh, \@output_line);
  }
  
  close $wh or die "$base_dir/precinct.txt: $!";
  
  return;
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
  my $precincts = {};
  my $localities = { 'K' => { 'id' => 10001, 'name' => "Kent" }, 'N' => { 'id' => 10003, 'name' => "New Castle" }, 'S' => { 'id' => 10005, 'name' => "Sussex" } };

  # set up the CSV reader
  my $fh = Tie::Handle::CSV->new(
    $file,
    header => 1,
    key_case => 'lower',
    sep_char => ','
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
    if ($. == 2) {
      print $wh "$polling_headers" . "\n";
    }
        
    if(!exists $precincts->{$csv_line->{'code-ed'} . $csv_line->{'code-rd'}}) {
      $precincts->{$csv_line->{'code-ed'} . $csv_line->{'code-rd'}} = {
        name => "Precinct " . $csv_line->{'code-ed'} . $csv_line->{'code-rd'},
        locality_id => $localities->{$csv_line->{'county'}}->{'id'},
        polling_location_id => $localities->{$csv_line->{'county'}}->{'id'} . $csv_line->{'code-ed'} . $csv_line->{'code-rd'},
      };
    }

    push(@output_line, (
      $localities->{$csv_line->{'county'}}->{'id'} . $csv_line->{'code-ed'} . $csv_line->{'code-rd'},
      $csv_line->{'name'},
      $csv_line->{'addr 1'},
      $csv_line->{'city'},
      "DE",
      $csv_line->{'zip'},
    ));
      
    $csv->print($wh, \@output_line);

  }
  
  close $wh or die "polling_location.txt: $!";

  output_localities($localities, $base_dir);
  output_precincts($precincts, $base_dir); 
}

my $run_time = time() - $^T;

print "Job took $run_time seconds\n";

#if ($address) {
#    $hashref = Geo::StreetAddress::US->parse_address($address);
#    $hashref = Geo::StreetAddress::US->parse_location($address) || { error => 'Could not geocode' };
#    $json = encode_json $hashref;
#    print "$json";
#}
