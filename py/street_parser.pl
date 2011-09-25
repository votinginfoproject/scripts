#!/usr/bin/perl

=head1 [street_parser.pl]

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
  
  foreach my $name (keys %{$localities}) {
    my @output_line;
    push(@output_line,($localities->{$name}, $name, "County"));
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

sub output_polling_locations {
  my $polling_locations = shift;
  my $base_dir = shift;
  my @output_headers = qw(id location_name line1 line2 city state zip);
  my $headers = join("|",@output_headers);
  
  # output file
  open my $wh, ">", "$base_dir/polling_location.txt" or die "$base_dir/polling_location.txt: $!";
  my $csv = Text::CSV_XS->new ({
    eol => $/,
    sep_char => "|"
  });
  
  print $wh "$headers" . "\n";
  
  foreach my $id (keys %{$polling_locations}) {
    my @output_line;
    push(@output_line,($id, $polling_locations->{$id}->{location_name}, $polling_locations->{$id}->{line1}, $polling_locations->{$id}->{line2}, $polling_locations->{$id}->{city}, $polling_locations->{$id}->{state}, $polling_locations->{$id}->{zip}));
    $csv->print($wh, \@output_line);
  }
  
  close $wh or die "$base_dir/polling_location.txt: $!";
  
  return;
}

sub parse_csv_args {
  my $csv_str = shift;
  return [split ',', $csv_str];
}

if ($file) {
  # get file minutia
  my $path =  abs_path($file) or die "File Not Found: $!";
  my $base_dir = dirname($path) or die "Directory Not Found: $!";
  my $header;
  my @output_headers = qw(id start_house_number end_house_number odd_even_both street_direction street_name street_suffix address_direction state city zip precinct_id);
  my $street_headers = join("|", @output_headers);
  my $localities = {};
  my $precincts = {};
  my $polling_locations = {};
  my $locality_count = 54001; # keep track of the county numbers
  
  # set up the CSV reader
  my $fh = Tie::Handle::CSV->new(
    $file,
    header => 1,
    key_case => 'lower',
    sep_char => '|'
  );
  
  # output file
  open my $wh, ">", "$base_dir/street_segment.txt" or die "$base_dir/street_segment.txt: $!";
  my $csv = Text::CSV_XS->new ({
    eol => $/,
    sep_char => "|"
  });

#   my $csv = Tie::Handle::CSV->new(
#     "$base_dir/street_segment.txt",
#     header => [qw/ id start_house_number end_house_number odd_even_both street_direction street_name street_suffix address_direction state city zip precinct_id /],
#     key_case => 'lower',
#     sep_char => '|',
#     open_mode => '>'
#   );
  
  
  $header = $fh->header;
  
  
  # read from file
  while (my $csv_line = <$fh>) {
    my @output_line;
    my $polling_address;
    
    if(!exists $localities->{$csv_line->{'county_name'}}) {
      $localities->{$csv_line->{'county_name'}} = $locality_count;
      # ASSERT: Counties are in ascending lexical order
      # keep the numbers odd to follow FIPS5
      $locality_count += 2;
    }
    
    if(!exists $polling_locations->{$localities->{$csv_line->{'county_name'}} . $csv_line->{'precinct_number'}}) {
      $polling_address = join(" ",
        $csv_line->{'ad_num'},
        $csv_line->{'ad_num_half'}
      );
      
      $polling_address = join(" ",
        trim($polling_address),
        $csv_line->{'ad_str1'}
      );
      
      $polling_address = trim($polling_address) . " " . $csv_line->{'unit'};
      
      $polling_locations->{$localities->{$csv_line->{'county_name'}} . $csv_line->{'precinct_number'}} = {
        location_name => $csv_line->{'poll_name'},
        line1 => trim($polling_address),
        line2 => $csv_line->{'ad_str2'},
        city => $csv_line->{'ad_city'},
        state => $csv_line->{'ad_st'},
        zip => trim($csv_line->{'ad_zip5'} . $csv_line->{'ad_zip4'}),
      };
    }
    
    if(!exists $precincts->{$localities->{$csv_line->{'county_name'}} . $csv_line->{'precinct_number'}}) {
      $precincts->{$localities->{$csv_line->{'county_name'}} . $csv_line->{'precinct_number'}} = {
        name => $csv_line->{'county_name'} . " " .$csv_line->{'precinct_number'},
        locality_id => $localities->{$csv_line->{'county_name'}},
        polling_location_id => $localities->{$csv_line->{'county_name'}} . $csv_line->{'precinct_number'},
      };
    }
    
    # because the reader eats the header, line number starts at '2'
    if ($. == 2) {
      print $wh "$street_headers" . "\n";
    }
    
    foreach my $key (keys %{$csv_line}) {
      $csv_line->{$key} = trim($csv_line->{$key})
    }
    
    $address = join(" ",
      $csv_line->{'house no'},
      $csv_line->{'street'},
      $csv_line->{'street2'}
    );
    
    $address = trim($address) . " " . $csv_line->{'unit'};

    $address = join(" ",
      trim($address),
      $csv_line->{'city'},
      $csv_line->{'state'} . ",",
      $csv_line->{'zip'}
    );
    
    $hashref = Geo::StreetAddress::US->parse_location($address);
    
    if(scalar $hashref) {
      foreach my $el (@output_headers) {
        if($el eq 'id') {
          push(@output_line, $.);
        }

        if($el eq 'start_house_number' || $el eq 'end_house_number') {
          push(@output_line, $hashref->{'number'} || "");
        }

        if($el eq 'odd_even_both') {
          push(@output_line, 'both');
        }

        if($el eq 'street_direction') {
          push(@output_line, $hashref->{'prefix'} || "");
        }
        
        if($el eq 'street_name') {
          push(@output_line, $hashref->{'street'} || "");
        }
        
        if($el eq 'street_suffix') {
          if(exists $hashref->{'type'}) {
            push(@output_line, $hashref->{'type'} || "");
          } elsif(exists $hashref->{'type1'}) {
            push(@output_line, $hashref->{'type1'} || "");
          } elsif(exists $hashref->{'type2'}) {
            push(@output_line, $hashref->{'type2'} || "");
          }
        }
        
        if($el eq 'address_direction') {
          push(@output_line, $hashref->{'suffix'} || "");
        }
        
        if($el eq 'state') {
          push(@output_line, $hashref->{'state'} || "");
        }
        
        if($el eq 'city') {
          push(@output_line, $hashref->{'city'} || "");
        }
        
        if($el eq 'zip') {
          push(@output_line, $hashref->{'zip'} || "");
        }
        
        if($el eq 'precinct_id') {
          push(@output_line, $localities->{$csv_line->{'county_name'}} . $csv_line->{'precinct_number'} || "");
        }
        
      }
      
      
      $csv->print($wh, \@output_line);
      
    } else {
      next;
    }
  }
  
  close $wh or die "street_segment.txt: $!";
  
  output_localities($localities, $base_dir);
  output_precincts($precincts, $base_dir);
  output_polling_locations($polling_locations, $base_dir);
  
}

my $run_time = time() - $^T;

print "Job took $run_time seconds\n";

#if ($address) {
#    $hashref = Geo::StreetAddress::US->parse_address($address);
#    $hashref = Geo::StreetAddress::US->parse_location($address) || { error => 'Could not geocode' };
#    $json = encode_json $hashref;
#    print "$json";
#}
