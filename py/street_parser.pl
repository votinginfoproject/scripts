#!/usr/bin/perl                                                                                                                                                                                                                

=head1 [address_parse.pl]

 description:

=cut

use Geo::StreetAddress::US;
#use Data::Dumper;
use JSON;
use strict;
use warnings;
use Getopt::Long;

my $prog = $0;
my $usage = <<EOQ;
Usage for $0:

  >$prog [-test -help -verbose -address]

EOQ

my $date = get_date();

my $help;
my $test;
my $debug;
my $verbose =1;
my $address ='';

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

                    'run'       => \$run,
                    'dry_run'   => \$dry_run,

                   );

if ($help || !$ok || !$address) {
    print $usage;
    exit;
}

sub get_date {
    my ($day, $mon, $year) = (localtime)[3..5] ;

    return my $date= sprintf "%04d-%02d-%02d", $year+1900, $mon+1, $day;
}

sub parse_csv_args {

    my $csv_str =shift;
    return [split ',', $csv_str];
}

if ($address) {
    my $hashref = Geo::StreetAddress::US->parse_address($address);
    my $json_obj = encode_json $hashref;
    
    print "$json_obj";
}