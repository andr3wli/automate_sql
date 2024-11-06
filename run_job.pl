#!/usr/bin/perl
# By: Andrew Li any questions can be sent to andrew.li@gov.bc.ca

# load perl modules 
use strict;
use warnings;
use File::Spec::Functions qw( catfile );
use DBD::Oracle;
use MIME::Lite; # for mailing 
use Cwd 'abs_path';
use Time::HiRes qw(time);

# Check if a job name is provided - no email for failure 
if (@ARGV != 1) {
    die "Usage: $0 <job_name>\n";
}

my $job_name = $ARGV[0];
my $parms_folder = catfile $ENV{HOME}, "nm_jobs/parms";
my $job_path = abs_path($0); # purely for the email messages for aesthetic purposes
my $current_time = `date +"%H:%M:%S"`; # again this is purely ffor aesthisc purpsoes 
my $current_date = `date +"%Y-%m-%d"`; # again again, this is puely for aestheic purposes 
my $sql_file = catfile($parms_folder, "$job_name.sql");     
my $mail_file = catfile($parms_folder, "$job_name.mail");

# make sure the sql fiel and mail file exists - no eimail for failuter
if (!-e $sql_file && !-e $mail_file) {
    die "Both SQL file '$sql_file' and mail file '$mail_file' are not found.\n";
} elsif (!-e $sql_file) {
    die "SQL file '$sql_file' not found.\n";
} elsif (!-e $mail_file) {
    die "Mail file '$mail_file' not found.\n";
}

# pull the email addresses from the file - no email for failure
my @my_addresses;
open(my $mail_fh, '<', $mail_file) or die "Could not open '$mail_file': $!\n";
while (my $line = <$mail_fh>) {
    chomp $line;
    next if $line =~ /^\s*#/;
    push @my_addresses, $line if $line =~ /.+@.+\..+/;  
}
close($mail_fh);

# make sure that there is at least one perons on the mail list - no email for failure
if (@my_addresses == 0) {
    die "At least one valid address must be present in the '$mail_file'";
}

# fix to make sure EVERYONE on the list gets sent email not just the first person
my $recipient_list = join(', ', @my_addresses);

print "Email to be sent to the following: $recipient_list\n";

# default failure subject line 
my $mail_subject = "** ${job_path} ${job_name} FAILED **";

# failure messages body - have everything the same except the failure reason 
sub failure_message {
    my ($failure_reason) = @_;
    return "$job_path $job_name FAILED \n$failure_reason\n\n Failure Time: $current_time Failure Date: $current_date\n";
}
# set as globalk variabel that use thorughout the script, will be populated only when error occurs 
my $mail_body;

my $final_email = sub {
    my ($subject, $body) = @_;  # Accept body as parameter, keep the subject the same 

    my $msg = MIME::Lite->new(
        Type     => 'text/plain', 
        Encoding => 'quoted-printable',
        To       => $recipient_list,
        Subject  => $subject ,
        Data     => $body,
    );

    $msg->send;
};

# Attempt to open the password file - send failure email if this does not exists
my $pw_file = catfile $ENV{HOME}, ".pw_file";
# my $pw_file = catfile $ENV{HOME}, ".pw_file_test";

sub read_dotfile {
    open(my $fh2, '<', $pw_file) or do {
    my $failure_reason = "\nCould not open file '$pw_file': $!";
    $mail_body = failure_message($failure_reason);
    $final_email->($mail_subject, $mail_body);
    die $mail_body;                                       
};
    my @lines = <$fh2>;
    close($fh2);

    my @elements;
    foreach my $line (@lines) {
        next if $line =~ /^\s*#/;   
        chomp $line;
        my @line_elements = split(',', $line);
        push @elements, @line_elements;
    }

    return @elements;
}
my @login_info = read_dotfile();
my $db_username = $login_info[0];
my $db_password = $login_info[1];

# Check that the first element is the username and starts with string and ends with _o
if ($db_username !~ /_(lvl[234])$/) {
    my $failure_reason = "\ndatabase username not found in the expected format (example: andrew_li_lvl3) in $pw_file";
    $mail_body = failure_message($failure_reason);
    $final_email->($mail_subject, $mail_body);
    die $mail_body;
}
# Check that the fourth element is present 
if (!defined $db_password) {
    my $failure_reason = "\ndatabase password not found in $pw_file";
    $mail_body = failure_message($failure_reason);
    $final_email->($mail_subject, $mail_body);
    die $mail_body;
}

# function to remove comments for the sql files
sub remove_comments {
    my ($sql) = @_;
    $sql =~ s/--.*?$//gm; 
    $sql =~ s/\/\*.*?\*\// /gs; 
    $sql =~ s/^\s+|\s+$//g;

    return $sql;
}

# open the corresponding sql file 
open(my $sql_fh, '<', $sql_file) or do {
    my $failure_reason = "\nCould not open file '$sql_file': $!";
    $mail_body = failure_message($failure_reason);
    $final_email->($mail_subject, $mail_body);
    die $mail_body;   
};
my $sql_content = do { local $/; <$sql_fh> }; 
close($sql_fh);
$sql_content =~ s/^\s+|\s+$//g;

# remove all comments in the sql fiels 
my $cleaned_sql = remove_comments($sql_content);

# split by semi colon if there are any
my @sql_statements = split(/;\s*/, $cleaned_sql);


# Connect to the database 
my $db_name = 'database';
my $dbh = DBI->connect("dbi:Oracle:$db_name", $db_username, $db_password)
    or do {
        my $failure_reason = "\nCannot connect to database: $DBI::errstr\n";
            $failure_reason .= "Check database name, username, or password in the $pw_file";
        $mail_body = failure_message($failure_reason);
        $final_email->($mail_subject, $mail_body);
        die $mail_body;
    };

# my $sth = $dbh->prepare($sql_content) or do{
#     my $failure_reason = "\nCould not prepare sql statement: $DBI::errstr\n";
#     $mail_body = failure_message($failure_reason);
#     $final_email->($mail_subject, $mail_body);
#     die $mail_body;  
# };

# $sth->execute() or do {
#      my $failure_reason = "\nCould not execute sql statement: $DBI::errstr\n";
#      $mail_body = failure_message($failure_reason);
#     $final_email->($mail_subject, $mail_body);
#     die $mail_body;  
# };

# populate this after eachiteration or stuff used in loop
my $sth;  
my $any_failure = 0;
my $successful_queries = 0; 
my $success_body = "** $job_path $job_name completed without errors **\n\n";
my @all_rows;  
my $script_start_time = time();

# some counters for sql commands 
my $rows_inserted = 0;
my $rows_deleted = 0;
my $tables_created = 0;
my $tables_dropped = 0;

foreach my $sql (@sql_statements) {
    next if $sql =~ /^\s*$/;  

    # Start timing this query
    my $query_start_time = time();  

    # Get the type of query: delete, insert, create, drop, and everything else assume select
    my $operation_type;
    if ($sql =~ /^\s*DELETE/i) {
        $operation_type = 'DELETE';
    } elsif ($sql =~ /^\s*INSERT/i) {
        $operation_type = 'INSERT';
    } elsif ($sql =~ /^\s*CREATE/i) {
        $operation_type = 'CREATE';
    } elsif ($sql =~ /^\s*DROP/i) {
        $operation_type = 'DROP';
    } elsif ($sql =~ /^\s*SELECT/i) {
        $operation_type = 'SELECT';
    } else {
        $operation_type = 'SELECT'; # anything else just assume its select 
    }

    # prep the SQL statement
    $sth = $dbh->prepare($sql) or do {
        my $failure_reason = "\nCould not prepare SQL statement: $DBI::errstr\nSQL: $sql";
        my $mail_body = failure_message($failure_reason);
        $final_email->($mail_subject, $mail_body);
        die $mail_body;
    };

    # execute the SQL statement
    if ($sth->execute()) {
        my @query_rows;
        while (my $row = $sth->fetchrow_arrayref) {
            push @query_rows, [@$row];   
        }
        push @all_rows, @query_rows;

        my $num_columns = @{$sth->{NAME}};
        my $query_execution_time = time() - $query_start_time;
        # everyone gets this - number of queries 
        $success_body .= "Query number $successful_queries was executed successfully:\n$sql\n";

        # build the success message based on query type:
        if ($operation_type eq 'DELETE') {
            $rows_deleted = $sth->rows;
            $success_body .= "$rows_deleted rows deleted; " .
                            "Execution time: " . sprintf("%.2f", $query_execution_time) . " seconds\n\n";
        } elsif ($operation_type eq 'INSERT') {
            $rows_inserted = $sth->rows;
            $success_body .= "$rows_inserted rows inserted; " .
                            "Execution time: " . sprintf("%.2f", $query_execution_time) . " seconds\n\n";
        } elsif ($operation_type eq 'CREATE') {
            $tables_created++; 
            $success_body .= "$tables_created tables created; " . 
                            "Execution time: " . sprintf("%.2f", $query_execution_time) . " seconds\n\n";
        } elsif ($operation_type eq 'DROP') {
            $tables_dropped++; 
            $success_body .= "$tables_dropped tables dropped; " .
                            "Execution time: " . sprintf("%.2f", $query_execution_time) . " seconds\n\n";
        } else {
            # everyone gets this assumme its just some seletect 
        $success_body .= "Number of rows from this query: " . scalar @query_rows . "; " .
                            "Number of columns from this query: $num_columns; " .
                            "Execution time: " . sprintf("%.2f", $query_execution_time) . " seconds\n\n";
        }
        
        $successful_queries++;
    } else {
        $any_failure = 1;
        my $failure_reason = "\nCould not execute SQL statement: $DBI::errstr\nSQL: $sql";
        my $mail_body = failure_message($failure_reason);
        $final_email->($mail_subject, $mail_body);
        die $mail_body;
    }
}

# only if all queries ran successfully- summarize 
unless ($any_failure) {
    my $success_subject = "**${job_path} ${job_name} was successful**";
    my $script_duration = time() - $script_start_time;
    $success_body .= "Total number of SQL queries executed: $successful_queries\n";
    $success_body .= "Total job duration: " . sprintf("%.2f", $script_duration) . " seconds\n";
    $success_body .= "Completion Time: $current_time";
    $success_body .= "Completion Date: $current_date\n";

    $final_email->($success_subject, $success_body);
    print $success_body;
}

# close it 
$sth->finish();
$dbh->disconnect;

