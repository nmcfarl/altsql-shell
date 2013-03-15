package App::AltSQL::Model::Pg;

=head1 NAME

App::AltSQL::Model::Pg

=head1 DESCRIPTION

Initial attempt at a Postgres model class

=cut

use Moose;
use DBI;
use Sys::SigAction qw(set_sig_handler);
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
extends 'App::AltSQL::Model';

has [qw(host user password database port)] => ( is => 'ro' );
has [qw(no_auto_rehash)] => ( is => 'ro' );
sub args_spec {
	return (
		host => {
			cli  => 'host|h=s',
			help => '-h HOSTNAME | --host HOSTNAME',
		},
		user => {
			cli  => 'user|u=s',
			help => '-u USERNAME | --user USERNAME',
		},
		password => {
			help => '-p | --password=PASSWORD | -pPASSWORD',
		},
		database => {
			cli  => 'database|d=s',
			help => '-d DATABASE | --database DATABASE',
		},
		port => {
			cli  => 'port=i',
			help => '--port PORT',
		},
	);
}

sub db_connect {
	my $self = shift;
	my $dsn = 'DBI:Pg:' . join (';',
		map { "$_=" . $self->$_ }
		grep { defined $self->$_ }
		qw(database host port)
	);
	my $dbh = DBI->connect($dsn, $self->user, $self->password, {
		PrintError => 0,
	}) or die $DBI::errstr . "\nDSN used: '$dsn'\n";
	$self->dbh($dbh);

	if ($self->database) {
		$self->current_database($self->database);
        $self->update_autocomplete_entries($self->database);
	}
}


sub update_autocomplete_entries {
	my ($self, $database) = @_;

	return if $self->no_auto_rehash;
    #$sth = $dbh->table_info( $catalog, $schema, $table, $type );
#    warn Dumper($database,$sth->fetchall_arrayref );

#    return
	my $cache_key = 'autocomplete_' . $database;
	if (! $self->{_cache}{$cache_key}) {
		$self->log_debug("Reading table information for completion of table and column names\nYou can turn off this feature to get a quicker startup with -A\n");
		my %autocomplete;
        my $rows = $self->dbh->selectall_arrayref(
"SELECT
  c.relname,
  pgatt.attname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     join pg_catalog.pg_attribute pgatt on pgatt.attrelid = c.oid
WHERE       
  pg_catalog.pg_table_is_visible(c.oid)
  and   pgatt.attnum > 0 AND NOT pgatt.attisdropped
  and reltype > 0
  and n.nspname = 'public'
  and c.relkind != 'S'
ORDER BY 1,2 ;");
        #warn Dumper($rows);
        foreach my $row (@$rows) {
			$autocomplete{$row->[0]} = 1; # Table
			$autocomplete{$row->[1]} = 1; # Column
			$autocomplete{$row->[0] . '.' . $row->[1]} = 1; # Table.Column
        }

        for my $word  (qw(
			action add after aggregate all alter as asc auto_increment avg avg_row_length
			both by
			cascade change character check checksum column columns comment constraint create cross
			current_date current_time current_timestamp
			data database databases day day_hour day_minute day_second
			default delayed delay_key_write delete desc describe distinct distinctrow drop
			enclosed escape escaped explain
			fields file first flush for foreign from full function
			global grant grants group
			having heap high_priority hosts hour hour_minute hour_second
			identified ignore index infile inner insert insert_id into isam
			join
			key keys kill last_insert_id leading left limit lines load local lock logs long 
			low_priority
			match max_rows middleint min_rows minute minute_second modify month myisam
			natural no
			on optimize option optionally order outer outfile
			pack_keys partial password primary privileges procedure process processlist
			read references reload rename replace restrict returns revoke right row rows
			second select show shutdown soname sql_big_result sql_big_selects sql_big_tables sql_log_off
			sql_log_update sql_low_priority_updates sql_select_limit sql_small_result sql_warnings starting
			status straight_join string
			table tables temporary terminated to trailing type
			unique unlock unsigned update usage use using
			values varbinary variables varying
			where with write
			year_month
			zerofill

            date_trunc NOW 
                       )){
            $autocomplete{$word} = 1; # Row
        }


		$self->{_cache}{$cache_key} = \%autocomplete;
	}


	$self->app->term->autocomplete_entries( $self->{_cache}{$cache_key} );
}




sub handle_sql_input {
	my ($self, $input, $render_opts) = @_;

	# Figure out the verb of the SQL by either using regex or a parser.  If we
	# use the parser, we get error checking here instead of the server.
	my $verb;
	($verb, undef) = split /\s+/, $input, 2;

	# Run the SQL
	
	my $t0 = gettimeofday;

	my $sth = $self->execute_sql($input);
	return unless $sth; # error may have been reached (and reported)

	# Track which database we're in for autocomplete
	if (my ($database) = $input =~ /^use \s+ (\S+)$/ix) {
		$self->current_database($database);
	}

	my %timing = ( prepare_execute => gettimeofday - $t0 );

	my $view = $self->app->create_view(
		sth => $sth,
		timing => \%timing,
		verb => $verb,
	);
	$view->render(%$render_opts);

	return $view;
}

no Moose;
__PACKAGE__->meta->make_immutable;

=head1 COPYRIGHT

Copyright (c) 2012 Eric Waters and Shutterstock Images (http://shutterstock.com).  All rights reserved.  This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included with this module.

=head1 AUTHOR

Eric Waters <ewaters@gmail.com>

=cut

1;
