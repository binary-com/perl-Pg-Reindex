package Pg::Reindex;
require Exporter;
@ISA       = qw(Exporter);
@EXPORT_OK = qw(prepare reindex);

use 5.010001;
use strict;
use warnings;

use DBI;
use DBD::Pg qw/:async/;
use AnyEvent;
use POSIX qw/SIGTERM SIGINT SIG_BLOCK SIG_UNBLOCK/;
use Getopt::Long;
use Pod::Usage;
use Time::HiRes ();

our ( $dbh, $opt_dryrun, $opt_throttle_on, $opt_throttle_off );

=head1 NAME

Pg::Reindex - The great new Pg::Reindex!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Pg::Reindex;

    my $foo = Pg::Reindex->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS


=head2 reindex

=cut

sub lg {
    return print localtime() . ": ", @_;
}

sub query {
    my ( $descr, @param ) = @_;
    my $sql = pop @param;

    # warn $sql;

    my $tm = Time::HiRes::time;

    my $stmt
        = ref $sql ? $sql : $dbh->prepare( $sql, { pg_async => PG_ASYNC } );

    my $done   = AE::cv;
    my $cancel = sub {
        $dbh->pg_cancel if $dbh->{pg_async_status} == 1;
        $done->send;
    };
    my $pg_w = AE::io $dbh->{pg_socket}, 0, sub {
        $dbh->pg_ready and $done->send;
    };

    my $sigblock = POSIX::SigSet->new( SIGTERM, SIGINT );
    POSIX::sigprocmask SIG_BLOCK, $sigblock;
    my @sig_w = map { AE::signal $_, $cancel } qw/TERM INT/;
    $stmt->execute(@param);
    POSIX::sigprocmask SIG_UNBLOCK, $sigblock;

    $done->wait;

    die "query cancelled\n" unless $dbh->{pg_async_status} == 1;

    my $rc = $dbh->pg_result;
    my $result = $stmt->{Active} ? $stmt->fetchall_arrayref : undef;

    lg sprintf "$descr took %.3f s\n", ( Time::HiRes::time- $tm ) if $descr;

    return wantarray ? ( $rc, $result ) : $result;
}

sub wquery {    ## no critic
    goto \&query unless $opt_dryrun;

    my ( $descr, @param ) = @_;
    my $sql = pop @param;

    my $n = 1;
    $sql =~ s/\?/'$'.$n++/ge;
    $sql =~ s/\$(\d+)/$dbh->quote($param[$1-1])/ge;

    print "$sql;\n";

    return 1;
}

sub throttle {
    return if $opt_dryrun;
    state $q = $dbh->prepare( <<'SQL', { pg_async => PG_ASYNC } );
SELECT coalesce(max(pg_xlog_location_diff(pg_current_xlog_location(), r.flush_location)), 0)
  FROM pg_stat_replication r
SQL

    my ($xlog_diff) = @{ query( '', $q )->[0] };

    if ( $xlog_diff > $opt_throttle_on ) {
        lg "streaming lag = $xlog_diff ==> pausing\n";
    LOOP: {
            do {
                select undef, undef, undef, 1;    ## no critic
                ($xlog_diff) = @{ query( '', $q )->[0] };
            } while ( $xlog_diff > $opt_throttle_off );

            # sleep for another 30 sec and check every second the lag.
            # sometimes the wal sender process disconnects and reconnects
            # a moment later. In that case we may have fallen below the
            # throttle limit simply because we checked at the wrong time.
            for ( my $i = 0; $i < 30; $i++ ) {
                select undef, undef, undef, 1;    ## no critic
                ($xlog_diff) = @{ query( '', $q )->[0] };
                redo LOOP if $xlog_diff > $opt_throttle_off;
            }
        }
        lg "streaming lag = $xlog_diff -- continuing\n";
    }
    return;
}

sub next_index {
    my @list = query '', <<'SQL';
WITH wl AS (
    UPDATE reindex.worklist
       SET status='in progress'
     WHERE idx=(SELECT idx
                 FROM reindex.worklist
                 WHERE status<>'done'
                 ORDER BY ord
                 LIMIT 1)
 RETURNING idx
)
SELECT c.oid, n.nspname, quote_ident(n.nspname), c.relname, quote_ident(c.relname),
       pg_catalog.pg_get_indexdef(c.oid) indexdef,
       pg_catalog.pg_relation_size(c.oid::regclass)
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid
  JOIN wl ON wl.idx=c.oid
SQL

    return @{ $list[1]->[0] || [] };
}

sub do_transaction {
    my ( $stmt_pointer, $limit, $sub ) = @_;

LOOP: {
        eval {
            if ($opt_dryrun) {
                wquery '', 'BEGIN ISOLATION LEVEL REPEATABLE READ';
            } else {
                $dbh->begin_work;
                wquery '', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ';
            }

            $sub->();

            if ($opt_dryrun) {
                wquery '', 'COMMIT';
            } else {
                $dbh->commit;
            }
            1;
        } or do {
            my $sqlstate = $dbh->state;
            my $err      = $@;
            eval { $dbh->rollback };    ## no critic
            if ( $limit-- ) {
                for my $state (qw/40P01 40001/)
                {    # deadlock detected; serialization failure
                    if ( $sqlstate eq $state ) {
                        $err = ">>$$stmt_pointer<<\n$err" if $stmt_pointer;
                        $err =~ s/\s+$//;
                        $err =~ s/\n/\n      /g;
                        lg
                            "      SQL state $state ==> retry transaction\n      $err\n";
                        redo LOOP;
                    }
                }
            }
            die $err;
        };
    }
    return;
}

sub wait_for_concurrent_tx {
    return if $opt_dryrun;
    eval {
        $dbh->begin_work;

        while (
            !query( '',
                'SELECT txid_current()=txid_snapshot_xmin(txid_current_snapshot())'
            )->[0]->[0]
            )
        {
            select undef, undef, undef, .5;    ## no critic
        }

        $dbh->rollback;
        1;
    } or do {
        my $err = $@;
        eval { $dbh->rollback };               ## no critic
        die $err;
    };
    return;
}

sub _reindex {
    my ( $oid, $nspname, $quoted_nspname, $idxname, $quoted_idxname, $idxdef,
        $size, $opt_validate )
        = @_;

    throttle;    # wait for streaming replicas to catch up

    lg "Rebuilding Index $quoted_nspname.$quoted_idxname\n";

    my @log_id;
    @log_id = query '', $oid, <<'SQL' unless $opt_dryrun;
INSERT INTO reindex.log(tstmp, nspname, tblname, idxname, sz_before, status)
SELECT now(), n.nspname, tc.relname, ic.relname, pg_catalog.pg_relation_size(i.indexrelid::regclass), 'started'
  FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_class ic ON i.indexrelid=ic.oid
  JOIN pg_catalog.pg_class tc ON i.indrelid=tc.oid
  JOIN pg_catalog.pg_namespace n ON ic.relnamespace=n.oid
 WHERE i.indexrelid=$1
RETURNING id
SQL

    my $tmp = '__temp_reidx';
    $idxdef =~ s/^(CREATE (?:UNIQUE )?INDEX) (\S+)/$1 CONCURRENTLY $tmp/
        or die "Cannot replace index name in $idxdef\n";

    my $retry = 5;
    my ( @rc, $err );
    while ( --$retry > 0 ) {
        @rc = eval { wquery "  CREATE CONCURRENTLY", $idxdef } and last;
        $err = $@;
        eval {
            query "$quoted_nspname.$quoted_idxname creation failed. Dropping",
                qq{DROP INDEX $quoted_nspname.$tmp};
            1;
        }
            or warn "While dropping the index: $@";
    }

    unless ( $rc[0] ) {
        chomp $err;

        query '', $log_id[1]->[0]->[0], $err, <<'SQL' unless $opt_dryrun;
UPDATE reindex.log
   SET status='failed to create temp index: ' || $2,
       tm_taken=now()-tstmp
 WHERE id=$1
SQL

        die "Cannot create index: $err";
    }

    my @revalidate;
    my $current_cmd;

    eval {
        do_transaction \$current_cmd, 100, sub {

            # check if the index still exists
            @rc = query '', $oid, $nspname, $idxname, $current_cmd = <<'SQL';
SELECT 1
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=$1
   AND c.relkind='i'
   AND n.nspname=$2
   AND c.relname=$3
SQL

            if ( $rc[1]->[0]->[0] == 1 ) {
                unless ($opt_dryrun) {
                    @rc = query '', $log_id[1]->[0]->[0],
                        "$quoted_nspname.$tmp", $current_cmd = <<'SQL';
UPDATE reindex.log
   SET sz_after=pg_catalog.pg_relation_size($2::regclass)
 WHERE id=$1
RETURNING sz_after
SQL
                    lg sprintf(
                        "    size: %d ==> %d (%.2f%%)\n",
                        $size,
                        $rc[1]->[0]->[0],
                        $rc[1]->[0]->[0] * 100 / $size - 100
                    );
                }

                my @trans
                    = (
                    "ALTER INDEX $quoted_nspname.$tmp RENAME TO $quoted_idxname"
                    );
                @rc = query '', $oid, <<'SQL';
SELECT con.conname, quote_ident(con.conname),
       n.nspname, quote_ident(n.nspname),
       con.contype,
       pg_get_constraintdef(con.oid),
       con.conrelid::regclass::text,
       con.confrelid::regclass::text,
       con.confmatchtype,
       key.key, fkey.fkey
  FROM pg_catalog.pg_constraint con
  JOIN pg_catalog.pg_namespace n ON n.oid=con.connamespace
 CROSS JOIN LATERAL (SELECT array_agg(quote_ident(a.attname))
                       FROM unnest(con.conkey) x(k) JOIN pg_catalog.pg_attribute a
                         ON x.k=a.attnum AND a.attrelid=con.conrelid) key(key)
 CROSS JOIN LATERAL (SELECT array_agg(quote_ident(a.attname))
                       FROM unnest(con.confkey) x(k) JOIN pg_catalog.pg_attribute a
                         ON x.k=a.attnum AND a.attrelid=con.confrelid) fkey(fkey)
  JOIN (VALUES ('p'::TEXT, 1::INT),
               ('u'::TEXT, 2::INT),
               ('f'::TEXT, 3::INT)) ord(type, ord) ON ord.type=con.contype
 WHERE con.conindid=$1
   AND con.contype<>'x'        -- exclusion constraints are not yet implemented
 ORDER BY ord.ord ASC
SQL

                if ( @{ $rc[1] } ) {
                    for my $con ( @{ $rc[1] } ) {
                        my ($conname,        $quoted_conname, $nspname,
                            $quoted_nspname, $contype,        $condef,
                            $rel,            $frel,           $matchtype,
                            $key,            $fkey
                        ) = @$con;
                        if ( $contype eq 'u' ) {
                            unshift @trans,
                                "ALTER TABLE $rel DROP CONSTRAINT $quoted_conname";
                            push @trans,
                                (
                                "ALTER TABLE $rel ADD  CONSTRAINT $quoted_conname "
                                    . "UNIQUE USING INDEX $quoted_idxname" );
                        } elsif ( $contype eq 'p' ) {
                            unshift @trans,
                                "ALTER TABLE $rel DROP CONSTRAINT $quoted_conname";
                            push @trans,
                                (
                                "ALTER TABLE $rel ADD  CONSTRAINT $quoted_conname "
                                    . "PRIMARY KEY USING INDEX $quoted_idxname"
                                );
                        } elsif ( $contype eq 'f' ) {
                            unshift @trans,
                                "ALTER TABLE $rel DROP CONSTRAINT $quoted_conname";
                            push @trans,
                                "ALTER TABLE $rel ADD  CONSTRAINT $quoted_conname $condef NOT VALID";
                            push @revalidate,
                                $con;   # needs to be revalidated after commit
                        } elsif ( $contype eq 'x' ) {
                            ...; # exclusion constraints are not yet implemented
                        } else {
                        }
                    }
                } else {
                    unshift @trans,
                        "DROP INDEX $quoted_nspname.$quoted_idxname";
                }

                wquery '', $current_cmd = $_ for (@trans);
            } else {
                eval {
                    query
                        "Index $quoted_nspname.$quoted_idxname has vanished. Dropping temporary",
                        qq{DROP INDEX $quoted_nspname.$tmp};
                    1;
                }
                    or warn "While dropping the index: $@";
            }
        };

        1;
    } or do {
        my $err = $@;
        chomp $err;
        $err = ">>$current_cmd<<\n$err";

        eval {
            query
                "Transaction for $quoted_nspname.$quoted_idxname failed. Dropping",
                qq{DROP INDEX $quoted_nspname.$tmp};
            1;
        }
            or warn "While dropping the index: $@";

        query '', $log_id[1]->[0]->[0], $err, <<'SQL' unless $opt_dryrun;
UPDATE reindex.log
   SET status='failed to rename index or recreate constraints: ' || $2,
       tm_taken=now()-tstmp
 WHERE id=$1
SQL

        die "$err";
    };

    unless ($opt_validate) {
        query '', $log_id[1]->[0]->[0], $err, <<'SQL' unless $opt_dryrun;
UPDATE reindex.log
   SET status='done: constraints not validated',
       tm_taken=now()-tstmp
 WHERE id=$1
SQL

        return 1;
    }

    wait_for_concurrent_tx;

    my @not_validated;
    for my $con (@revalidate) {
        my ($conname,   $quoted_conname, $nspname, $quoted_nspname,
            $contype,   $condef,         $rel,     $frel,
            $matchtype, $key,            $fkey
        ) = @$con;
        my $sql;

        my $join_cond
            = '('
            . join( ', ', map {"b.$_"} @$fkey ) . ')=('
            . join( ', ', map {"a.$_"} @$key ) . ')';
        my $match = (
            $matchtype eq 's'      # MATCH SIMPLE
            ? 'AND '
            : $matchtype eq 'f'    # MATCH FULL
            ? ' OR '
            : do {...}
        );                         # MATCH PARTIAL not yet implemented by PG
        $match = join( $match, map {"a.$_ IS NOT NULL"} @$key );

        @rc = wquery "  Validate FK constraint $quoted_conname on $rel",
            $rel, $conname, <<"SQL";
UPDATE pg_catalog.pg_constraint
   SET convalidated = NOT EXISTS(SELECT 1
                                   FROM ONLY $rel a
                                   LEFT JOIN ONLY $frel b ON $join_cond
                                  WHERE b.$fkey->[0] IS NULL      -- inner join failed
                                    AND ($match))
 WHERE conrelid=\$1::regclass::oid
   AND conname=\$2
RETURNING convalidated
SQL
        unless ($opt_dryrun) {
            lg '    ' . ( $rc[1]->[0]->[0] ? '' : 'NOT ' ) . "VALID\n";
            push @not_validated, $quoted_conname unless $rc[1]->[0]->[0];
        }
    }

    unless ($opt_dryrun) {
        if (@not_validated) {
            query '', $log_id[1]->[0]->[0],
                '[' . join( '], [', @not_validated ) . ']', <<'SQL';
UPDATE reindex.log
   SET status='failed: some constraints could not be validated: ',
       tm_taken=now()-tstmp
 WHERE id=$1
SQL
        } else {
            query '', $log_id[1]->[0]->[0], <<'SQL';
UPDATE reindex.log
   SET status='done',
       tm_taken=now()-tstmp
 WHERE id=$1
SQL
        }
    }

    return 1;
}

sub init {

}

sub prepare {

    $dbh = shift;
    my @opt_namespaces = @{ shift @_ };
    my @opt_tables     = @{ shift @_ };
    my @opt_indexes    = @{ shift @_ };

    my $qual;
    my @param;

    query '', 'SET client_min_messages TO WARNING';

    query '', 'CREATE SCHEMA IF NOT EXISTS reindex';

    query '', <<'SQL';
CREATE TABLE IF NOT EXISTS reindex.log(
  id BIGSERIAL PRIMARY KEY,
  tstmp TIMESTAMP,
  nspname NAME,
  tblname NAME,
  idxname NAME,
  sz_before BIGINT,
  sz_after BIGINT,
  status TEXT,
  tm_taken INTERVAL
)
SQL

    query '', <<'SQL';
CREATE UNLOGGED TABLE IF NOT EXISTS reindex.worklist(
  ord SERIAL,
  idx OID,
  status TEXT
)
SQL

    if (@opt_namespaces) {
        $qual .= "   AND n.nspname IN ("
            . join( ', ', ('?') x ( 0 + @opt_namespaces ) ) . ")\n";
        push @param, @opt_namespaces;
    } else {
        $qual .= <<'SQL';
   AND n.nspname !~ '^pg_'
   AND n.nspname <> 'information_schema'
   AND n.nspname <> 'reindex'
SQL
    }

    if (@opt_tables) {
        $qual .= "   AND EXISTS(SELECT 1
                FROM pg_catalog.pg_class xc
                JOIN pg_catalog.pg_index xi ON xc.oid=xi.indrelid
               WHERE xi.indexrelid=c.oid
                 AND xc.relname IN ("
            . join( ', ', ('?') x ( 0 + @opt_tables ) ) . "))\n";
        push @param, @opt_tables;
    }

    if (@opt_indexes) {
        $qual .= "   AND c.relname IN ("
            . join( ', ', ('?') x ( 0 + @opt_indexes ) ) . ")\n";
        push @param, @opt_indexes;
    }

    query '', 'TRUNCATE reindex.worklist';
    query '',
        q{SELECT pg_catalog.setval('reindex.worklist_ord_seq', 1, false)};

    query '', @param, <<'SQL'. $qual . <<'SQL';
INSERT INTO reindex.worklist(idx, status)
SELECT c.oid, 'planned'
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid
 WHERE c.relkind = 'i'
SQL
 ORDER BY n.nspname, c.relname
SQL
    return;
}

sub reindex {

    my ( $options, $opt_validate );

    ( $dbh, $options, $opt_dryrun ) = @_;

    $opt_validate     = $options->{Validate};
    $opt_throttle_on  = $options->{ThrottleOn} || 10000000;
    $opt_throttle_off = $options->{ThrottleOff} || 100000;

    while ( my @idx = next_index() ) {
        _reindex( @idx, $opt_validate );
        query '', $idx[0],
            q{UPDATE reindex.worklist SET status='done' WHERE idx=$1};
    }
}

=head1 AUTHOR

Torsten Förtsch, C<< <binary at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-pg-reindex at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Pg-Reindex>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Pg::Reindex


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Pg-Reindex>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Pg-Reindex>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Pg-Reindex>

=item * Search CPAN

L<http://search.cpan.org/dist/Pg-Reindex/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2015 Torsten Förtsch.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1;    # End of Pg::Reindex
