import sys
from datetime import datetime, timedelta
import sqlite3
import numpy as np
import pandas as pd
import strictyaml as yml


NSTACK = 4
MAXSEQ = 12
MAXTDELTA = timedelta(0, 1800, 0)


def set_database_state(dbconn):
    cursor = dbconn.cursor()
    cursor.execute("""\
UPDATE observations SET stage = 1, status = 'processing'
  WHERE telescope = 'GOTO1' AND camera = 'UT1' AND instrument = 'CCD1' AND
    obsdate < '2018-09-09 23:45:00'

""")
    cursor.execute("""\
UPDATE observations SET stage = 1, status = 'completed'
  WHERE telescope = 'GOTO1' AND camera = 'UT1' AND instrument = 'CCD1' AND
    obsdate < '2018-09-09 23:30:00'

""")
    cursor.execute("""\
UPDATE observations SET stage = 2, status = 'processing'
  WHERE telescope = 'GOTO1' AND camera = 'UT1' AND instrument = 'CCD1' AND
    obsdate < '2018-09-09 23:15:00'
""")
    cursor.execute("""\
UPDATE observations SET stage = 2, status = 'completed'
  WHERE telescope = 'GOTO1' AND camera = 'UT1' AND instrument = 'CCD1' AND
    obsdate < '2018-09-09 23:05:00'
""")
    cursor.execute("""\
UPDATE observations SET stage = 3, status = 'processing'
  WHERE telescope = 'GOTO1' AND camera = 'UT1' AND instrument = 'CCD1' AND
    obsdate BETWEEN '2018-09-09 22:56:00' AND '2018-09-09 23:02:00'
""")
    cursor.execute("""\
UPDATE observations SET stage = 3, status = 'completed'
  WHERE telescope = 'GOTO1' AND camera = 'UT1' AND instrument = 'CCD1' AND
    obsdate < '2018-09-09 22:50:00'
""")
    dbconn.commit()


def read(dbconn, indep_cols, fromdate, todate):

    args = []
    sql = "select * from observations"

    sqldates = []
    if fromdate:
        args.append(fromdate)
        sqldates.append("""
obsdate >= (
    select obsdate from observations
        where status = 'completed' and stage = 3
        and obsdate >= ?
        order by obsdate
        limit 1
    )
""")
    if todate:
        sqldates.append("""
obsdate <= (
    select obsdate from observations
        where status = 'completed' and stage = 3
        and obsdate <= ?
        order by desc obsdate
        limit 1
    )
""")
    if sqldates:
        print(args)
        df = pd.read_sql("""select obsdate from observations
        where status = 'completed' and stage = 3
        and obsdate >= ?
        order by obsdate
        limit 1""", dbconn, params=args)
        print(df.head())
        sql += " WHERE " + " AND ".join(sqldates)

    df = pd.read_sql(sql, dbconn, params=args)
    df['obsdate'] = pd.to_datetime(df['obsdate'])
    if indep_cols:
        df = df.set_index(indep_cols)
    df.sort_index(inplace=True)
    return df


def process_single(df):
    # Silence a Pandas warning. This occurs at the dfsel[...].iloc[i] stage,
    # and can be ignored
    pd.options.mode.chained_assignment = None

    columns = ['imagetype', 'target', 'obsdate', 'filter', 'exptime',
               'iobs', 'nobs', 'stage', 'status', 'set']
    print(df.index)
    for index in df.index.unique():
        print(index)
        dfsel = df.loc[index, columns]
        # Ensure it's ordered by date
        dfsel.sort_values('obsdate', inplace=True)
        dfshift = dfsel.shift(1)
        # We compare exptime, a float. If, however, the float was set explicitly,
        # and didn't come about from a calculation, the comparison will be exact,
        # and we're good. Otherwise, things become quite a bit more complicated.
        selcols = ['imagetype', 'target', 'filter', 'exptime']

        # Compare the shifted frame; this results in a four column frame with booleans.
        # Summing these booleans across the columns creates a non-zero value
        # for every change across these columns.
        # We combine that with other requirements, such as iobs == nobs and
        # the maximum allowed time interval.
        # Note that 'set' is only unique within a single set of a multi-index
        dfsel['set'] = (((dfsel[selcols] != dfshift[selcols]).sum(axis=1) > 0) |
                        (dfshift['iobs'] == dfshift['nobs']) |
                        ((dfsel['obsdate'] - dfshift['obsdate']) > MAXTDELTA)).cumsum()

        # Ignore the last set: it may be incomplete, and data may still be incoming
        for name, group in list(dfsel.groupby(by='set'))[:-1]:
            selindex = (dfsel['set'] == name)

            if len(group) == 1 or len(group) > MAXSEQ:
                # Single frames or large sequences are passed through with a no-op
                dfsel.loc[selindex, 'stage'] = 4
                dfsel.loc[selindex, 'status'] = 'notprocessed'
                continue
            # Iterate in chunks of NSTACK
            for pos in range(0, len(group), NSTACK):
                subset = group[pos:pos+NSTACK]
                ready = (np.all(subset['stage'] == 3) &
                         np.all(subset['status'].isin(['completed', 'notprocessed'])))
                if ready:
                    # Get the actual indices in dfsel for this subset
                    i = np.where(selindex)[0][pos:pos+NSTACK]
                    dfsel['stage'].iloc[i] = 4
                    dfsel['status'].iloc[i] = 'starting'
                    # Make the 'set' id unique for this subset
                    dfsel['set'].iloc[i] = dfsel['set'].max() + 1

        break
    # Assign back to the original dataframe
    df.loc[index, columns] = dfsel
    return df


def process(dbconn, indep_cols):
    fromdate, todate = datetime(2018, 9, 9, 12, 0, 0), None
    if fromdate:
        if isinstance(fromdate, timedelta):
            fromdate = datetime.now() - abs(fromdate)
    if todate:
        if isinstance(todate, timedelta):
            todate = datetime.now() + todate
    
    columns = read(dbconn, indep_cols, fromdate, todate)
    for colset in columns:
        df = read_single(dbconn, colset, fromdate, todate)

    df = process_single(df)


def read_conf(filename):
    with open(filename) as fp:
        config = yml.load(fp.read()).data
    return config


def main():
    config = read_conf(sys.argv[1])
    indep_cols = config['independent']
    dbconn = sqlite3.connect(sys.argv[2])
    set_database_state(dbconn)

    process(dbconn, indep_cols)
    print("= = = = =")
    print(df.loc[('GOTO1', 'UT1', 'CCD1')].head(15))
    print("= = = = =")
    print(df.loc[('GOTO1', 'UT1', 'CCD1')].iloc[25:35])
    print("= = = = =")
    print(df.loc[('GOTO1', 'UT1', 'CCD1')].iloc[50:65])



if __name__ == '__main__':
    main()
