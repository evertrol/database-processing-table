import sys
import sqlite3
from datetime import datetime, timedelta
from collections import namedtuple


Obs = namedtuple('Observation', ['filter', 'exptime', 'target', 'repeats'])

READOUTTIME = 12
CONFIG = {
    'telescopes': {
        'GOTO1': {
            'cameras': {
                'UT1': {
                    'instruments': ['CCD1'],
                    'filters': ['L', 'B', 'V', 'R', 'I']
                },
                'UT2': {
                    'instruments': ['CCD2'],
                    'filters': ['L', 'B', 'V', 'R', 'I']
                },
                'UT3': {
                    'instruments': ['CCD3'],
                    'filters': ['L', 'B', 'V', 'R', 'I']
                },
                'UT4': {
                    'instruments': ['CCD4'],
                    'filters': ['L', 'B', 'V', 'R', 'I']
                },
            },
        },
        'GOTO2': {
            'cameras': {
                'UT1': {
                    'instruments': ['CCD1'],
                    'filters': ['L', 'G', 'R', 'I']
                },
                'UT2': {
                    'instruments': ['CCD2'],
                    'filters': ['L', 'G', 'R', 'I']
                },
                'UT4': {
                    'instruments': ['CCD4'],
                    'filters': ['L', 'G', 'R', 'I']
                }
            }
        }
    }
}


def setup_schedule():
    schedule = []
    for filt, repeats in zip('LBVGR', (3, 1, 1, 1, 1)):
        schedule.append(
            Obs(filter=filt, exptime=180, target='GRB', repeats=repeats)
        )
    schedule.append(
        Obs(filter='R', exptime=15, target='Ceph', repeats=20)
    )
    for filt, repeats in zip('LBGVR', (6, 2, 2, 2, 2)):
        schedule.append(
            Obs(filter=filt, exptime=120, target='GW123456', repeats=repeats)
        )
    for field in [23, 44, 56, 79]:
        name = "Field{}".format(field)
        schedule.extend([
            Obs(filter='L', exptime=120, target=name, repeats=3),
            Obs(filter='B', exptime=120, target=name, repeats=1),
            Obs(filter='V', exptime=120, target=name, repeats=1),
            Obs(filter='G', exptime=120, target=name, repeats=1),
            Obs(filter='R', exptime=120, target=name, repeats=1),
        ])
    schedule.append(
        Obs(filter='L', exptime=80, target='And123', repeats=12),
    )
    schedule.extend([
        Obs(filter='L', exptime=120, target='Peg54', repeats=3),
        Obs(filter='L', exptime=80, target='Cas54', repeats=4),
    ])
    for field in [88, 123, 135, 77]:
        name = "Field{}".format(field)
        # Use three sequences of just two L filter observations;
        # combining these sequences should *not* cross the border of
        # the individual sequences, that is, the stacks should be 2, 2
        # and 2 images, not 3 and 3.
        for filt, repeats in zip('LLLBVGR', (2, 2, 2, 1, 1, 1, 1)):
            schedule.append(
                Obs(filter=filt, exptime=120, target=name, repeats=repeats)
            )
    print(len(schedule))
    return schedule


def setup_database(filename):
    conn = sqlite3.connect(filename)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE observations (
id INT PRIMARY KEY,
telescope TEXT,
camera TEXT,
instrument TEXT,
filter TEXT,
imagetype TEXT,
target TEXT,
exptime REAL,
obsdate TEXT,
iobs INT,
nobs INT,
stage INT DEFAULT 0,
status TEXT DEFAULT "unknown",
"set" INT DEFAULT 0
)""")
    conn.commit()
    cursor.close()
    return conn


def simulate_schedule(schedule, dbconn, startdate, clouddate):
    cursor = dbconn.cursor()
    imagetype = "SCIENCE"
    for telescope in ['GOTO1', 'GOTO2']:
        for camname, camconfig in CONFIG['telescopes'][telescope]['cameras'].items():
            filters = camconfig['filters']
            obsdate = startdate
            for instrument in camconfig['instruments']:
                for obs in schedule:
                    if obs.filter not in filters:
                        continue
                    for iobs in range(obs.repeats):
                        cursor.execute("""INSERT INTO observations
(telescope, camera, instrument, filter, imagetype, target, exptime, obsdate, iobs, nobs) VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (telescope, camname, instrument, obs.filter, imagetype, obs.target, obs.exptime, obsdate,
      iobs+1, obs.repeats))
                        obsdate += timedelta(0, obs.exptime+READOUTTIME, 0)
                        if clouddate and obsdate > clouddate:
                            # Abort current sequence, and wait until it clears
                            obsdate += timedelta(0, 5400, 0)
                            clouddate = False
                            # Now repeat the failed sequence
                            for iobs in range(obs.repeats):
                                cursor.execute("""INSERT INTO observations
(telescope, camera, instrument, filter, imagetype, target, exptime, obsdate, iobs, nobs) VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (telescope, camname, instrument, obs.filter, imagetype, obs.target, obs.exptime, obsdate,
      iobs+1, obs.repeats))
                                obsdate += timedelta(0, obs.exptime+READOUTTIME, 0)
                            break
    dbconn.commit()
    cursor.close()


def main():
    filename = sys.argv[1]
    startdate = datetime(2018, 9, 9, 19, 35, 30)
    clouddate = datetime(startdate.year, startdate.month, startdate.day, 21, 13, 0)
    schedule = setup_schedule()
    dbconn = setup_database(filename)
    simulate_schedule(schedule, dbconn, startdate, clouddate)
    dbconn.close()


if __name__ == '__main__':
    main()
