import argparse
import math
import re
import sys
from typing import Generator
from collections import namedtuple
from datetime import datetime

import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument('file', help='Alphamentis Track Aero System dashboard run log file')


class InvalidFileFormatError(Exception):
    pass


SpeedSensorRecord = namedtuple('SpeedSensorRecord',
                               ['timestamp',
                                'value',
                                'elapsed_time',
                                'count',
                                'circumference'])

PowerSensorRecord = namedtuple('PowerSensorRecord',
                               ['timestamp',
                                'value',
                                'event_count',
                                'elapsed_time'])


class DashboardRunLog():
    """
    Garmin Track Aero System dashboardRun log file parser.
    """
    def __init__(self, filename: str):
        self.filename = filename

    @property
    def settings(self) -> dict:
        with open(self.filename) as f:
            data = {}
            while True:
                l = f.readline()
                if not l or re.search(r'^#\tTime and Date', l):
                    break
            while True:
                l = f.readline()
                if not l or re.search(r'^###', l):
                    break
                m = re.search(r'^#\t\t(?P<label>Start Date|Start Time|Run Number):?\t(?P<value>[^\t]+)\n$', l)
                if not m:
                    continue
                data[m.group('label').lower()] = m.group('value')

            while True:
                l = f.readline()
                if not l or re.search(r'^#\tRider and Device Data', l):
                    break
            while True:
                l = f.readline()
                if not l or re.search(r'^###', l):
                    break
                m = re.search(r'^#\t\t(?P<label>[A-Z_]+)\t(?P<value>[^\t]+)(?:\t(?P<param1>[^\t]+)\t(?P<param2>[^\t]+))?\n$', l)
                if not m:
                    continue
                if m.group('param1'):
                    data[m.group('label').lower()] = (m.group('value'), m.group('param1'), m.group('param2'))
                else:
                    data[m.group('label').lower()] = m.group('value')
            return data

    def speed(self) -> Generator[SpeedSensorRecord, None, None]:
        """
        Generator to get the first value of the ANT+ Bicycle Speed sensor.

        ANT+ will continue to transmit the same value while the sensor
        is updated, so only updated values are retrieved
        """
        settings = self.settings
        _, sensor_id = settings['speed'][0].split('_')
        circumference = float(settings['speed'][1])

        pattern = fr'^[A-Z0-9]+_{sensor_id}\t(?P<timestamp>\d+\.\d+)\tS\t0\t(?P<timer>\d+)\t(?P<count>\d+)\t\t'
        next_pattern = fr'^SPEED\t[A-Z0-9]+_{sensor_id}\t(?P<timestamp>\d+\.\d+)\t(?P<speed>\d+\.\d+)\n'
        with open(self.filename) as f:
            while True:
                l = f.readline()
                if not l:
                    break
                # find first entry
                m = re.search(pattern, l)
                if not m:
                    continue
                rev_count = int(m.group('count'))
                meas_time = int(m.group('timer'))
                if meas_time == 0:
                    continue
                speed = round(circumference * rev_count * 1024 / meas_time, 6) if meas_time > 0 else 0.0

                # find data entry
                while True:
                    l = f.readline()
                    if not l:
                        break
                    m = re.search(next_pattern, l)
                    if not m:
                        continue

                    result = SpeedSensorRecord(datetime.fromtimestamp(float(m.group('timestamp'))),
                                               float(m.group('speed')),
                                               round(meas_time / 1024, 6),
                                               rev_count,
                                               circumference)
                    break

                # seek next copy
                while True:
                    l = f.readline()
                    if not l:
                        break
                    if re.search(pattern, l):
                        break
                yield result

    def power(self) -> Generator[PowerSensorRecord, None, None]:
        """
        Generator to get the first value of the ANT+ bicycle Power sensor.
        """
        settings = self.settings
        _, sensor_id = settings['power'][0].split('_')
        offset = int(settings['power'][1])

        pattern = fr'^[A-Z0-9]+_{sensor_id}\t(?P<timestamp>\d+\.\d+)\tS\t(?P<event_count>\d+)\t(?P<elapsed_time>\d+)\t(?P<torque_ticks>\d+)\t(?P<slope>\d+)\t'
        next_pattern = fr'^POWER\t[A-Z0-9]+_{sensor_id}\t(?P<timestamp>\d+\.\d+)\t(?P<power>\d+\.\d+)\n'
        with open(self.filename) as f:
            while True:
                l = f.readline()
                if not l:
                    break
                # find first entry
                m = re.search(pattern, l)
                if not m or m.group('event_count') == '0':
                    continue

                event_count = int(m.group('event_count'))
                elapsed_time = int(m.group('elapsed_time')) * 0.0005
                # find power entry
                while True:
                    l = f.readline()
                    if not l:
                        break
                    m = re.search(next_pattern, l)
                    if not m:
                        continue

                    timestamp = datetime.fromtimestamp(float(m.group('timestamp')))
                    power = float(m.group('power'))
                    result = PowerSensorRecord(timestamp,
                                           power,
                                           event_count,
                                           round(elapsed_time, 6))
                    break
                # skip to next copy entry
                while True:
                    l = f.readline()
                    if not l:
                        break
                    if re.search(pattern, l):
                        break
                yield result

    def cg_speed(self) -> Generator[SpeedSensorRecord, None, None]:
        """
        Generator to get the latest value of the TAS center of gravity speed.

        TAS returns the Center of Gravity speed(Vcg) based on the ANT+ Bicycle Speed
        sensor value, corrected by estimating the lean angle.
        TAS returns several different center of gravity speeds with the same timestamp.
        This implementation uses the **last** value.

        NOTE: もしかしたら平均値を返すべきか？
        """
        settings = self.settings
        _, sensor_id = settings['speed'][0].split('_')
        circumference = float(settings['speed'][1])

        pattern = fr'^[A-Z0-9]+_{sensor_id}\t(?P<timestamp>\d+\.\d+)\tS\t0\t(?P<timer>\d+)\t(?P<count>\d+)\t\t'
        next_pattern = fr'^CG_SPEED_[A-Z0-9]+_{sensor_id}\t(?P<timestamp>\d+\.\d+)\t(?P<speed>\d+\.\d+)\n'
        latest = None
        with open(self.filename) as f:
            while True:
                l = f.readline()
                if not l:
                    break
                # find first entry
                m = re.search(pattern, l)
                if not m:
                    continue
                rev_count = int(m.group('count'))
                meas_time = int(m.group('timer'))
                if meas_time == 0:
                    continue
                # find latest data entry
                result = None
                while True:
                    l = f.readline()
                    if not l:
                        break
                    if result and re.search(pattern, l):
                        break
                    m = re.search(next_pattern, l)
                    if not m:
                        continue
                    result = SpeedSensorRecord(datetime.fromtimestamp(float(m.group('timestamp'))),
                                               float(m.group('speed')),
                                               round(meas_time / 1024, 6),
                                               rev_count,
                                               circumference)
                if result is None:
                    break
                yield result


    def to_df(self) -> pd.DataFrame:
        speed = list(self.speed())
        v = pd.DataFrame({'sensor': ['speed' for x in speed],
                          'value': [x.value for x in speed],
                          'elapsed_time': [x.elapsed_time for x in speed]},
                         index=[x.timestamp.strftime('%F %T.%f') for x in speed])
        cg_speed = list(self.cg_speed())
        vcg = pd.DataFrame({'sensor': ['cg_speed' for x in cg_speed],
                            'value': [x.value for x in cg_speed],
                            'elapsed_time': [x.elapsed_time for x in cg_speed]},
                           index=[x.timestamp.strftime('%F %T.%f') for x in cg_speed])
        power = list(self.power())
        w = pd.DataFrame({'sensor': ['power' for x in power],
                          'value': [x.value for x in power],
                          'elapsed_time': [x.elapsed_time for x in power]},
                         index=[x.timestamp.strftime('%F %T.%f') for x in power])
        df = pd.concat([v, vcg, w])
        df.index = pd.to_datetime(df.index)
        return df


def main():
    args = parser.parse_args()
    log = DashboardRunLog(args.file)
    speed, power = log.speed(), log.power()

    records = list(speed) + list(power)
    records = sorted(records, key=lambda x: x.timestamp)
    csv = [f'{x.timestamp.strftime("%F %T.%f")},{"power" if isinstance(x, PowerSensorRecord) else "speed"},{x.value:.6f},{x.elapsed_time:.6f}' for x in records]
    print("timestamp,sensor,value,elapsed_time")
    print("\n".join(csv))
    print(log.settings, file=sys.stderr)


if __name__ == '__main__':
    main()

