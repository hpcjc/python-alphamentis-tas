# Garmin Track Aero System dashboard log file parser

This library obtains sensor data and setting values from
the Garmin Track Aero System('TAS') dashboard log file.

## USAGE

```
from alphamentis.tas import DashboardRunLog

tas = DashboardRunLog(path_to_file)
print(tas.settings)
for x in tas.speed():
    print(s)
```

## INSTALL

```
pip install git+https://github.com/hpcjc/python-alphamentis-tas
```
