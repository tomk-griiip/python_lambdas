
# -------------- static method's to help building the beans objects ---------------#

"""
return the lap distance
"""


def get_lap_distance(laps: []):
    return max(laps, key=lambda item: item.distance).distance


"""
return the lap time 
"""


def get_lap_time(laps: []):
    return max(laps, key=lambda item: item.lapTime).lapTime


"""
return the lap finish long 
"""


def get_lap_long(laps: []):
    return max(laps, key=lambda item: item.gpsLong).gpsLong


"""
return the lap finish lat 
"""


def get_lap_lat(laps: []):
    return max(laps, key=lambda item: item.gpsLat).gpsLat
