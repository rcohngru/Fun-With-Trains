import pandas as pd
import numpy as np

class TrainData:
  
  def __init__(self):
    self.cities = load_cities()
    self.lines = load_lines()
    seld.station_lines = load_station_lines()
    self.stations = load_stations()
    self.systems = load_systems()


  def load_cities():
    '''
    Loads the cities.csv into a dataframe and cleans it.

    Returns: cities dataframe object
    '''
    cities = pd.read_csv('data/cities.csv')

    #converting coordinates to lat and long
    point_to_coords(cities, 'coords')
    #dropping unnecessary columns
    cities = cities.drop(columns=['country_state'])
    return cities

  def load_lines():
    lines = pd.read_csv('data/lines.csv')
    return lines

  def load_station_lines():
    # need to convert date times to datetime object
    # can index into by doing df['colname'].dt.[year|month|day|hour|minute|sec]
    station_lines = pd.read_csv('data/station_lines.csv')
    station_lines['created_at'] = pd.to_datetime(station_lines['created_at'])
    station_lines['updated_at'] = pd.to_datetime(station_lines['updated_at'])
    return station_lines

  def load_stations():
    stations = pd.read_csv('data/stations.csv')
    stations=stations.dropna(subset=['name'])
    stations=stations[stations.opening>0]
    stations=stations[stations.opening<=2030]
    point_to_coords(stations, 'geometry')

    clean_bad_dates(stations, 'buildstart', 'opening', pd.isnull)

    # a number of stations have -2, -1, 0, or 201 for their buildstart dates. I change them to
    # be the same year as opening
    f = lambda x: True if x <= 201 else False
    clean_bad_dates(stations, 'buildstart', 'opening', f)


    # a number of stations have 999999 or 999998 for their buildstart dates. I change them to
    # be the same year as opening
    f = lambda x: True if x > 2030 else False
    clean_bad_dates(stations, 'buildstart', 'opening', f)
    stations['buildstart'] = stations['buildstart'].astype('int64')

    indices = stations[stations['closure'].isnull()].index
    stations.loc[indices, 'closure'] = 99999
    indices = stations[stations['closure'] > 2030].index
    stations.loc[indices, 'closure'] = 99999

    return stations

  def load_systems():
    systems = pd.read_csv('data/systems.csv')
    return systems

  def load_tracks():
    tracks = pd.read_csv('data/tracks.csv')
    clean_bad_dates(tracks, 'buildstart', 'opening', pd.isnull)
    
    f = lambda x: True if x <= 0 else False
    clean_bad_dates(tracks, 'buildstart', 'opening', f)
    
    f = lambda x: True if x > 2030 and x < 999998 else False
    clean_bad_dates(tracks, 'buildstart', 'opening', f)
    
    tracks.at[4090, ['buildstart', 'opening']] = 2008
    
    #Need to figure out if dropping tracks where buildstart == 999999 or 0
    
    clean_bad_dates(tracks, 'opening', 'buildstart', pd.isnull)
    
    f = lambda x: True if x == 0 else False
    clean_bad_dates(tracks, 'opening', 'buildstart', f)
    
    f = lambda x: True if x > 2030 else False
    clean_bad_dates(tracks, 'opening', 'buildstart', f)
    
    indices = tracks[tracks['closure'].isnull()].index
    tracks.loc[indices, 'closure'] = 99999
    indices = tracks[tracks['closure'] > 2030].index
    tracks.loc[indices, 'closure'] = 99999
    
    tracks['buildstart'] = tracks['buildstart'].astype('int64')
    tracks['closure'] = tracks['closure'].astype('int64')

    return tracks


  def clean_bad_dates(df, col_name, replace_name, func):
    indices = df[df[col_name].apply(func)].index
    df.loc[indices, col_name] = df.loc[indices, replace_name]

  def point_to_coords(df, col_name):
    '''
    Parses a dataframe column for latitude and longitude coordinates

    Converts a dataframe column of form POINT(<long> <lat>) into two separate columns

    Parameters:
        -df : a pandas dataframe
        -col_name : the name of the dataframe column to be parsed
    '''
    df['lat'] = df[col_name].apply(lambda x: x.split('POINT(')[1].split()[0])
    df['long'] = df[col_name].apply(lambda x: x.split('POINT(')[1].split()[1].split(')')[0])
