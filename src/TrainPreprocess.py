import pandas as pd
import numpy as np

class TrainData():
  
  def __init__(self):
    print('here')
    self.cities = self.load_cities()
    self.lines = self.load_lines()
    self.station_lines = self.load_station_lines()
    self.stations = self.load_stations()
    self.systems = self.load_systems()
    self.tracks = self.load_tracks()
    self.track_lines = self.load_tracks()


  def load_cities(self):
    '''
    Loads the cities.csv into a dataframe and cleans it.

    Returns: cities dataframe object
    '''
    cities = pd.read_csv('data/cities.csv')

    #converting coordinates to lat and long
    self.point_to_coords(cities, 'coords')
    #dropping unnecessary columns
    cities = cities.drop(columns=['country_state'])
    return cities

  def load_lines(self):
    lines = pd.read_csv('data/lines.csv')
    return lines

  def load_station_lines(self):
    # need to convert date times to datetime object
    # can index into by doing df['colname'].dt.[year|month|day|hour|minute|sec]
    station_lines = pd.read_csv('data/station_lines.csv')
    station_lines['created_at'] = pd.to_datetime(station_lines['created_at'])
    station_lines['updated_at'] = pd.to_datetime(station_lines['updated_at'])
    return station_lines

  def load_stations(self):
    stations = pd.read_csv('data/stations.csv')
    stations=stations.dropna(subset=['name'])
    stations=stations[stations.opening>0]
    stations=stations[stations.opening<=2030]
    self.point_to_coords(stations, 'geometry')

    self.clean_bad_dates(stations, 'buildstart', 'opening', pd.isnull)

    # a number of stations have -2, -1, 0, or 201 for their buildstart dates. I change them to
    # be the same year as opening
    f = lambda x: True if x <= 201 else False
    self.clean_bad_dates(stations, 'buildstart', 'opening', f)


    # a number of stations have 999999 or 999998 for their buildstart dates. I change them to
    # be the same year as opening
    f = lambda x: True if x > 2030 else False
    self.clean_bad_dates(stations, 'buildstart', 'opening', f)
    stations['buildstart'] = stations['buildstart'].astype('int64')

    indices = stations[stations['closure'].isnull()].index
    stations.loc[indices, 'closure'] = 99999
    indices = stations[stations['closure'] > 2030].index
    stations.loc[indices, 'closure'] = 99999

    return stations

  def load_systems(self):
    systems = pd.read_csv('data/systems.csv')
    return systems

  def load_tracks(self):
    tracks = pd.read_csv('data/tracks.csv')
    self.clean_bad_dates(tracks, 'buildstart', 'opening', pd.isnull)
    self.clean_bad_dates(tracks, 'opening', 'buildstart', pd.isnull)
    
    f = lambda x: True if x <= 0 else False
    self.clean_bad_dates(tracks, 'buildstart', 'opening', f)
    
    f = lambda x: True if x > 2030 and x <= 999999 else False
    self.clean_bad_dates(tracks, 'buildstart', 'opening', f)
    
    tracks.at[4090, ['buildstart', 'opening']] = 2008
    
    #Need to figure out if dropping tracks where buildstart == 999999 or 0
    
    
    
    f = lambda x: True if x == 0 else False
    self.clean_bad_dates(tracks, 'opening', 'buildstart', f)
    
    f = lambda x: True if x > 2030 else False
    self.clean_bad_dates(tracks, 'opening', 'buildstart', f)
    
    indices = tracks[tracks['closure'].isnull()].index
    tracks.loc[indices, 'closure'] = 99999
    indices = tracks[tracks['closure'] > 2030].index
    tracks.loc[indices, 'closure'] = 99999
    
    tracks['buildstart'] = tracks['buildstart'].astype('int64')
    tracks['closure'] = tracks['closure'].astype('int64')
    
    indices = tracks[tracks['buildstart'] == 0].index
    tracks = tracks.drop(index=indices)
    indices = tracks[tracks['buildstart'] == 999999].index
    tracks = tracks.drop(index=indices)
    self.linestring_to_coords(tracks, 'geometry')

    return tracks

  def load_track_lines(self):
    # need to convert date times to datetime object
    # can index into by doing df['colname'].dt.[year|month|day|hour|minute|sec]
    track_lines = pd.read_csv('data/track_lines.csv')
    track_lines['created_at'] = pd.to_datetime(track_lines['created_at'])
    track_lines['updated_at'] = pd.to_datetime(track_lines['updated_at'])

    return track_lines

  def clean_bad_dates(self, df, col_name, replace_name, func):
    indices = df[df[col_name].apply(func)].index
    df.loc[indices, col_name] = df.loc[indices, replace_name]

  def point_to_coords(self, df, col_name):
    '''
    Parses a dataframe column for latitude and longitude coordinates

    Converts a dataframe column of form POINT(<long> <lat>) into two separate columns

    Parameters:
        -df : a pandas dataframe
        -col_name : the name of the dataframe column to be parsed
    '''
    df['long'] = df[col_name].apply(lambda x: x.split('POINT(')[1].split()[0])
    df['lat'] = df[col_name].apply(lambda x: x.split('POINT(')[1].split()[1].split(')')[0])


  def linestring_to_coords(self, df, col_name):
    f = lambda x: x.lstrip('LINESTRING(').rstrip(')').split(',')
    lats = lambda x: [y.split()[1] for y in x]
    longs = lambda x: [y.split()[0] for y in x]
    df['lats'] = df['geometry'].apply(f).apply(lats)
    df['longs'] = df['geometry'].apply(f).apply(longs)



if __name__ == "__main__":
   pass
