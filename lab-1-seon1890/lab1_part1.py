#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python lab1_part1.py music_small.db

import sys
import sqlite3


# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]

# We connect to the database using 
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    year = (1990,)
    for row in conn.execute('SELECT count(*) FROM artist WHERE artist_active_year_begin=?', year):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print('Tracks from {}: {}'.format(year[0], row[0]))
        
        # The [0] bits here tell us to pull the first column out of the 'year' tuple
        # and query results, respectively.

    # ADD YOUR CODE STARTING HERE
    
    # Question 1
    print('Question 1:')
with sqlite3.connect(db_file) as conn:
    for row in conn.execute("SELECT id, artist_name from artist where lower(artist_name) like '%green%'"):
        print(row)
    print('---')
    
    # Question 2
    print('Question 2:')
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    for row in conn.execute('SELECT DISTINCT album_type FROM album'):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print(row)
    
    print('---')
    
    # Question 3
    print('Question 3:')
    
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    for row in conn.execute('SELECT id, album_title from album order by album_listens DESC limit 1'):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print(row)
    
    print('---')
    
    # Question 4
    print('Question 4:')
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    for row in conn.execute('SELECT COUNT(*) FROM artist WHERE artist_wikipedia_page IS NOT NULL'):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print(row)
    print('---')
              
    # Question 5
    print('Question 5:')
with sqlite3.connect(db_file) as conn:
    for row in conn.execute('SELECT count(*) as langc, track_language_code from  track where track_language_code is not null  group by track_language_code HAVING COUNT(*) >= 3'):
        print(row)
    
    
    print('---')
    
    # Question 6
    print('Question 6:')
with sqlite3.connect(db_file) as conn:
    for row in conn.execute('SELECT count(track.track_title) from track INNER JOIN artist on track.artist_id = artist.id WHERE artist_latitude < 0'):
        print('The number of tracks from artists in the Southern hemisphere:', row[0])
    
    print('---')
    
    # Question 7
    print('Question 7:')
    
    for row in conn.execute('SELECT count(album.album_title = artist.artist_name) FROM track INNER JOIN album on track.album_id = album.id INNER JOIN artist on track.artist_id = artist.id '):
        print('Albums where the album title is identical to the artist name:', row[0])
    
    print('---')
    
    # Question 8
    print('Question 8:')
    
    for row in conn.execute('SELECT DISTINCT count(artist.artist_name), artist.artist_name, artist.id FROM track INNER JOIN album on track.album_id = album.id INNER JOIN artist on track.artist_id = artist.id'):
        print(row)
    
    print('---')
