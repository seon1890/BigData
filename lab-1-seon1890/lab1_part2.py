#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python lab1_part2.py music_small.db

import sys
import sqlite3
import timeit


# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]


# The query to be optimized is given here
MY_QUERY = """SELECT artist.*, count(distinct track.id) as tracks
                FROM track INNER JOIN artist
                ON track.artist_id = artist.id
                GROUP BY artist_id
                HAVING tracks > 10"""

NUM_ITERATIONS = 100

def run_my_query(conn):
    for row in conn.execute(MY_QUERY):
        pass

 
def run_general_query(conn, query):
    for row in conn.execute(query):
        pass


# We connect to the database using
with sqlite3.connect(db_file) as conn:
    # We use a "cursor" to mark our place in the database.
    cursor = conn.cursor()

    # We could use multiple cursors to keep track of multiple
    # queries simultaneously.

    orig_time = timeit.repeat('run_my_query(conn)', globals=globals(), number=NUM_ITERATIONS)
    print("Before optimization:")

    print(f'Mean time: {sum(orig_time)/NUM_ITERATIONS:.3f} [seconds/query]')
    print(f'Best time: {min(orig_time)/NUM_ITERATIONS:.3f} [seconds/query]')

    # MAKE YOUR MODIFICATIONS TO THE DATABASE HERE
    run_general_query(conn, "CREATE INDEX t_artist_id on track(artist_id)")
    run_general_query(conn, "CREATE INDEX a_artist_id on artist(id)")
    #run_general_query(conn, "CREATE INDEX t_track_id on track(id)")

    new_time = timeit.repeat('run_my_query(conn)', globals=globals(), number=NUM_ITERATIONS)
    print("After optimization:")

    print(f'Mean time: {sum(new_time)/NUM_ITERATIONS:.3f} [seconds/query]')
    print(f'Best time   : {min(new_time)/NUM_ITERATIONS:.3f} [seconds/query]')
