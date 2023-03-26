# Lab 1 results

Your name: 

## Part 1
Paste the results of your queries for each question given in the README below:

1. 
(262, 'Shawn Greenlee')
(6409, 'The Green Kingdom')
(18361, 'Veronica Green')
(20122, 'Georgia & August Greenberg')


2.
('Album',)
('Live Performance',)
('Single Tracks',)
('Radio Program',)
(None,)


3.
(13010, 'Entries')

4.
(244,)

5.
(4, 'de')
(1191, 'en')
(12, 'es')
(16, 'fr')
(3, 'it')
(5, 'pt')
(4, 'ru')
(3, 'sr')
(3, 'tr')

6.
The number of tracks from artists in the Southern hemisphere: 200

7.
Albums where the album title is identical to the artist name: 8756

8.
(8756, 'Nicky Cook', 4)

## Part 2

- Execution time before optimization:
- Execution time after optimization:

Before optimization:
Mean time: 0.063 [seconds/query]
Best time: 0.012 [seconds/query]
After optimization:
Mean time: 0.022 [seconds/query]
Best time   : 0.004 [seconds/query]

- Briefly describe how you optimized for this query:

By adding indexes on the columns used in the query, the database does not have to do a scan on the tables. I found out that the database was doing a scan by running a 'EXPLAIN QUERY PLAN' which highlighted the full table scan.

- Did you try anything other approaches?  How did they compare to your final answer?

I tried adding an additional index on track.id, but realized that this had no use since the 'EXPLAIN QUERY PLAN' showed that the database was not affected by it.