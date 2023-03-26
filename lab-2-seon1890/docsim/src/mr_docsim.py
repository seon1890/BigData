#! /usr/bin/env python

import re
import string
import pathlib

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


WORD_RE = re.compile(r"[\S]+")


class MRDocSim(MRJob):
    """
    A class to count word frequency in an input file.
    """

    def mapper_get_words(self, _, line):
        """

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) pairs
        """

        # This part extracts the name of the current document being processed
        current_file = jobconf_from_env("mapreduce.map.input.file")

        # Use this doc_name as the identifier of the document
        doc_name = pathlib.Path(current_file).stem

        for word in WORD_RE.findall(line):
            # strip any punctuation
            word = word.strip(string.punctuation)

            # enforce lowercase
            word = word.lower()

            # TODO: start implementing here!
            yield doc_name + "," + word, 1

    def reducer_get_counts(self, key, values):
        yield key, sum(values)

    def reduce_flip(self, key, value):
        doc, word = key.split(",")
        yield word, (doc, value)

    def reduce_pairs(self, _, values):
        files = []
        for val in values:
            files.append(val[0])

        files = list(set(files))
        pairs = [(a, b) for idx, a in enumerate(files) for b in files[idx + 1:]]

        for pair in pairs:
            p1v = 0
            p2v = 0
            for val in values:
                if val[0] == pair[0]:
                    p1v = val[1]
                if val[0] == pair[1]:
                    p2v = val[1]

            yield(pair[0] + "," + pair[1], min(p1v, p2v))

    def reduce_sum_pairs(self, key, values):
        yield key, sum(values)


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_words,
                reducer=self.reducer_get_counts
            ),
            MRStep(
                reducer=self.reduce_flip
            ),
            MRStep(
                reducer=self.reduce_pairs
            ),
            MRStep(
                reducer=self.reduce_sum_pairs
            )
        ]


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRDocSim.run()
