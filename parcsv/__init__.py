"""
Perform an operation on every item in a CSV file and store the results in an
output CSV file using multiple cores if available.
"""
import abc
import csv
import io
import logging
import pathlib
import signal
import sys
from contextlib import redirect_stdout, redirect_stderr
from multiprocessing import Pool
from typing import List, Dict, Any

# Exports
__all__ = ["Mapper", "process_files"]

from progress.counter import Counter

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger()
LOG.handlers = []
handler = logging.FileHandler("parcsv.log")
handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
LOG.addHandler(handler)


SUCCEEDED_FIELD = '_succeeded'


class Mapper(abc.ABC):
    """
    Maps rows from an input CSV file to rows in an output CSV file containing
    the fields specified in the __init__ call.
    """

    def __init__(self, fieldnames: List[str]) -> None:
        """
        Creates a Mapper.

        :param fieldnames: the field names to create in the output CSV file.
        The map function should return a dictionary that contains keys for each
        of these fieldnames."""
        self.fieldnames = fieldnames

    def map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        """
        Maps a single row from an input CSV file to a single row in an output
        CSV file containing the fields specified in the __init__ call.
        """
        out = io.StringIO()
        error = io.StringIO()
        with redirect_stdout(out), redirect_stderr(error):
            try:
                result = self._map(x)
            except Exception as ex:
                LOG.error("Item: %s - Exception: %s", x, ex)
                return {SUCCEEDED_FIELD: False}
            else:
                if error.getvalue():
                    LOG.error("Item: %s - Output: %s", x, error.getvalue())
                if out.getvalue():
                    LOG.info("Item: %s - Output: %s", x, out.getvalue())
                result[SUCCEEDED_FIELD] = True
                return result

    @abc.abstractmethod
    def _map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        """
        Override this method to map x to a new dict containing keys for each
        str in self.fieldnames.
        """


def _output_file_name(input_file_name: str, suffix: str) -> str:
    path = pathlib.Path(input_file_name)
    return str(path.with_name(path.stem + suffix).with_suffix(".csv"))


def process_file(
        mapper: Mapper, fname: str, pool: Pool, chunksize: int,
        file_name_suffix: str = "_mapped"
) -> None:
    """
    Creates a new CSV file by running each row in the input file named fname
    through the mapper using multiple processes.
    """
    ROW_FIELD = "_row"
    with open(fname, "r") as csvin, open(
        _output_file_name(fname, file_name_suffix), "w"
    ) as csvout, Counter(f"{fname}: ") as counter:
        fieldnames = [ROW_FIELD, SUCCEEDED_FIELD] + mapper.fieldnames
        reader = csv.DictReader(csvin)
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for i, row in enumerate(pool.imap(mapper.map, reader, chunksize=chunksize), 1):
            counter.next()
            try:
                row[ROW_FIELD] = i
                writer.writerow(row)
                csvout.flush()
            except ValueError as ex:
                LOG.error("error writing row: %s", str(ex))


def process_files(mapper: Mapper, fnames: List[str], chunksize: int = 1) -> None:
    """
    Applies the mapper to all of the files in fnames.
    """

    def initializer():
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    with Pool(initializer=initializer) as pool:
        try:
            for fname in fnames:
                process_file(mapper, fname, pool, chunksize=chunksize)
        except KeyboardInterrupt:
            sys.stderr.write("Cancelled by Ctrl-C!\n")
            pool.terminate()
            pool.join()
            sys.exit(130)
