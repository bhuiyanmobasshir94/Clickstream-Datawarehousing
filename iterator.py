import io
from typing import Iterator, Optional

from config import *
from utils import create_client_engine


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ""

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret) :]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)


def clean_csv_value(value) -> str:
    if value is None:
        return r"\N"
    return str(value).replace("\n", "\\n")


def copy_string_iterator(df, size):
    client, client_engine = create_client_engine()
    string_iterator = StringIteratorIO(
        (
            "|".join(
                map(
                    clean_csv_value,
                    (
                        row["referrer"],
                        row["resource"],
                        row["type"],
                        row["number_of_occurrences"],
                        row["date"],
                        row["wiki"],
                    ),
                )
            )
            + "\n"
            for _, row in df.iterrows()
        )
    )
    # print(string_iterator.read())

    client.cursor.copy_from(
        string_iterator, f"staging_{DATAWAREHOUSE_TABLE}", sep="|", size=size
    )
    client.disconnect()
