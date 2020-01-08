from typing import Generator, Dict
import xlrd
from util import StringProcessor

sp = StringProcessor()


class BytesFileHandler:
    @classmethod
    def xlsx(cls, content: bytes, sheet_no: int = 0) -> Generator[Dict, None, None]:
        """ Extract the data of an Excel sheet from a byte stream """
        sheet = xlrd.open_workbook(file_contents=content).sheet_by_index(sheet_no)

        keys = sheet.row_values(0)
        keys = [sp.normalize(x) for x in keys]

        for idx in range(1, sheet.nrows):
            yield dict(zip(keys, sheet.row_values(idx)))


if __name__ == "__main__":

    content = b""
    with open("data/bytes.txt", "rb") as f:
        content = f.read()

    rows = BytesFileHandler.xlsx(content)

    for row in rows:
        print(row)
        break
