from typing import Generator, Dict, List, Union
from datetime import datetime

import xlrd
from util import StringProcessor

sp = StringProcessor()


class BytesFileHandler:
    @classmethod
    def xlsx(
        cls, content: bytes, sheet_no: int = 0, date_columns: List[str] = None
    ) -> Generator[Dict, None, None]:
        """ Extract the data of an Excel sheet from a byte stream """
        date_columns = date_columns or []
        sheet = xlrd.open_workbook(file_contents=content).sheet_by_index(sheet_no)

        keys = sheet.row_values(0)
        keys = [sp.normalize(x) for x in keys]

        for idx in range(1, sheet.nrows):
            result = dict(zip(keys, sheet.row_values(idx)))
            for dc in date_columns:
                value = result.get(dc)
                # print(f"{dc=}, {value=}")
                result[dc] = cls._parse_excel_date(value, sheet.book.datemode)

            yield result

    @classmethod
    def _parse_excel_date(cls, value: Union[float, None], date_mode: int = 0):
        if value:
            return datetime(*xlrd.xldate_as_tuple(value, date_mode))
        else:
            return value


if __name__ == "__main__":
    content = b""
    with open("data/bytes.txt", "rb") as f:
        content = f.read()

    rows = BytesFileHandler.xlsx(
        content, date_columns=["frac_start_date", "frac_end_date"]
    )

    # rows = BytesFileHandler.xlsx(content)

    for row in rows:
        print(row)
        break

        cls = BytesFileHandler
        sheet_no = 0
        BytesFileHandler._parse_excel_date(value, date_mode)

    datetime(*xlrd.xldate_as_tuple(43798.74804875, 0))
