from typing import Generator, Dict, List, Union
from datetime import datetime
import logging

import xlrd
from util import StringProcessor

logger = logging.getLogger(__name__)

sp = StringProcessor()


class BytesFileHandler:
    @classmethod
    def xlsx(
        cls, content: bytes, sheet_no: int = 0, date_columns: List[str] = None
    ) -> Generator[Dict, None, None]:
        """ Extract the data of an Excel sheet from a byte stream """
        date_columns = date_columns or []

        try:
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
        except TypeError as te:
            logger.error(f"Error converting bytes to xlsx -- {te}")
            yield {}

    @classmethod
    def _parse_excel_date(cls, value: Union[float, None], date_mode: int = 0):
        if value:
            return datetime(*xlrd.xldate_as_tuple(value, date_mode))
        else:
            return value
