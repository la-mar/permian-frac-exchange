import re


class StringProcessor:
    re_ws = re.compile(r"\s+")
    re_dup_ws = re.compile(r"\s\s+")
    re_non_alphanum = re.compile(r"(?ui)\W")
    re_non_num = re.compile(r"[^\d+]")
    replacement = "_"
    tolower = True

    def __init__(
        self, replacement: str = "_", tolower: bool = True, toupper: bool = False
    ):

        self.replacement = replacement
        self.tolower = tolower
        self.toupper = toupper

    def alphanum_only(self, s: str) -> str:
        """ Replace all non-alphanumeric characters with whitespace. Whitespace is forced as a replacement so separators can optionally be inserted later.
        """

        return self.re_non_alphanum.sub(" ", s)

    def numeric_only(self, s: str) -> str:
        """ Replace all non-numeric characters with whitespace. Whitespace is forced as a replacement so separators can optionally be inserted later.
        """

        return self.re_non_num.sub(" ", s)

    def dedupe_whitespace(self, s: str) -> str:
        """ remove duplicated whitespaces
        """
        return self.re_dup_ws.sub(" ", s)

    def remove_whitespace(self, s: str) -> str:
        """ remove all whitespaces
        """
        return self.re_ws.sub("", s)

    def fill_whitespace(self, s: str, replacement: str = None) -> str:
        replacement = replacement if replacement is not None else self.replacement
        return s.replace(" ", replacement)

    def normalize(self, s: str, int_compatable: bool = False) -> str:
        """Normalizes the given string. Operations performed on the string are:
                - remove all non-alphanumeric characters
                - squish sequential whitespace to a single space
                - convert to all lower case
                - strip leading and trailing whitespace

        Arguments:
            s {str} -- a string to process

        Keyword Arguments:
            int_compatable {bool} -- return an int parsable string. The string is NOT converted to an integer type.

        Returns:
            str -- the normalized string
        """

        if s is not None:
            s = str(s)
            s = self.alphanum_only(s)
            s = self.dedupe_whitespace(s)
            s = str.strip(s)

            if int_compatable:
                s = self.remove_whitespace(s)
                # dont actually convert to int to avoid the potential failure point during ingestion
            else:
                s = self.fill_whitespace(s)

                if self.toupper:
                    s = s.upper()
                elif self.tolower:
                    s = s.lower()

        return s
