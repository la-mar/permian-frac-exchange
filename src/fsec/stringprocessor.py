import re


class StringProcessor(str):
    re_wsws = re.compile(r"\s\s+")
    re_non_alphanum = re.compile(r"(?ui)\W")

    @classmethod
    def alphanum_only(cls, s: str, rep: str = " ") -> str:
        return cls.re_non_alphanum.sub(rep, s)

    @classmethod
    def dedupe_whitespace(cls, s: str, rep: str = " ") -> str:
        return cls.re_wsws.sub(rep, s)

    @classmethod
    def normalize(cls, s: str, replacement: str = " ", lower=True) -> str:
        """Normalizes the given string. Operations performed on the string are:
                - remove all non-alphanumeric characters
                - squish sequential whitespace to a single space
                - convert to all lower case
                - strip leading and trailing whitespace

        Arguments:
            string {str} -- a string to process

        Keyword Arguments:
            replacement {str} -- replacement for regex substitutions (default: {''})

        Returns:
            str -- the normalized string
        """

        if s is not None:
            s = cls.alphanum_only(s, replacement)
            s = cls.dedupe_whitespace(s, replacement)

            if not lower:
                s = cls.upper(s)
            else:
                s = cls.lower(s)
            s = cls.strip(s)
        return s


if __name__ == "__main__":

    sp = StringProcessor

    sp.normalize("test               TEST   Test")
