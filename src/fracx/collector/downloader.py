from __future__ import annotations

import io
import logging
import os
import ssl
from ftplib import FTP, FTP_TLS, error_perm
from timeit import default_timer as timer
from typing import Dict, Generator, List, Union

from config import get_active_config
from util import hf_size


logger = logging.getLogger(__name__)

conf = get_active_config()


class ImplicitFTP_TLS(FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS.
    Source: https://stackoverflow.com/questions/12164470/python-ftp-implicit-tls-connection-issue"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value


class Ftp(FTP):
    """Extends ftplib.FTP, adding components to integrate with an FtpModel
    object to facilitate directory navigation, file listing, and downloading.
    """

    _basepath = "/"

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        destination: str = None,
        basepath: str = None,
        port: Union[int, str] = 21,
        **kwargs,
    ):
        super().__init__()
        self.url = url
        self.username = username
        self.password = password
        self.destination = destination
        self.connect(host=url, port=int(port))
        self.login(username, password)
        self.m = None  # model hook
        self.basepath = basepath or "."
        self.cwd(self.basepath)

    @property
    def basepath(self):
        return self._basepath

    @basepath.setter
    def basepath(self, path: str) -> None:
        if path == self._basepath:
            logger.info(
                """Attempt to alter basepath resulted in no change. The current
                        value of basepath is equal to the provided value:\n
                        {path}""".format(
                    path=self._basepath
                )
            )
        else:
            try:
                self._basepath = path
                self.cwd(self.basepath)
                logger.debug(f"Basepath changed to {self._basepath}")
            except Exception as e:
                logger.warning(f"{e} -- Could not change basepath to {self._basepath}")

    def list_files(self, path: str = None, contains: str = ".") -> List:
        """Return a listing of the contents in the ftp directory specified by the
        input path. If no path is specified, returns the listing of the ftp's root
        directory.

        Contents of the directory are filtered according to the 'contains' parameter.

        Arguments:
            path {str} -- path do a directory on the ftp server (default: None)
            contains(str) -- string qualifying file names that should be returned
                             (default: '.')

        Returns:
            list -- a list of file names
        """

        return [file for file in self.nlst(path or self.basepath) if contains in file]

    @staticmethod
    def from_config() -> Ftp:
        """Create an Ftp object from environment variables. Looks for environment
           variables prefixed with "FRACX_" that correspond to the required parameter
           name.

           FRACX_FTP_URL = Url to ftp site (e.g. provier.domain.com)
           FRACX_FTP_USERNAME = Username credential
           FRACX_FTP_PASSWORD = Password credential
           FRACX_FTP_PATH = Path to the ftp directory containing the frac schedules
                           (e.g. /example/path/to/FracSchedules/)
           FRACX_DOWNLOAD_DIR = Path to location to save downloaded files


        Returns:
            Ftp
        """
        c = conf.with_prefix("collector_ftp")
        return Ftp(
            url=c.get("url"),
            username=c.get("username"),
            password=c.get("password"),
            basepath=c.get("outpath"),
            port=c.get("port"),
        )

    def upload(self, filename: str):
        """Downloads all files located in the ftp directory located at the basepaths.

        Arguments:
            filename {str} -- path to local file to upload

        Keyword Arguments:
            to {str} -- repository path to which the file should be uploaded. If not
            specified, the current directory of the ftp instance will be used.
            (default: {None})

        """

        to = os.path.join(conf.COLLECTOR_FTP_INPATH, os.path.basename(filename))
        status = "error"
        try:
            with open(os.path.join(filename), "rb") as f:
                print(self.storbinary("STOR " + to, f))
                logger.info("Uploaded: " + filename)

        except error_perm as ep:
            logger.warning(f"{ep} -- {filename}")

        except TimeoutError as te:
            logger.error(f"{te} -- {filename}")
            status = "timeout"
        except Exception as e:
            logger.error(f"{e} -- {filename}")
            status = "error"
        else:
            status = "success"

        return {"to": to, "filename": filename, "status": status}

    def get_all(self) -> Generator:
        """Downloads all files located in the ftp directory located at the basepaths.

        Keyword Arguments:
            to {str} -- local download path. If not specified, the destination path
            used to instatiate the Ftp object is used. If it is also None, the download
            will fail. (default: {None})

        Returns:
            Generator -- returns a generator yielding tuples containing the downloaded
                         file's path, name, and status (e.g. filepath, filename,
                         download_status)

        """

        for filename in self.list_files():
            yield self.get(filename)

    def get(self, filename: Union[str, None]) -> Dict[str, Union[str, bytes, None]]:
        """Implementation of the core downloading function.  Each call to this
        method operates on a single file, downloading it to the designated location.

        Arguments:
            filename {str} -- repository path from which the file should be downloaded

        Keyword Arguments:
            to {str} -- local download path. If not specified, the destination path
            used to instatiate the Ftp object is used. If it is also None, the download
            will fail. (default: {None})

        Returns:
            tuple -- (filepath, filename, download_status)
        """

        status = "ERROR"

        try:
            result = {"status": status, "filename": filename, "content": b""}
            logger.info(f"Starting download from {self.url}...")
            byteio = io.BytesIO()

            ts = timer()
            self.retrbinary("RETR " + filename, byteio.write)  # type: ignore
            te = timer()
            exc_time = round(te - ts, 0)

            content = byteio.getvalue()
            size = len(content)

            logger.info(
                "Download successful (download size: %s, download_time: %ss)",
                hf_size(size or 0),
                exc_time,
                extra={"download_bytes": size, "download_seconds": exc_time},
            )

            status = "success"
            result["content"] = content

        except error_perm as e:
            logger.warning(f"{e} -- {filename}")

        return result

    @property
    def latest_filename(self) -> Union[str, None]:

        names = self.nlst()

        latest_time = None
        latest_name = None

        for name in names:
            time = self.voidcmd("MDTM " + name)
            if (latest_time is None) or (time > latest_time):
                latest_name = name
                latest_time = time

        return latest_name

    def get_latest(self) -> Dict:
        return self.get(self.latest_filename)

    def cleanup(self):
        """ Delete all files in the current directory except for the most recent export """
        files = self.nlst()
        latest = self.latest_filename
        for filename in files:
            if filename != latest:
                result = self.delete(filename)
                logger.info(f"Deleted old export from FTP: {result}")


class Sftp(ImplicitFTP_TLS, Ftp):
    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        destination: str = None,
        basepath: str = None,
        **kwargs,
    ):
        super().__init__()
        self.url = url
        self.username = username
        self.password = password
        self.destination = destination
        self.ssl_version = ssl.PROTOCOL_SSLv23
        self.connect(host=url, port=990)
        self.login(username, password)
        self.m = None  # model hook
        self.basepath = basepath or "."
        self.cwd(self.basepath)


if __name__ == "__main__":
    logging.basicConfig(level=10)
    f = Ftp.from_config()
    result = f.get_latest()

    f.cleanup()

    # fname = result.get("filename")
    # f.delete(fname)
