import logging
import pytest  # noqa

from collector.downloader import Ftp, InvalidCredentialsError

logger = logging.getLogger(__name__)


class TestsFTP:
    def test_ftpserver_class(self, ftpserver):
        assert ftpserver.uses_TLS is False

    def test_ftpserver(self, ftpserver):
        logger.warning("LOGIN")
        logger.warning(ftpserver.get_login_data())

        login = ftpserver.get_login_data()
        url = login["host"]
        username = login["user"]
        password = login["passwd"]
        port = login["port"]

        ftp = Ftp(url, username, password, port=port)
        ftp.list_files()

    def test_load_ftp_from_config(self, ftpserver, conf):
        login = ftpserver.get_login_data()
        conf.COLLECTOR_FTP_URL = login["host"]
        conf.COLLECTOR_FTP_USERNAME = login["user"]
        conf.COLLECTOR_FTP_PASSWORD = login["passwd"]
        conf.COLLECTOR_FTP_PORT = login["port"]
        Ftp.from_config(conf)

    def test_load_ftp_from_config_bad_password(self, ftpserver, conf):
        login = ftpserver.get_login_data()
        conf.COLLECTOR_FTP_PASSWORD = "badpassword"
        conf.COLLECTOR_FTP_URL = login["host"]
        conf.COLLECTOR_FTP_PORT = login["port"]

        with pytest.raises(InvalidCredentialsError):
            Ftp.from_config(conf)

    # def test_load_ftp_from_config_bad_url(self, ftpserver, conf):
    #     login = ftpserver.get_login_data()
    #     username = login["user"]
    #     password = login["passwd"]

    #     with pytest.raises(ConnectionRefusedError):
    #         Ftp("localhost", username, password, port=21)

    def test_ftp_access_basepath(self, ftp):
        assert ftp.basepath == "."

    def test_ftp_set_basepath_to_same_value(self, ftp):
        ftp.basepath = "."
        assert ftp.basepath == "."

    def test_ftp_set_basepath_to_invalid_value(self, ftpserver):
        login = ftpserver.get_login_data()
        url = login["host"]
        username = login["user"]
        password = login["passwd"]
        port = login["port"]

        ftp = Ftp(url, username, password, port=port)
        with pytest.raises(Exception):
            ftp.basepath = "/no/no/no"

    def test_check_credentials_all_present(self, ftp, conf):
        creds = conf.with_prefix("collector_ftp")
        ftp.check_connection_details(creds)

    def test_missing_credentials(self, ftp, conf):
        creds = conf.with_prefix("collector_ftp")
        creds["username"] = None
        creds["password"] = None
        with pytest.raises(InvalidCredentialsError):
            ftp.check_connection_details(creds)

    def test_ftp_upload(self, ftp, tmpdir):
        path = tmpdir.mkdir("sub").join("upload.txt")
        path.write("content")
        dest = "/upload.txt"
        result = ftp.upload(path, to=dest)
        assert result == {"to": dest, "filename": str(path), "status": "success"}

    def test_ftp_upload_file_no_exist(self, ftp):
        result = ftp.upload("/pretend.txt", to="/pretend.txt")
        assert result["status"] == "error"

    def test_ftp_get_file(self, ftp, tmpdir):
        path = tmpdir.mkdir("sub").join("upload.txt")
        path.write("content")
        dest = "/upload.txt"
        result = ftp.upload(path, to=dest)
        result = ftp.get(dest)
        assert result["status"] == "success"
        assert result["filename"] == "/upload.txt"
        assert result["content"] == b"content"

    def test_ftp_get_latest_file(self, ftp, tmpdir):
        tempdir = tmpdir.mkdir("sub")
        path = tempdir.join("upload.txt")
        path.write("content")
        dest = "/upload.txt"
        result = ftp.upload(path, to=dest)

        path = tempdir.join("latest.txt")
        path.write("latest content")
        dest = "/latest.txt"
        result = ftp.upload(path, to=dest)

        result = ftp.get_latest()
        assert result["status"] == "success"
        assert result["filename"] == "latest.txt"
        assert result["content"] == b"latest content"

    def test_ftp_cleanup(self, ftp, tmpdir):
        tempdir = tmpdir.mkdir("sub")
        path = tempdir.join("upload.txt")
        path.write("content")
        dest = "/upload.txt"
        ftp.upload(path, to=dest)

        path = tempdir.join("latest.txt")
        path.write("latest content")
        dest = "/latest.txt"
        ftp.upload(path, to=dest)

        ftp.cleanup()
        assert len(ftp.list_files()) == 1

