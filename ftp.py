import os
import ftplib
import pathlib
from datetime import datetime
from config import ftp_server
from tqdm import tqdm


class FTPManager:
    def __init__(self):
        self.host = ftp_server["server"]
        self.username = ftp_server["username"]
        self.password = ftp_server["password"]

    def _create_local_dir(self, customer_name):
        """Create a local directory to store the downloaded files"""

        datetime_now = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_dir = pathlib.Path(f"tmp/{customer_name}/{datetime_now}/")
        local_dir.mkdir(parents=True, exist_ok=True)
        return local_dir

    def download_files(self, ftp_folder_name):
        """Download order files from the FTP server"""

        try:
            # Starting connection to FTP
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login(self.username, self.password)

            # remote_folder = f"test_dropshipper/{ftp_folder_name}/orders"
            remote_folder = f"dropshipper/{ftp_folder_name}/orders"

            files = self.ftp.nlst(remote_folder)

            # Create the local directory for downloads
            local_dir = self._create_local_dir(ftp_folder_name)

            for file in tqdm(files, desc=f"Downloading {ftp_folder_name} files"):
                # Skip directories
                if file.endswith("/"):
                    continue

                # Construct the local file path
                local_file_path = local_dir / pathlib.Path(file).name

                # Download the file
                with open(local_file_path, "wb") as local_file:
                    self.ftp.retrbinary(
                        f"RETR {remote_folder}/{pathlib.Path(file).name}",
                        local_file.write,
                    )

            self.ftp.quit()

            return str(local_dir)

        except ftplib.all_errors as e:
            # Closing
            self.ftp.quit()

            print(f"There was an error downloading order files from FTP server: {e}")

    def moving_files(self, all_files, destination, remove_from_tmp=False):
        """Move files to the logs folder in the FTP server"""

        try:
            # Starting connection to FTP
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login(self.username, self.password)

            for dropshiper_file_path in tqdm(
                all_files.values(), desc=f"Moving valid files to {destination}"
            ):
                for tupple in dropshiper_file_path:
                    # The tupple can be a file path or a tupple with the file path and the reason
                    if remove_from_tmp:
                        file_path = tupple[0]
                    else:
                        file_path = tupple

                    if remove_from_tmp:
                        # Removing file from tmp folder
                        os.remove(file_path)

                    ftp_folder_name = file_path.split("\\")[1]
                    file_name = file_path.split("\\")[-1]

                    origin_folder = (
                        f"dropshipper/{ftp_folder_name}/orders/{file_name}"
                        # f"test_dropshipper/{ftp_folder_name}/orders/{file_name}"
                    )
                    log_folder = (
                        f"dropshipper_logs/{destination}/{ftp_folder_name}/{file_name}"
                    )

                    try:
                        self.ftp.rename(origin_folder, log_folder)
                        print(
                            f"File moved successfully from {origin_folder} to {log_folder}"
                        )

                    except ftplib.all_errors as e:
                        print(
                            f"There was an error moving invalid folders in the FTP server: {e}"
                        )

            # Closing
            self.ftp.quit()

        except ftplib.all_errors as e:
            print(f"There was an error removing files from FTP server: {e}")
