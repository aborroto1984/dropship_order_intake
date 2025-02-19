from email_helper import send_email
from ftp import FTPManager
from dropship_db import ExampleDb
from invalid_file_checker import InvalidFileChecker
from xlsx_parser import XlsxParser
from tqdm import tqdm
import traceback
import os


def main():
    try:
        d_db = ExampleDb()
        ftp = FTPManager()

        # Getting the dropshipper data
        dropshipper_data = d_db.load_dropship_data()
        international_accounts = d_db.get_international_accounts()
        excluded_shipping_states = d_db.load_excluded_shipping_states()

        parser = XlsxParser(dropshipper_data, d_db)

        # Placeholders
        all_valid_files = {}
        all_invalid_files = {}
        all_order_objs = {}
        dropshipper_names = []

        for dropshipper in dropshipper_data.values():
            dropshipper_id = dropshipper["id"]
            dropshipper_name = dropshipper["name"]
            dropshipper_names.append(dropshipper_name)
            ftp_folder_name = dropshipper["ftp_folder_name"]
            header_template = dropshipper["headers"]

            # Downloading the files from the FTP server
            new_orders_file_path = ftp.download_files(ftp_folder_name)

            # If there are no new orders, skip to the next dropshipper
            if not new_orders_file_path:
                continue
            elif not os.listdir(new_orders_file_path):
                os.rmdir(new_orders_file_path)
                continue

            # Checking if the files are valid
            checker = InvalidFileChecker(d_db, parser)
            valid_files, invalid_files = checker.validate_files(
                new_orders_file_path, header_template
            )

            # Adding the files to dictionaries using the dropshipper_id as the key
            if valid_files:
                # NOTE: Turn off to not store the file names in the database to check for duplicates
                for path in tqdm(
                    valid_files, desc=f"Storing file names for {ftp_folder_name}"
                ):
                    file_name, pos = parser.data_extractor(
                        path, dropshipper["po_header_name"]
                    )
                d_db.store_file_names(file_name, pos, dropshipper_id, path)
                all_valid_files[dropshipper_id] = valid_files

            if invalid_files:
                all_invalid_files[dropshipper_name] = invalid_files

        # Parsing the files
        if all_valid_files:
            po_objs, unparsed_skus = parser.file_parser(all_valid_files)
            all_order_objs.update(po_objs)

        # Checking the allowed skus
        all_order_objs = parser.check_allowed_skus(all_order_objs, dropshipper_data)

        # If there are no new orders, skip the rest of the code
        if not all_order_objs:
            if all_valid_files:
                ftp.moving_files(all_valid_files, "order_logs")
            if all_invalid_files:
                ftp.moving_files(all_invalid_files, "error_logs", remove_from_tmp=True)
            return

        # Checking the shipping states
        unable_to_ship, shipable_orders_objs = parser.check_shipping_states(
            all_order_objs, excluded_shipping_states, international_accounts
        )

        # Sending an email with the orders that can't be shipped
        if unable_to_ship:
            send_email(
                "Orders Unable to Ship",
                f"Orders unable to ship: {unable_to_ship}",
            )

        # NOTE: Turn off to not store the orders in the database
        if shipable_orders_objs:
            if d_db.store_purchase_orders(shipable_orders_objs):
                # Moving the valid files to the order_logs folder
                ftp.moving_files(all_valid_files, "order_logs")

            else:
                send_email(
                    "Error Storing Orders",
                    "There was an error storing the orders in the database. Valid files were never moved from their FTP folders. Re-run the  dropship_order_import script to try again.",
                )
        # Moving the invalid files to the error_logs folder
        ftp.moving_files(all_invalid_files, "error_logs", remove_from_tmp=True)

        d_db.close()

    except Exception as e:
        print(f"There was an error: {e}")
        send_email("An Error Occurred", f"Error: {e}\n\n{traceback.format_exc()}")
        raise e


if __name__ == "__main__":
    main()
