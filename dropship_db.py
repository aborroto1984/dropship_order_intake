import pyodbc
import pandas as pd
from config import create_connection_string, db_config
from email_helper import send_email
from datetime import datetime
from tqdm import tqdm


class ExampleDb:
    def __init__(self):
        try:
            # DropshippSellerCloud database connection
            self.conn_sc = pyodbc.connect(
                create_connection_string(db_config["ExampleDb"])
            )
            self.cursor_sc = self.conn_sc.cursor()
        except pyodbc.Error as e:
            print(f"Error establishing connection to the Example database: {e}")
            raise

    def check_for_duplicate_files(self, file_path):
        """Check if the file has already been uploaded to the database"""
        try:
            file_name = file_path.split("\\")[-1]

            self.cursor_sc.execute(
                """
                SELECT * FROM PurchaseOrderFiles 
                WHERE file_name = ?
                """,
                file_name,
            )

            if self.cursor_sc.fetchone():
                return False
            else:
                return True

        except Exception as e:
            print(f"Error while checking for duplicate files: {e}")
            raise

    def check_for_duplicate_orders(self, purchase_order_number):
        """Check if the order has already been uploaded to the database"""
        try:
            self.cursor_sc.execute(
                """
                SELECT * FROM PurchaseOrders
                WHERE purchase_order_number = ?
                """,
                purchase_order_number,
            )

            if self.cursor_sc.fetchone():
                return True
            else:
                return False

        except Exception as e:
            print(f"Error while checking for duplicate orders: {e}")
            raise

    def store_file_names(self, file_name, pos, dropshipper_id, path):
        """Store the file name and purchase order numbers in the database"""

        files_not_uploaded = []

        try:
            self.cursor_sc.execute(
                """
                INSERT INTO PurchaseOrderFiles (dropshipper_id, file_name, date)
                VALUES (?, ?, ?)
                """,
                dropshipper_id,
                file_name,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            file_id = self.cursor_sc.execute("SELECT @@IDENTITY AS id").fetchone().id

            self.cursor_sc.executemany(
                """
                INSERT INTO PurchaseOrderFileItems (purchase_order_file_id, purchase_order_number)
                VALUES (?, ?)
                """,
                [(file_id, po) for po in pos],
            )

        except Exception as e:
            print(f"Error while storing the file path: {e}")
            files_not_uploaded.append(path)

        self.conn_sc.commit()

        if files_not_uploaded:
            send_email(
                "Error Uploading Files to Database",
                f"Error uploading the following files to the database: {files_not_uploaded}",
            )

    def store_purchase_orders(self, po_objs):
        """Store the purchase orders in the database"""

        for po_obj in tqdm(po_objs.values(), desc="Storing purchase orders"):
            try:
                # Inserting into PurchaseOrders
                self.cursor_sc.execute(
                    """
                            INSERT INTO PurchaseOrders (
                                purchase_order_number,
                                purchase_order_date,
                                date_added,
                                customer_first_name,
                                customer_last_name,
                                address,
                                city,
                                state,
                                zip,
                                country,
                                phone,
                                dropshipper_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, 
                                (SELECT id FROM States WHERE code = ?), ?, 
                                (SELECT id FROM Countries WHERE two_letter_code = ?), ?, ?)
                                """,
                    po_obj["purchase_order_number"],
                    po_obj["purchase_order_date"],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    po_obj["customer_first_name"],
                    po_obj["customer_last_name"],
                    po_obj["address"],
                    po_obj["city"],
                    po_obj["state"],
                    po_obj["zip"],
                    po_obj["country"],
                    po_obj["phone"],
                    po_obj["dropshipper_id"],
                )

                purchase_order_id = (
                    self.cursor_sc.execute("SELECT @@IDENTITY AS id").fetchone().id
                )

                for sku, quantity in po_obj["items"].items():
                    self.cursor_sc.execute(
                        """
                                INSERT INTO PurchaseOrderItems (
                                    purchase_order_id,
                                    sku,
                                    quantity)
                                    VALUES (?, ?, ?)
                                    """,
                        purchase_order_id,
                        sku,
                        quantity,
                    )

            except Exception as e:
                print(f"Error while storing new purchase orders: {e}")
                self.conn_sc.rollback()
                send_email(
                    "Error Storing Orders",
                    f"The following error occurred while storing new purchase orders: {e}\norder: {po_obj} was unable to be stored in the ExampleDb.",
                )
                continue

        self.conn_sc.commit()

        return True

    def load_dropship_data(self):
        """Load dropshipper data from the database"""

        try:
            self.cursor_sc.execute(
                """
                SELECT 
                    d.id,
                    d.name,
                    d.code,
                    d.use_our_shipping_account,
                    d.ftp_folder_name,
                    ff.name AS format_name,
                    po_ffd.header_name AS po_header_name, -- Specific PO header name
                    STRING_AGG(ffd.header_name, ', ') WITHIN GROUP (ORDER BY ffd.header_order) AS all_header_names -- Aggregate all header names
                FROM 
                    dropshippers d 
                JOIN 
                    DropshipperFileFormats dff ON dff.dropshipper_id = d.id
                JOIN 
                    FileFormats ff ON ff.id = dff.format_id
                LEFT JOIN 
                    FileFormatDetails po_ffd ON po_ffd.id = d.po_header_format_detail_id
                JOIN 
                    FileFormatDetails ffd ON ffd.format_id = ff.id
                WHERE 
                    ff.type = 'order' 
                    AND d.po_header_format_detail_id IS NOT NULL
                GROUP BY 
                    d.id, d.name, d.code, d.use_our_shipping_account, d.ftp_folder_name, ff.name, po_ffd.header_name
                ORDER BY 
                    d.name;
                """
            )
            dropshipper_data = {}

            for row in self.cursor_sc.fetchall():
                dropshipper_data[row.ftp_folder_name] = {
                    "id": row.id,
                    "name": row.name,
                    "code": row.code,
                    "use_our_shipping_account": row.use_our_shipping_account,
                    "ftp_folder_name": row.ftp_folder_name,
                    "format": row.format_name,
                    "po_header_name": row.po_header_name,
                    "headers": row.all_header_names.split(", "),
                }

            return dropshipper_data

        except Exception as e:
            print(f"Error while getting data from the Dropship database: {e}")
            raise

    def get_country_and_states(self):
        """Get country and states from the database"""

        try:
            self.cursor_sc.execute(
                """
                SELECT
                    c.name as country_name,
                    c.two_letter_code,
                    c.three_letter_code, 
                    s.code, 
                    s.name as state_name
                FROM Countries c
                JOIN States s ON s.country_id = c.id
                """
            )

            rows = self.cursor_sc.fetchall()
            if rows:
                result = {}
                for row in rows:
                    country_key = (
                        row.country_name,
                        row.two_letter_code,
                        row.three_letter_code,
                    )
                    if country_key not in result:
                        result[country_key] = {}
                    result[country_key][row.state_name] = row.code

            return result

        except Exception as e:
            print(f"Error while getting country and states: {e}")
            raise

    def get_header_maps(self):
        """Get header mappings from the database. The header mapping are necessary because not al dropshippers use the same headers"""

        try:
            query = """
                    SELECT  hm.normalized_name, ffd.header_name
                    FROM FileFormatDetails ffd
                    INNER JOIN HeaderMappings hm ON ffd.header_mapping_id = hm.id
                    """
            self.cursor_sc.execute(query)

            rows = self.cursor_sc.fetchall()

            header_variants = {}
            for normalized_name, variant in rows:
                if normalized_name in header_variants:
                    header_variants[normalized_name].append(variant)
                else:
                    header_variants[normalized_name] = [variant]

            return header_variants

        except Exception as e:
            print(f"Error while getting header maps: {e}")
            raise

    def load_excluded_shipping_states(self):
        """Returns a list of state codes where our shipping account does not ship to"""
        try:
            self.cursor_sc.execute(
                """
                SELECT DISTINCT s.code FROM States s
                JOIN ExcludedShippingStates ess ON ess.state_id = s.id
                """
            )
            return [row.code for row in self.cursor_sc.fetchall()]

        except Exception as e:
            print(f"Error while getting excluded shipping states: {e}")
            raise

    def get_international_accounts(self):
        """Get the international dropshipper shipping accounts"""

        try:
            self.cursor_sc.execute(
                """
                SELECT id, code FROM Dropshippers
                WHERE name LIKE '%international%'
                """
            )

            return {
                self._get_dropshipper_id(row.code): row.id
                for row in self.cursor_sc.fetchall()
            }

        except Exception as e:
            print(f"Error while getting international accounts: {e}")
            raise

    def _get_dropshipper_id(self, dropshipper_code):
        """Get the dropshipper id from the database"""

        try:
            self.cursor_sc.execute(
                """
                SELECT id FROM Dropshippers
                WHERE code = ?
                """,
                dropshipper_code,
            )
            return self.cursor_sc.fetchone().id
        except Exception as e:
            print(f"Error while getting dropshipper id: {e}")
            raise

    def close(self):
        self.cursor_sc.close()
        self.conn_sc.close()
