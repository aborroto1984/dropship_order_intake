import pandas as pd
import numpy as np
from datetime import datetime
from dropship_db import ExampleDb
from tqdm import tqdm
import re


class XlsxParser:
    def __init__(self, dropshipper_data, d_db: ExampleDb):
        self.dropshipper_data = dropshipper_data
        self.d_db = d_db
        self.headder_maps = self.d_db.get_header_maps()
        self.country_and_states = self.d_db.get_country_and_states()

    def check_shipping_states(
        self, all_po_objs, excluded_shipping_states, international_accounts
    ):
        """This function removes orders with excluded shipping states"""
        try:
            shipable_orders_objs = {}
            unable_to_ship = {}
            for po_number, po_obj in tqdm(
                all_po_objs.items(), desc="Checking shipping states"
            ):
                if po_obj["state"] not in excluded_shipping_states:
                    shipable_orders_objs[po_number] = po_obj
                elif po_obj["dropshipper_id"] in international_accounts:
                    po_obj["dropshipper_id"] = international_accounts[
                        po_obj["dropshipper_id"]
                    ]
                    shipable_orders_objs[po_number] = po_obj
                else:
                    unable_to_ship[po_number] = po_obj

            return unable_to_ship, shipable_orders_objs
        except Exception as e:
            print(f"Error checking shipping states: {e}")
            raise

    def file_parser(self, valid_files):
        """This function parses the files and returns a list of purchase order objects"""
        dfs = []
        for dropshipper_id, file_paths in valid_files.items():
            for file_path in file_paths:
                df = self._df_reader(file_path)
                # Removing whitespaces from the column names
                df.columns = [col.replace(" ", "") for col in df.columns]
                df = df.dropna(how="all").fillna("")
                # Adding the dropshipper_id column to the dataframe
                df.loc[:, "dropshipper_id"] = np.nan
                # Updating the dropshipper_id column to the value for rows where the column is not empty
                df.loc[df.notna().any(axis=1), "dropshipper_id"] = dropshipper_id
                dfs.append(self.standardize_columns(df))

        # Concatenating all the dataframes into one
        df = pd.concat(dfs, ignore_index=True)

        return self._parse(df)

    def standardize_columns(self, df):
        """This function standardizes the columns of the dataframe by renaming them to the standard names"""

        for standard_name, variant_names in self.headder_maps.items():
            for variant_name in variant_names:
                if variant_name in df.columns:
                    df.rename(columns={variant_name: standard_name}, inplace=True)
                    break
        return df

    def _transform_data(self, df):
        """This function transforms the data in the dataframe"""

        # Transform city
        df["city"] = df["city"].apply(self._text_formater)

        # Zip code formatting
        df["zip"] = df["zip"].apply(self._zip_formater)

        # Name formatting
        df["customer_first_name"] = df["customer_first_name"].apply(lambda x: x.title())
        df["customer_last_name"] = df["customer_last_name"].apply(lambda x: x.title())

        # Address concatenation
        df["address"] = df.apply(
            lambda row: (
                row["address_1"] + " " + row["address_2"]
                if "address_2" in row
                else row["address_1"]
            ),
            axis=1,
        )

        # Country and State formatting
        df[["country", "state"]] = df.apply(
            lambda row: pd.Series(
                self._country_and_state_formater(row["country"], row["state"])
            ),
            axis=1,
        )

        # Phone number formatting
        df["phone"] = df["phone"].apply(self._phone_formater)

        # Handling missing purchase order dates
        if "purchase_order_date" not in df.columns:
            df["purchase_order_date"] = ""

        df["purchase_order_date"] = df["purchase_order_date"].apply(
            lambda x: datetime.now().strftime("%Y-%m-%d %H:%M:%S") if x == "" else x
        )

        # Convert quantity to integer
        df["quantity"] = df["quantity"].astype(int)

        # Convert dropshipper ID to integer
        df["dropshipper_id"] = df["dropshipper_id"].astype(int)

        return df

    def _has_all_required_columns(self, row: pd.Series):
        """This function checks if the row has all the required columns"""

        required_columns = [
            "purchase_order_number",
            "customer_first_name",
            "address_1",
            "city",
            "country",
            "state",
            "zip",
            "sku",
            "quantity",
        ]

        # Identify missing columns
        missing_columns = [col for col in required_columns if getattr(row, col) == ""]

        if missing_columns:
            return False, missing_columns
        else:
            return True, missing_columns

    def _has_valid_sku(self, sku):
        """This function checks if the sku has valid characters"""

        pattern = r"^[a-zA-Z0-9/-]*$"
        return bool(re.match(pattern, sku))

    def _parse(self, df):
        """This function parses the dataframe and returns a list of purchase order objects"""
        unparsed_skus = {}
        po_objs = {}

        df = df.dropna(how="all").fillna("")

        df = self._transform_data(df)

        for row in tqdm(df.itertuples(), desc="Parsing files"):
            try:
                sku = row.sku.replace(" ", "")
                able_to_parse, missing_columns = self._has_all_required_columns(row)
                correct_sku_format = self._has_valid_sku(sku)

                if row.city == "" or row.country == "" or row.state == "":
                    pass

                if not able_to_parse:
                    raise Exception("Missing required columns")

                if not correct_sku_format:
                    raise Exception("Sku has invalid characters")

                purchase_order_number = row.purchase_order_number

                # This checks if the purchase order number is already in the dictionary if it is it adds the item to the items list
                if purchase_order_number in po_objs:
                    po_objs[purchase_order_number]["items"][sku] = row.quantity
                    continue

                # If the purchase order number is not in the dictionary it creates a new purchase order object
                else:
                    # The key in the purchase order object is the purchase order number
                    po_objs[purchase_order_number] = {}
                    po_obj = po_objs[purchase_order_number]
                    po_obj["purchase_order_number"] = purchase_order_number
                    po_obj["purchase_order_date"] = row.purchase_order_date
                    po_obj["customer_first_name"] = row.customer_first_name
                    po_obj["customer_last_name"] = row.customer_last_name
                    po_obj["address"] = row.address
                    po_obj["city"] = row.city
                    po_obj["country"] = row.country
                    po_obj["state"] = row.state
                    po_obj["zip"] = row.zip
                    po_obj["phone"] = row.phone
                    po_obj["dropshipper_id"] = row.dropshipper_id
                    po_obj["items"] = {sku: row.quantity}

            except Exception as e:
                # Finding the dropshipper name
                for data in self.dropshipper_data.values():
                    if data["id"] == row.dropshipper_id:
                        dropshipper_name = data["name"]
                        break

                if dropshipper_name in unparsed_skus:
                    unparsed_skus[dropshipper_name].append((row, missing_columns))
                else:
                    unparsed_skus[dropshipper_name] = [(row, missing_columns)]

                continue

        return po_objs, unparsed_skus

    def _country_and_state_formater(self, country, state):
        """This function formats the country and state to the two letter codes"""
        try:
            # Removing anythis that is not a letter
            country = re.sub("[^a-zA-Z]+", "", country)
            state = re.sub("[^a-zA-Z]+", "", state)
            country_not_found = False

            for country_key, states in self.country_and_states.items():
                # It checks if the country in the dataframe is in the country and states dictionary
                if country.upper() in country_key or country.title() in country_key:
                    country_two_letter_code = country_key[1]
                    country = country_two_letter_code

                    # If the state string is longer than 2 characters it checks if the full state name is in the states dictionary
                    if len(state) > 2:
                        state = states[state.title()]
                    # Else it returns the state as is but capitalized
                    else:
                        state = state.upper()

                    country_not_found = False
                    break

                else:
                    country_not_found = True

            if country_not_found:
                country = None
                state = None

            return country, state

        except Exception as e:
            print(f"Error formating country and state: {e}")
            return country, state

    def _text_formater(self, text, remove_empty_spaces=False):
        """This function formats the test to title case and removes all non letter characters"""
        try:
            if remove_empty_spaces:
                # Removing anything that is not a letter or an empty space
                text = re.sub("[^a-zA-Z]+", "", text)
            else:
                # Removing anything that is not a letter
                text = re.sub("[^a-zA-Z ]+", "", text)

            # Correcting the capitalization
            if text.isupper():
                return text.title()
            return text
        except Exception as e:
            print(f"Error formating text: {e}")
            return text

    def _phone_formater(self, phone, shipstation=False):
        """This function formats the phone number to an int and removes all non numeric characters"""
        remove = "[^0-9]"
        try:
            if shipstation:
                # Extracting just the phone number digits, excluding the extension
                phone_number_match = re.search(
                    r"\+?\d+[\s-]?(\d+)[\s-]?(\d+)[\s-]?(\d+)", phone
                )

                if phone_number_match:
                    phone = int("".join(phone_number_match.groups()))
                else:
                    phone = 0
            else:
                phone = int(re.sub(remove, "", phone))

            # if phone == 0 or None:
            #     phone = None

            return phone
        except Exception as e:
            print(f"Error formating phone number: {e}")
            return phone

    def _zip_formater(self, zip_code):
        if len(zip_code) < 5:
            return str(zip_code).zfill(5)
        if len(zip_code) > 5:
            return zip_code[:5]
        else:
            return zip_code

    def _df_reader(self, file_path):
        """This function reads the file and returns a dataframe"""
        try:
            df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

        except UnicodeDecodeError:
            try:
                df = pd.read_csv(
                    file_path, dtype=str, encoding="ISO-8859-1"
                )  # Trying with latin1 encoding
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(
                        file_path, dtype=str, encoding="cp1252"
                    )  # Trying with Windows encoding
                except UnicodeDecodeError as e:
                    print(f"Error reading the file: {e}")
                    return None

        return df

    def data_extractor(self, path, po_header_name):
        try:
            file_name = path.split("\\")[-1]

            pos = []

            df = self._df_reader(path)
            df = df[pd.notnull(df[po_header_name]) & (df[po_header_name] != "")]
            df[po_header_name] = df[po_header_name].astype(str)
            pos = df[po_header_name].tolist()

            return file_name, pos

        except Exception as e:
            print(f"Error while parsing the path: {e}")

    def _sku_cleaner(self, sku_number):
        """This function cleans the skus"""
        starts_with_sku = sku_number.startswith("SKU ")
        if starts_with_sku:
            return None
        # Check for 'S' at start and '-R', '-S', or '-P' anywhere in sku
        elif sku_number.startswith("S") or any(
            marker in sku_number for marker in ["-R", "-S", "-P", "-FBA"]
        ):
            # Use regex to remove 'S' at start and '-R', '-S', '-P' anywhere
            sku_number = re.sub(r"^S|(-R)|(-S)|(-P)|(-FBA)", "", sku_number)
        elif "-SML" in sku_number:
            # No action needed, alias is already None
            pass

        return sku_number
