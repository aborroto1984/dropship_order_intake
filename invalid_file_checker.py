from typing import Callable
import pandas as pd
from dropship_db import ExampleDb
from xlsx_parser import XlsxParser
from tqdm import tqdm
import os


class InvalidFileChecker:
    def __init__(self, d_db: ExampleDb, parser: XlsxParser):
        self.d_db = d_db
        self.parser = parser

    def _check(self, file_path: str, rule: Callable[[str], bool]) -> bool:
        """Check if a file passes a rule"""

        return rule(file_path)

    def validate_files(self, file_path, header_template):
        """Validate files in a folder"""

        invalid_files = []
        valid_files = []

        # Checking if file is invalid
        for root, dirs, files in os.walk(file_path):
            for file in tqdm(files, desc=f"Checking for valid files in folder{root}"):
                # Getting the header template for the specific customer
                full_path = os.path.join(root, file)
                # Validity flag
                is_valid = True

                # List of rules NOTE: add more if needed
                rules = [
                    is_not_empty,
                    is_csv,
                    follows_template(header_template, self.parser),
                    is_not_duplicate(self.d_db),
                    it_has_required_content(self.parser),
                ]

                # Checking if file is invalid
                for rule in rules:
                    valid, reason = self._check(full_path, rule)
                    if not valid:
                        invalid_files.append((full_path, reason))

                        is_valid = False

                        print(f"File {full_path} is invalid")
                        break

                if is_valid:
                    valid_files.append(full_path)

        return valid_files, invalid_files


def clean_and_save_csv(csv_file_path):
    # Read the CSV into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Remove spaces from column headers
    df.columns = df.columns.str.replace(" ", "", regex=True)

    # Save the DataFrame back to the same CSV file
    df.to_csv(csv_file_path, index=False)


# Rules for checking files ========================================
def is_not_empty(file_path: str) -> bool:
    """Check if a file is empty"""

    result = os.path.getsize(file_path) > 0
    if result:
        return True, None
    else:
        return False, "File is empty"


def is_csv(file_path: str) -> bool:
    """Check if a file is a csv"""

    result = file_path.endswith(".csv")
    if result:
        return True, None
    else:
        return False, "File is not a csv"


def follows_template(
    header_template: list, parser: XlsxParser
) -> Callable[[str], bool]:
    """Check if a file follows a template"""

    def check_template(file_path: str) -> bool:
        try:
            clean_and_save_csv(file_path)

            df = parser._df_reader(file_path)
            columns = df.columns.tolist()

            result = columns == header_template
            if result:
                return True, None
            else:
                return False, "File does not follow template"

        except Exception as e:
            return False, "There was an error checking the template"

    return check_template


def is_not_duplicate(d_db: ExampleDb) -> Callable[[str], bool]:
    """Check if a file is a duplicate"""

    def check_for_duplicate(file_path: str) -> bool:
        result = d_db.check_for_duplicate(file_path)
        if result:
            return True, None
        else:
            return False, "File is a duplicate"

    return check_for_duplicate


def it_has_required_content(parser: XlsxParser) -> Callable[[str], bool]:
    """Check if a file has required content"""

    def check_column_values(file_path: str) -> bool:
        try:
            df = parser._df_reader(file_path)
            df.dropna(how="all", inplace=True)

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

            df = parser.standardize_columns(df)

            for column in required_columns:
                values = df[column].tolist()
                values = [value for value in values if pd.notna(value)]

                result = len(values) > 0

                if not result:
                    return False, f"Column {column} is empty"
                else:
                    continue

            return True, None

        except Exception as e:
            return False

    return check_column_values
