import datetime
import re
from mescobrad_edge.plugins.questionnaire_anonymisation_plugin.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
from datetime import date

class GenericPlugin(EmptyPlugin):

    def age(self, birthdate):
        # Get today date
        today = date.today()

        # A bool that represents if today's day/month precedes the birth day/month
        one_or_zero = ((today.month, today.day) <
                       (birthdate.month, birthdate.day))

        # Calculate the difference in years from the date object's components
        year_difference = today.year - birthdate.year

        # The difference in years is not enough.
        # To get it right, subtract 1 or 0 based on if today precedes the
        # birthdate's month/day.

        # To do this, subtract the 'one_or_zero' boolean
        # from 'year_difference'. (This converts
        # True to 1 and False to 0 under the hood.)
        age = year_difference - one_or_zero

        return age

    def create_list_of_answer(self, series):
        # Assuming that the "measure_level" column is part of the series
        return series.apply(lambda x: x.split(';') if isinstance(x, str) else [x])

    def check_max_answer_allowed(self, series, json_response_df):
        curr_max_allowed = json_response_df.loc[json_response_df['name'] == series.name, 'answer_number'].values[0]
        curr_max_allowed = int(curr_max_allowed)
        answers_list = self.create_list_of_answer(series)
        for answers in enumerate(answers_list):
            if len(answers) > curr_max_allowed:
                return True
        return False

    def split_and_clean(self, curr_list_answers, token=r'[,=]'):
        curr_list_answers_list = list(
            curr_list_answers.str.split(token).values)
        curr_list_answers_list = list(
            map(str.strip, [item for sublist in curr_list_answers_list for item in sublist]))
        return curr_list_answers_list

    def validate_uploaded_data(self, series, json_response_df):
        return self.check_max_answer_allowed(series, json_response_df)

    def check_file_content(self, url, data):
        """
        Validates the content of the uploaded CSV file against the metadata manager:
        1. Ensures that the column names in the CSV file match the expected variable names in the metadata manager.
        If there's a mismatch, the file will not be processed further.
        2. For each column that matches a variable name:
        - Verifies the data type to ensure it aligns with what's expected (e.g., categorical, ordinal, numeric, boolean, text).
        - Checks if the number of answers provided in any row does not exceed the maximum allowed for that variable.
        - Ensures that values are not empty or null. 
        If any of these checks fail, the file is considered invalid. The validation has a list of errors that will be displayed to the user once the validation is complete.
        """
        import requests
        import json
        import pandas as pd

        response = requests.get(url)
        json_response = json.loads(response.text)
        json_response_df = pd.DataFrame(json_response)
                
        errors = []  # List to collect errors
    
        
        for column_name, series in data.iteritems():
            print("Processing.. " + column_name)
            if column_name not in json_response_df['name'].values:
                errors.append(f"File has unrecognised column(s): {column_name}")
            else:
                custom_verification = self.validate_uploaded_data(series, json_response_df[json_response_df['name'] == column_name])
                if custom_verification:  # If there's an error
                    errors.append(f"File is not valid for column: {column_name}")

        if errors:
            for error in errors:
                print(error)
            return True  # File contains errors
        else:
            print("File is valid")
            return False  # File is valid
    def download_script(self, folder_path, file_name):
        """
        Download python file needed to trigger the calculation of the latent variable
        """

        import os
        import boto3
        from botocore.config import Config

        s3 = boto3.resource('s3',
                            endpoint_url=self.__OBJ_STORAGE_URL__,
                            aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID__,
                            aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET__,
                            config=Config(signature_version='s3v4'),
                            region_name=self.__OBJ_STORAGE_REGION__)

        path_to_file = file_name.split(
            "s3a://" + self.__OBJ_STORAGE_BUCKET__ + "/")[1]
        path_to_download = os.path.join(
            folder_path, os.path.basename(file_name))

        # Download script with defintion of latent variables calculation
        try:
            s3.Bucket(self.__OBJ_STORAGE_BUCKET__).download_file(
                path_to_file, path_to_download)
            print(
                f"File '{path_to_file}' downloaded successfully to '{path_to_download}'.")
        except Exception as e:
            print(f"Error downloading file: {e}")

    def create_command(self, key, row, variables):
        """
        Create the command which will trigger the corresponding script to run
        with correctly sent parameters
        """

        create_command = ["python", key]
        for var in variables:
            create_command.extend(["--"+var, str(row[var])])
        return create_command

    def calculate_latent_variables(self, columns, data):
        """
        Perform the calculation of the latent variables which are determine by the data
        sent as an input csv file to process.
        """
        import subprocess
        import shutil
        import os

        # Get all variables according to columns from csv
        variables = self.get_all_variables_from_columns(columns)

        if variables:
            # Get all latent variables according to fetched variables
            latent_variables_to_calculate = self.get_latent_variables_info(
                variables)

            latent_to_variables_mapping = {}
            # Map variables to format -> latent_variable_calculation_file_name : list of variables needed for calculation
            for elem in latent_variables_to_calculate:
                variables = variables[elem['variable_id']]
                key = os.path.basename(elem['formula'])
                if key not in latent_to_variables_mapping:
                    latent_to_variables_mapping[key] = variables
                else:
                    latent_to_variables_mapping[key].append(variables)

            # Path to download script to calculate corresponding latent variable
            folder_script_path = "mescobrad_edge/plugins/questionnaire_anonymisation_plugin/latent_calc/"
            os.makedirs(folder_script_path, exist_ok=True)

            # Extract columns used for calculations
            # Perform result element by element
            # Final result add to the dataframe

            for lvar in latent_variables_to_calculate:
                key = os.path.basename(elem['formula'])
                self.download_script(folder_script_path, lvar['formula'])
                result_column = []
                for index, row in data.iterrows():
                    # Create the correct subprocess call
                    command = self.create_command(
                        folder_script_path+key, row, latent_to_variables_mapping[key])
                    # Execute the calculation of the corresponding latent variable
                    try:
                        result = subprocess.run(
                            command, capture_output=True, text=True, check=True)
                        result_column.append(result.stdout.strip())
                    except subprocess.CalledProcessError as e:
                        print("Error:", e)

                # Add the new latent variable and it's corresponding values into initial dataframe
                data[lvar['name']] = result_column
            # Remove downloaded scripts
            shutil.rmtree(folder_script_path)

        return data

    def get_all_variables_from_columns(self, columns):

        import json
        import requests

        or_params = []
        for elem in columns:
            or_params.append("name.eq."+elem)
        paramquery = ','.join(or_params)

        # Get variables by names in columns
        latent_join_table_url = "https://api-metadata.mescobrad.digital-enabler.eng.it/variables"
        latent_join_table_response = requests.get(latent_join_table_url, params={
                                                  "select": "*,variables_variables!fk_latent_variable(*)", "or": "("+paramquery+")", "variables.order": "variable_order"})
        latent_join_json = json.loads(latent_join_table_response.text)

        variables = {}
        # Map in format -> latent_variable_id : list of variables needed for calculation
        for elem in latent_join_json:
            variable_name = elem['name']
            latent_variable_list = elem['variables_variables']

            for element in latent_variable_list:
                latent_variable_id = element['latent_variable_id']
                if latent_variable_id not in variables:
                    variables[latent_variable_id] = []
                variables[latent_variable_id].append(variable_name)

        return variables

    def get_latent_variables_info(self, latent_variables):

        import json
        import requests

        or_params = []
        for elem in latent_variables.keys():
            or_params.append("variable_id.eq."+elem)
        paramquery = ','.join(or_params)

        # Get all latent variables and path to the file needed for calculation
        latent_join_table_url = "https://api-metadata.mescobrad.digital-enabler.eng.it/variables"
        latent_join_table_response = requests.get(latent_join_table_url, params={
                                                  "or": "("+paramquery+")", "formula": "neq.null"})
        latent_join_json = json.loads(latent_join_table_response.text)

        return latent_join_json

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import os
        import pandas as pd
        import hashlib
        import requests
        import json
        import boto3
        from botocore.client import Config

        # Init client
        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        """Remove all personal information from data."""
        files_to_anonymize = input_meta.file_name
        files_content = []
        file_path_template = "./{filename}"
        folder = "anonymous_data"
        if not os.path.isdir(folder):
            os.mkdir(folder)

        # Get the list of variables indicating columns that needs to be removed
        url = "https://api-metadata.mescobrad.digital-enabler.eng.it/variables"
        response = requests.get(url, params={'personaldata': "eq.True"})
        json_response = json.loads(response.text)
        columns_with_personal_data = [elem['name'] for elem in json_response]

        final_files_to_anonymize = []
        # load input data
        for file_name in files_to_anonymize:
            columns_to_remove = []
            data = pd.read_csv(file_path_template.format(filename=file_name))

            if self.check_file_content(url, data):
                # If file is not valid delete file
                s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                                "csv_data/"+file_name).delete()
                os.remove(file_path_template.format(filename=file_name))
                continue
            else:
                file_name_base, file_extension = os.path.splitext(file_name)

                # Processed files will be stored in the parquet format
                file_name_parquet = file_name_base + ".parquet"
                final_files_to_anonymize.append(file_name_parquet)

                # Remove downloaded csv file
                os.remove(file_path_template.format(filename=file_name))

                # Remove csv from the bucket
                s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                                "csv_data/"+file_name).delete()

            columns_to_remove = [
                column for column in data.columns if column in columns_with_personal_data]

            if "date_of_birth" in data.columns:
                data["date_of_birth"] = pd.to_datetime(
                    data["date_of_birth"], dayfirst=True)

            # Generate list of unique ids
            personal_data = data.loc[:, columns_to_remove]
            if "date_of_birth" in personal_data.columns:
                personal_data['date_of_birth'] = personal_data['date_of_birth'].dt.strftime(
                    "%d-%m-%Y")
            list_id = []

            for i in range(data.shape[0]):
                personal_id = "".join(personal_data.iloc[i].astype(str))
                id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
                list_id.append(id)

            data.insert(0, "PID", list_id)
            data.to_parquet(file_path_template.format(
                filename=file_name_parquet), index=False)

            if 'date_of_birth' in personal_data.columns:
                data['age'] = data["date_of_birth"].apply(self.age)

            # remove columns with personal information from the CSV files
            data.drop(columns=columns_to_remove, inplace=True)
            file_path = folder + "/" + file_name_parquet
            data.to_parquet(file_path, index=False)

        # return anonymized data
        return PluginActionResponse("text/csv", files_content, final_files_to_anonymize)
