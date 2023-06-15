from mescobrad_edge.plugins.questionnaire_anonymisation_plugin.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
from datetime import date

class GenericPlugin(EmptyPlugin):

    def age(self, birthdate):
        # Get today date
        today = date.today()

        # A bool that represents if today's day/month precedes the birth day/month
        one_or_zero = ((today.month, today.day) < (birthdate.month, birthdate.day))

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

    def check_file_content(self, url, columns):
        """If the name of the columns in the uploaded csv file are not consisted with
        the name of variables in metadata manager, file shouldn't be processed.
        """
        import requests
        import json

        response = requests.get(url)
        json_response = json.loads(response.text)
        metadata_variables = [elem['name'] for elem in json_response]

        for column in columns:
            if column not in metadata_variables:
                return True

        return False

    def download_script(self, folder_path, file_name):
        """
        Download python file needed to trigger the calculation of the latent variable
        """

        import os
        import boto3
        from botocore.config import Config

        s3 = boto3.resource('s3',
                    endpoint_url= self.__OBJ_STORAGE_URL__,
                    aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID__,
                    aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET__,
                    config=Config(signature_version='s3v4'),
                    region_name=self.__OBJ_STORAGE_REGION__)

        path_to_download = os.path.join(folder_path, file_name)

        # Download script with defintion of latent variables calculation
        # TO DO - Determine where exactly files will be placed within Data Lake
        s3.Bucket(self.__OBJ_STORAGE_BUCKET__).download_file("test_latent_calc/"+file_name, path_to_download)

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
        import json
        import requests
        import subprocess
        import shutil
        import os

        # Find the latent variables if there is reference field different from None
        url = "https://api-metadata.mescobrad.digital-enabler.eng.it/variables"
        # TO DO -- reference field doesn't exist currently
        # response = requests.get(url, params={"reference":"neq.null"})
        # json_response = json.loads(response.text)
        # Latent variables are retrieved
        # latent_variables = [[elem['name'], elem['reference']] for elem in json_response]

        # Retrieve the latent variables which uses the variables from columns in csv
        # TO DO -- Retrieve mapping between simple variables and latent variables, ones
        # the table is added

        # TO DO - Remove once entire infrustructure is ready
        variables = {"test_funkcija": ["var1", "var2", "var3"],
                     "test_latent": ["var4", "var5", "var6"]} # this will be the result of the missing part of code

        latent_variables_to_calculate = []

        # Check if all variables needed for calculation of the latent variable is
        # present in the input data, if not calculation can't be performed
        for key in variables:
            if all(element in columns for element in variables[key]):
                latent_variables_to_calculate.append(key)

        # Path to download script to calculate corresponding latent variable
        folder_script_path = "mescobrad_edge/plugins/questionnaire_anonymisation_plugin/latent_calc/"
        os.makedirs(folder_script_path, exist_ok=True)

        # Extract columns used for calculations
        # Perform result element by element
        # Final result add to the dataframe

        for lvar in latent_variables_to_calculate:
            self.download_script(folder_script_path, lvar+".py")
            result_column = []
            for index, row in data.iterrows():
                # Create the correct subprocess call
                command = self.create_command(folder_script_path+lvar+".py", row, variables[lvar])
                # Execute the calculation of the corresponding latent variable
                try:
                    result = subprocess.run(command, capture_output=True, text=True, check=True)
                    result_column.append(result.stdout.strip())
                except subprocess.CalledProcessError as e:
                    print("Error:", e)

            # Add the new latent variable and it's corresponding values into initial dataframe
            data[lvar] = result_column

        # Remove downloaded scripts
        shutil.rmtree(folder_script_path)

        return data

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
                                  endpoint_url= self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
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
        response = requests.get(url, params = {'personaldata': "eq.True"})
        json_response = json.loads(response.text)
        columns_with_personal_data = [elem['name'] for elem in json_response]

        final_files_to_anonymize = []
        # load input data
        for file_name in files_to_anonymize:
            columns_to_remove = []
            data = pd.read_csv(file_path_template.format(filename=file_name))

            if self.check_file_content(url, data.columns):
                # If file is not valid delete file
                s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__, "csv_data/"+file_name).delete()
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
                s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__, "csv_data/"+file_name).delete()

            data = self.calculate_latent_variables(data.columns, data)

            columns_to_remove = [column for column in data.columns if column in columns_with_personal_data]

            if "Question_date_of_birth" in data.columns:
                data["Question_date_of_birth"] = pd.to_datetime(data["Question_date_of_birth"], dayfirst=True)

            # Generate list of unique ids
            personal_data = data.loc[:, columns_to_remove]
            if "Question_date_of_birth" in personal_data.columns:
                personal_data['Question_date_of_birth'] = personal_data['Question_date_of_birth'].dt.strftime("%d-%m-%Y")
            list_id = []

            for i in range(data.shape[0]):
                personal_id = "".join(personal_data.iloc[i].astype(str))
                id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
                list_id.append(id)

            data.insert(0, "PID", list_id)
            data.to_parquet(file_path_template.format(filename=file_name_parquet), index=False)

            if 'Question_date_of_birth' in personal_data.columns:
               data['age'] = data["Question_date_of_birth"].apply(self.age)

            # remove columns with personal information from the CSV files
            data.drop(columns=columns_to_remove, inplace=True)
            file_path = folder + "/" + file_name_parquet
            data.to_parquet(file_path, index=False)

        # return anonymized data
        return PluginActionResponse("text/csv", files_content, final_files_to_anonymize)
