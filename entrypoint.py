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
            data = pd.read_csv("./" + file_name)

            if self.check_file_content(url, data.columns):
                # If file is not valid delete file
                s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__, "personal_data/"+file_name).delete()
                os.remove("./" + file_name)
                continue
            else:
                final_files_to_anonymize.append(file_name)

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
            if 'Question_date_of_birth' in personal_data.columns:
               data['age'] = data["Question_date_of_birth"].apply(self.age)

            data.to_csv("./" + file_name, index=False)

            # remove columns with personal information from the CSV files
            data.drop(columns=columns_to_remove, inplace=True)

            file_path = folder + "/" + file_name
            data.to_csv(file_path, index=False)

        # return anonymized data
        return PluginActionResponse("text/csv", files_content, final_files_to_anonymize)
