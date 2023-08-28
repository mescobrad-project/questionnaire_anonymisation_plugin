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

    import pandas as pd

    def create_list_of_answer(self, series):
        answers = series.get("measure_level", "")
        return answers.split(',')

    def check_max_answer_allowed(self, series, json_response_df):
        curr_max_allowed = json_response_df.loc[json_response_df['name']
                                                == series.name, 'answer_number'].values[0]
        curr_max_allowed = int(curr_max_allowed)
        answers_list = self.create_list_of_answer(series)
        return curr_max_allowed < len(answers_list)

    def is_numeric_value_in_list(self, data, value):
        for el in data:
            el_str = str(el)
            if el_str.isdigit() and str(value).isdigit():
                if int(value) == int(el_str):
                    return True
        return False

    def split_and_clean(self, curr_list_answers, token=r'[,=]'):
        curr_list_answers_list = list(
            curr_list_answers.str.split(token).values)
        curr_list_answers_list = list(
            map(str.strip, [item for sublist in curr_list_answers_list for item in sublist]))
        return curr_list_answers_list

    def check_data_type(self, series, json_response_df):
        import numpy as np
        curr_data_type = json_response_df.loc[json_response_df['name']
                                              == series.name, 'data_type'].values[0]
        curr_list_answers = json_response_df.loc[json_response_df['name']
                                                 == series.name, 'measure_level']
        curr_data_type = curr_data_type.lower()
        if curr_data_type == "categorical":
            curr_list_answers_list = self.split_and_clean(
                curr_list_answers, r'[,=]')
            for value in series.values:
                if not self.is_numeric_value_in_list(curr_list_answers_list, value):
                    return True
            return False

        elif curr_data_type == "ordinal":
            curr_list_answers_list = self.split_and_clean(
                curr_list_answers, r'[,]')
            for value in series.values:
                if value not in curr_list_answers_list:
                    return True

            return False

        elif curr_data_type == "numeric":
            for value in series.values:
                if not isinstance(value, (int, np.int64)):
                    return True

            return False

        elif curr_data_type == "boolean":
            accepted_boolean_values = ["Yes", "Y", "No", "N"]
            for value in series.values:
                if value not in accepted_boolean_values:
                    return True
            return False

        elif curr_data_type == "text":
            for value in series.values:
                if not isinstance(value, str):
                    return True

        return False

    def custom_verification(self, series, json_response_df):
        return self.check_data_type(series, json_response_df) and self.check_max_answer_allowed(series, json_response_df)

    def check_file_content(self, url, data):
        """If the name of the columns in the uploaded csv file are not consisted with
        the name of variables in metadata manager, file shouldn't be processed.
        """
        import requests
        import json
        import pandas as pd

        response = requests.get(url)
        json_response = json.loads(response.text)
        json_response_df = pd.DataFrame(json_response)

        for column_name, series in data.iteritems():
            if column_name in json_response_df['name'].values:
                custom_verification = self.custom_verification(
                    series, json_response_df[json_response_df['name'] == column_name])
                if custom_verification is True:
                    print("File is not valid")
                    raise ValueError("File is not valid")
            else:
                raise ValueError(
                    "File has unrecognised column(s): " + column_name)
        print("File is valid")
        return False  # File is valid

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
