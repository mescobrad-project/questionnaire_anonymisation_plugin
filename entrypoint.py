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


    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import os
        import pandas as pd
        import hashlib
        import requests
        import json

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

        # load input data
        for file_name in files_to_anonymize:
            columns_to_remove = []
            data = pd.read_csv("./" + file_name)

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
        return PluginActionResponse("text/csv", files_content, files_to_anonymize)
