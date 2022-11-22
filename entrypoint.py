from mescobrad_edge.plugins.edge_plugin_anonymize.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
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

        """Remove all personal information from data."""
        files_to_anonymize = input_meta.file_name
        files_content = []
        folder = "anonymous_data"
        if not os.path.isdir(folder):
            os.mkdir(folder)

        columns_to_remove = ["name", "surname"]
        # load input data
        for file_name in files_to_anonymize:
            data = pd.read_csv("./" + file_name)
            if "date_of_birth" in data.columns:
                columns_to_remove.append("date_of_birth")
                data["date_of_birth"] = pd.to_datetime(data["date_of_birth"], dayfirst=True)

            # Generate list of unique ids
            personal_data = data.loc[:, columns_to_remove]
            if "date_of_birth" in personal_data.columns:
                personal_data['date_of_birth'] = personal_data['date_of_birth'].dt.strftime("%d-%m-%Y")
            list_id = []

            for i in range(data.shape[0]):
                personal_id = "".join(personal_data.iloc[i])
                id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
                list_id.append(id)

            data.insert(0, "PID", list_id)
            if 'date_of_birth' in personal_data.columns:
               data['age'] = data["date_of_birth"].apply(self.age)

            data.to_csv("./" + file_name, index=False)

            # remove columns with personal information from the CSV files
            data.drop(columns=columns_to_remove, inplace=True)

            file_path = folder + "/" + file_name
            data.to_csv(file_path, index=False)

        # return anonymized data
        return PluginActionResponse("text/csv", files_content, files_to_anonymize)
