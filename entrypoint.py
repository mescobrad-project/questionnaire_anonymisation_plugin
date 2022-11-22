from mescobrad_edge.plugins.edge_plugin_anonymize.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata

class GenericPlugin(EmptyPlugin):

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

            # Generate list of unique ids
            personal_data = data.loc[:, columns_to_remove]
            list_id = []
            for i in range(data.shape[0]):
                personal_id = "".join(personal_data.iloc[i])
                id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
                list_id.append(id)
            data.insert(0, "PID", list_id)
            data.to_csv("./" + file_name, index=False)

            # remove columns with personal information from the CSV files
            data.drop(columns=columns_to_remove, inplace=True)

            file_path = folder + "/" + file_name
            data.to_csv(file_path, index=False)

        # return anonymized data
        return PluginActionResponse("text/csv", files_content, files_to_anonymize)