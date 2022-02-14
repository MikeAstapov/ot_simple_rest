class InterestingFieldsBuilder:
    """
    The builder class is responsible for creating the list of interesting fields from already loaded data.

    interesting fields consist of:
    :id: serial number of a column
    :text: name of a column
    :totalCount: number of not empty cells in the column (null is considered an empty cell)
    :static: list of dictionaries where every dictionary is an info about every unique value in a column consists of:
            :value: value itself
            :count: how many times the value appears in the column
            :%: percent of count from all rows in the data table
    """

    @staticmethod
    def _round_percent(percent, length):
        if length > 300:
            percent = round(percent, 2)
        elif 30 < length < 300:
            percent = round(percent, 1)
        else:
            percent = round(percent)
        return percent

    def get_interesting_fields(self, data):
        if data.empty:
            raise Exception('Empty data')
        interesting_fields = {}
        i = 0
        value_counts_columns = {}
        not_nan_for_every_col = data.count()
        for col in data.columns:
            interesting_fields[col] = {'id': i, 'text': col, 'totalCount': int(not_nan_for_every_col[col]), 'static': []}
            value_counts_columns[col] = data[col].value_counts()
            i += 1
        for col_name, unique_values in value_counts_columns.items():
            for value_as_index, value_counter in unique_values.items():
                value = value_as_index
                count = value_counter
                percent = count / data.shape[0] * 100
                percent = self._round_percent(percent, data.shape[0])
                interesting_fields[col_name]['static'].append({
                    'value': value,
                    'count': count,
                    '%': percent
                })
        return list(interesting_fields.values())
