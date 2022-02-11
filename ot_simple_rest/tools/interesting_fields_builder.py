class InterestingFieldsBuilder:

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
            return []
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
