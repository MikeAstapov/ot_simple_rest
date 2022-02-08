from .base_builder import BaseBuilder


class InterestingFieldsBuilder(BaseBuilder):

    def __init__(self, mem_conf, static_conf):
        super().__init__(mem_conf, static_conf)

    @staticmethod
    def _round_percent(percent, length):
        if length > 300:
            percent = round(percent, 2)
        elif 30 < length < 300:
            percent = round(percent, 1)
        else:
            percent = round(percent)
        return percent

    def _get_fields(self, data):
        if data.empty:
            return []
        interesting_fields = {}
        i = 0
        value_counts_columns = {}
        not_nan_for_every_col = data.count()
        for col in data.columns:
            interesting_fields[col] = {'id': i, 'text': col, 'totalCount': not_nan_for_every_col[col], 'static': []}
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

    def test_get_interesting_fields(self, data_path):
        data = self._load_json_lines_test(data_path)
        return self._get_fields(data)


    def get_interesting_fields(self, cid):
        data = self._load_json_lines(cid)
        return self._get_fields(data)
