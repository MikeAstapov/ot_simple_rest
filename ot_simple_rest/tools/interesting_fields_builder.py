from .base_builder import BaseBuilder


class InterestingFieldsBuilder(BaseBuilder):

    def __init__(self, mem_conf, static_conf):
        super().__init__(mem_conf, static_conf)

    def _get_fields(self, data):
        if data.empty:
            return []
        # interesting_fields = [{} for _ in range(data.shape[1])]  # to avoid having on dict referenced many times
        interesting_fields = {}
        i = 0
        vcs = {}
        data.dropna(inplace=True)
        for col in data.columns:
            interesting_fields[col] = {'id': i, 'text': data.columns[i], 'totalCount': data.shape[0], 'static': []}
            vcs[col] = data[col].value_counts()
            i += 1
        for k, v in vcs.items():
            for index, val in v.items():
                value = index
                count = val
                interesting_fields[k]['static'].append({
                    'value': value,
                    'count': count,
                    '%': count / data.shape[0] * 100
                })

        return list(interesting_fields.values())

    def test_get_interesting_fields(self, data_path):
        data = self._load_json_lines_test(data_path)
        return self._get_fields(data)


    def get_interesting_fields(self, cid):
        data = self._load_json_lines(cid)
        return self._get_fields(data)
