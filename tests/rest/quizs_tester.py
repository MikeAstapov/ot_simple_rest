import requests


class QuizsTester:
    """
    Test suite for /api/quiz(s) OT_REST endpoints.
    Test cases:
    - add quiz
    - delete quiz
    - edit quiz
    - get quiz
    - get list of quizs
    - get quiz questions
    - fill quiz
    - get filled quiz
    - add catalog
    - delete catalog
    - edit catalog
    - get catalog
    - get list of catalogs
    """

    def __init__(self, api_conf, db):
        self.config = api_conf
        self.db = db
        self.cookies = None

    def _cleanup(self):
        del_quiz_query = "DELETE FROM quiz;"
        del_filled_quiz_query = "DELETE FROM filled_quiz;"
        del_question_query = "DELETE FROM question;"
        del_catalog_query = "DELETE FROM catalog;"
        for query in [del_quiz_query, del_filled_quiz_query, del_question_query, del_catalog_query]:
            self.db.execute_query(query, with_commit=True, with_fetch=False)

    def auth(self):
        data = {'username': 'admin', 'password': '12345678'}
        resp = requests.post(f'http://{self.config["host"]}:{self.config["port"]}/api/auth/login', json=data)
        resp.raise_for_status()
        self.cookies = resp.cookies

    def check_cookies(self):
        if not self.cookies:
            self.auth()

    def send_request(self, *, endpoint, method='GET', data=None):
        methods = {'GET': requests.get,
                   'POST': requests.post,
                   'PUT': requests.put,
                   'DELETE': requests.delete}
        self.check_cookies()
        req_method = methods.get(method)
        if not req_method:
            raise TypeError(f'unknown type of http method: {method}')

        resp = req_method(f'http://{self.config["host"]}:{self.config["port"]}{endpoint}',
                          cookies=self.cookies, json=data)
        resp.raise_for_status()
        return resp.json()

    def test__create_quiz(self):
        data = {'name': 'test_quiz', 'questions': [
            {'type': 'text', 'sid': 1, 'text': 'question_1', 'is_sign': True, 'label': 'sign_question'},
            {'type': 'bool', 'sid': 2, 'text': 'are you ready?', 'is_sign': False, 'label': ''}
        ]}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quiz_data = self.db.execute_query("SELECT id, name FROM quiz;", as_obj=True)
            questions = self.db.execute_query("SELECT type, text, is_sign, label FROM question "
                                              "WHERE quiz_id = %s ORDER BY sid;",
                                              params=(quiz_data.id,), as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return quiz_data.name == data['name'] and questions[0].text == data['questions'][0]['text']

    def test__delete_quiz(self):
        data = {'name': 'test_quiz', 'questions': [
            {'type': 'text', 'sid': 1, 'text': 'question_1', 'is_sign': True, 'label': 'sign_question'}
        ]}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quizs_before = self.db.execute_query("SELECT id FROM quiz;", as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/qapi/quiz/delete?id={quizs_before[0].id}')
            quizs_after = self.db.execute_query("SELECT id FROM quiz;", as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return quizs_after == []

    def test__edit_quiz(self):
        data = {'name': 'test_quiz', 'questions': [
            {'type': 'text', 'sid': 1, 'text': 'question_1', 'is_sign': True, 'label': 'sign_question'}
        ]}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quiz_from_db = self.db.execute_query("SELECT id FROM quiz;", as_obj=True)
            edited_data = {'id': quiz_from_db.id, 'name': 'edited_quiz', 'questions': [
                {'type': 'bool', 'sid': 1, 'text': 'question_1', 'is_sign': True, 'description': None,
                 'catalog_id': None, 'label': 'sign_question'}]}

            self.send_request(method='PUT', endpoint=f'/qapi/quiz/edit', data=edited_data)
            edited_quiz = self.db.execute_query("SELECT id, name FROM quiz;", as_obj=True)
            questions = self.db.execute_query("SELECT type, text, is_sign, label FROM question "
                                              "WHERE quiz_id = %s ORDER BY sid;",
                                              params=(edited_quiz.id,), as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return edited_quiz.name == 'edited_quiz' and questions[0].type == 'bool'

    def test__get_quiz(self):
        data = {'name': 'test_quiz', 'questions': []}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quiz_from_db = self.db.execute_query(f"SELECT id FROM quiz;", as_obj=True)
            quiz_from_api = self.send_request(method='GET', endpoint=f'/qapi/quiz?id={quiz_from_db.id}')
        finally:
            self._cleanup()
        return quiz_from_api['data']['name'] == data['name']

    def test__get_quizs_list(self):
        data = [{'name': 'quiz_1', 'questions': []}, {'name': 'quiz_2', 'questions': []}]
        try:
            for d in data:
                self.send_request(method='POST', endpoint='/qapi/quiz/create', data=d)
            quizs_from_api = self.send_request(method='GET', endpoint=f'/qapi/quizs')
        finally:
            self._cleanup()
        return quizs_from_api['data'][0]['name'] == data[0]['name'] and \
               quizs_from_api['data'][1]['name'] == data[1]['name']

    def test__get_quiz_questions(self):
        data = {'name': 'test_quiz', 'questions': [
            {'type': 'text', 'sid': 1, 'text': 'question_1', 'is_sign': True, 'label': 'sign_question'}
        ]}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quiz = self.db.execute_query(f"SELECT id FROM quiz;", as_obj=True)
            quiz_questions = self.send_request(method='GET', endpoint=f'/qapi/quiz/questions?ids={quiz.id}')
        finally:
            self._cleanup()
        return quiz_questions['data'][0]['questions'][0]['text'] == 'question_1'

    def test__fill_quiz(self):
        data = {'name': 'test_quiz', 'questions': [
            {'type': 'text', 'text': 'question_1', 'is_sign': True, 'label': 'sign_question'}
        ]}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quiz = self.db.execute_query(f"SELECT id FROM quiz;", as_obj=True)
            filled_data = [{'id': quiz.id, 'questions': [
                {'type': 'text', 'text': 'question_1', 'is_sign': True, 'label': 'sign_question',
                 'answer': {'value': 'test'}}
            ]}]
            self.send_request(method='POST', endpoint='/qapi/quiz/filled/save', data=filled_data)
            answer = self.db.execute_query("SELECT value FROM textAnswer;", as_obj=True)
        finally:
            self._cleanup()
        return answer.value == filled_data[0]['questions'][0]['answer']['value']

    def test__get_filled_quizs(self):
        data = {'name': 'test_quiz', 'questions': [
            {'type': 'text', 'text': 'question_1', 'is_sign': True, 'label': 'sign_question'}
        ]}
        try:
            self.send_request(method='POST', endpoint='/qapi/quiz/create', data=data)
            quiz = self.db.execute_query(f"SELECT id FROM quiz;", as_obj=True)
            filled_data_1 = [{'id': quiz.id, 'questions': [
                {'type': 'text', 'text': 'question_1', 'is_sign': True, 'label': 'sign_question',
                 'answer': {'value': 'test_1'}}
            ]}]
            filled_data_2 = [{'id': quiz.id, 'questions': [
                {'type': 'text', 'text': 'question_1', 'is_sign': True, 'label': 'sign_question',
                 'answer': {'value': 'test_2'}}
            ]}]
            self.send_request(method='POST', endpoint='/qapi/quiz/filled/save', data=filled_data_1)
            self.send_request(method='POST', endpoint='/qapi/quiz/filled/save', data=filled_data_2)
            answers = self.db.execute_query("SELECT value FROM textAnswer;", fetchall=True, as_obj=True)
        finally:
            self._cleanup()
        return answers[0].value == filled_data_1[0]['questions'][0]['answer']['value'] and \
               answers[1].value == filled_data_2[0]['questions'][0]['answer']['value']

    def test__create_catalog(self):
        data = {'name': 'test_catalog', 'content': 'first\nsecond\nthird'}
        try:
            self.send_request(method='POST', endpoint='/qapi/catalog/create', data=data)
            catalog_data = self.db.execute_query("SELECT name, content FROM catalog;", as_obj=True)
        finally:
            self._cleanup()
        return catalog_data.name == data['name'] and catalog_data.content == data['content']

    def test__delete_catalog(self):
        data = {'name': 'test_catalog', 'content': 'first\nsecond\nthird'}
        try:
            self.send_request(method='POST', endpoint='/qapi/catalog/create', data=data)
            catalogs_before = self.db.execute_query("SELECT id FROM catalog;", as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/qapi/catalog/delete?id={catalogs_before[0].id}')
            catalogs_after = self.db.execute_query("SELECT id FROM catalog;", as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return catalogs_after == []

    def test__edit_catalog(self):
        data = {'name': 'test_catalog', 'content': 'first\nsecond\nthird'}
        try:
            self.send_request(method='POST', endpoint='/qapi/catalog/create', data=data)
            catalog_from_db = self.db.execute_query("SELECT id FROM catalog;", as_obj=True)
            edited_data = {'id': catalog_from_db.id, 'name': 'edited_catalog', 'content': 'first\nfifth\n'}
            self.send_request(method='PUT', endpoint=f'/qapi/catalog/edit', data=edited_data)
            edited_catalog = self.db.execute_query("SELECT name, content FROM catalog;", as_obj=True)
        finally:
            self._cleanup()
        return edited_catalog.name == edited_data['name'] and edited_catalog.content == edited_data['content']

    def test__get_catalog(self):
        data = {'name': 'test_catalog', 'content': 'first\nsecond\nthird'}
        try:
            self.send_request(method='POST', endpoint='/qapi/catalog/create', data=data)
            catalog_from_db = self.db.execute_query(f"SELECT id FROM catalog;", as_obj=True)
            catalog_from_api = self.send_request(method='GET', endpoint=f'/qapi/catalog?id={catalog_from_db.id}')
        finally:
            self._cleanup()
        return catalog_from_api['data']['name'] == data['name'] and \
               catalog_from_api['data']['content'] == data['content']

    def test__get_catalogs_list(self):
        data = [{'name': 'catalog_1', 'content': 'first\nsecond\nthird'},
                {'name': 'catalog_2', 'content': 'fourth\nfifth\nsixth'}]
        try:
            for d in data:
                self.send_request(method='POST', endpoint='/qapi/catalog/create', data=d)
            catalogs_from_api = self.send_request(method='GET', endpoint=f'/qapi/catalogs')
        finally:
            self._cleanup()
        return catalogs_from_api['data'][0]['name'] == data[0]['name'] and \
               catalogs_from_api['data'][1]['name'] == data[1]['name']

