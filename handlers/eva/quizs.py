import tornado.web

from handlers.eva.base import BaseHandler


class QuizsHandler(BaseHandler):
    async def get(self):
        quizs = self.db.get_quizs_data()
        self.write({'data': quizs})


class QuizHandler(BaseHandler):
    async def get(self):
        quiz_id = self.get_argument('id', None)
        if not quiz_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        try:
            quiz = self.db.get_quiz_data(quiz_id=quiz_id)
        except Exception as err:
            raise tornado.web.HTTPError(400, str(err))
        self.write({'data': quiz})


class QuizCreateHandler(BaseHandler):
    async def post(self):
        quiz_name = self.data.get('name', None)
        questions = self.data.get('questions', None)
        if None in [quiz_name, questions]:
            raise tornado.web.HTTPError(400, "params 'name' and 'questions' is needed")
        try:
            quiz_id = self.db.add_quiz(name=quiz_name,
                                       questions=questions)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': quiz_id})


class QuizDeleteHandler(BaseHandler):
    async def delete(self):
        quiz_id = self.get_argument('id', None)
        if not quiz_id:
            raise tornado.web.HTTPError(400, "params 'id' is needed")
        quiz_id = self.db.delete_quiz(quiz_id=quiz_id)
        self.write({'id': quiz_id})


class QuizSaveHandler(BaseHandler):
    async def get(self):
        quiz_id = self.get_argument('id', None)
        if not quiz_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        try:
            quiz = self.db.get_quiz_data(quiz_id=quiz_id)
        except Exception as err:
            raise tornado.web.HTTPError(400, str(err))
        self.write({'data': quiz})

    async def post(self):
        quiz_name = self.data.get('name', None)
        if not quiz_name:
            raise tornado.web.HTTPError(400, "params 'name' is needed")
        quiz_id = self.db.add_quiz(quiz_name=quiz_name)
        self.write({'id': quiz_id})

