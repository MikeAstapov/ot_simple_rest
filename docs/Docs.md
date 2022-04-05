**OT\_REST**

**1.** Назначение  
**2.** Структура  
**3.** Описание пакетов и модулей  
**4.** Эндпоинты
**5.** Ролевая модель в БД
<br>
<br>
---
<br>

**1. Назначение**

Приложение OT\_REST представляет собой веб-сервис, реализующий функционал для выполнения OTL запросов, получения результатов, сохранения их в кэш и выгрузки из кэша.

Текущая версия сервиса реализована с использованием python веб-фреймворка Tornado, позволяющего осуществлять асинхронную обработку запросов.
<br>
<br>
---
<br>


**2. Структура**

Структура приложения включает в себя:

1. Основной пакет приложения ot\_simple\_rest:
    - Исполняемый модуль **ot\_simple\_rest.py** , в котором происходит создание экземпляра приложения и запуск веб-сервера Tornado;
    - Пакет **handlers** , содержащий модули с реализованными хэндлерами для обработки запросов;
    - Пакет **jobs\_manager** , который отвечает за последовательное выполнение запросов типа makejob во избежание одновременных обращений к БД;
    - Пакет **parsers**. Здесь собраны парсеры для обработки OTL;
    - Пакет **task\_scheduler** - планировщик периодических вспомогательных задач;
    - Пакет **utils** , содержащий вспомогательные инструменты;
    - Конфигурационный файл;
2. Директория **docs** - содержит документацию по проекту.
3. Директория **nginx_example_configs** - ориентировочные конфигурационные файлы для NGINX.
4. Директория **sql** - включает в себя скрипты для создания БД.
5. Пакет **tests** - юнит-тесты.
<br>
---
<br>


**3. Описание пакетов и модулей**  

**1** Пакет ot\_simple\_rest  

**1.1.** Модуль **ot\_simple\_rest.py**

Это основной модуль приложения, который является точкой входа для запуска.

В нём последовательно выполняются: настройка журналирования (logging),  считывание файла конфигурации, создание экземпляра job-менеджера и его запуск, создание экземпляра приложения Tornado Application, конфигурация эндпоинтов и запуск приложения на веб-сервере Tornado с привязкой к указанному сетевому порту.  
<br>

**1.2.** Пакет **ot\_simple\_rest/jobs\_manager**

Пакет содержит модули jobs.py и manager.py реализующие, соответственно, классы заданий и менеджера для управления созданием и запуском заданий.

1.2.1 Модуль **ot\_simple\_rest/jobs\_manager/jobs.py**

В данном модуле реализован класс **Job** , позволяющий создавать задания для OT\_Dispatcher, проверять статус заданий и получать результаты выполнения.

Класс **Job** принимает следующие аргументы при создании экземпляра:

<pre>
<b>id</b> - идентификатор экземпляра джоба (например, handler_id);
<b>request</b> — объект запроса, полученный в хэндлере;  
<b>db_conf</b> — настройки подключения к БД;  
<b>mem_conf</b> — настройки хранилища кэша;  
<b>resolver_conf</b> — настройки для класса Resolver из пакета parsers/OTL_resolver;  
<b>tracker_max_interval</b> — размер интервала между проверками статуса OT_Dispatcher;  
<b>check_index_access</b> — флаг необходимости проверки доступа пользователя к индексам;  
</pre>

Список методов класса **Job** :

- _**check\_dispatcher\_status()**_ — проверка статуса диспетчера через обращение к БД;
- _**load\_and\_send\_from\_memcache(cid)**_ — данные из кэша по идентификатору _cid_;
- _**check\_cache( cache\_ttl, original\_OTL, tws, twf, field\_extraction, preview )**_ — проверка наличия в БД записи о существовании кэша задания с указанными параметрами;
- _**check\_running( original\_OTL, tws, twf, field\_extraction, preview )**_ — проверка наличия в БД записи о запущенном задании с указанными параметрами;
- _**get\_request\_params**_ — извлекает из объекта запроса (request) параметры OTL - запроса, необходимые для дальнейших манипуляций;
- _**start\_make**_ — проверяет существование кэша (check\_cache) и запущенного задания (check\_running). Если проверки дают отрицательный результат, создаёт новое задание в БД для OT\_Dispatcher с параметрами, полученными из get\_request\_params;
- _**start\_load**_ — проверяет статус задания с параметрами, полученными из get\_request\_params;

<br>


1.2.2 Модуль **ot\_simple\_rest/jobs\_manager/manager.py**

Данный модуль содержит реализацию класса **JobsManager** , предназначенного для инкапсуляции методов для работы с объектами класса **Job**. В частности, для запуска заданий разных типов.

Текущая реализация использует асинхронную очередь для обработки заданий типа MakeJob, которые создаются и помещаются в очередь менеджером. Менеджер также отслеживает появление заданий в этой очереди и последовательно запускает их. Данный механизм позволяет избежать коллизий при выполнении проверок в БД.

Класс **JobManager** принимает следующие аргументы при создании экземпляра:

<pre>
<b>db_conf</b> — настройки подключения к БД;  
<b>mem_conf</b> — настройки хранилища кэша;  
<b>disp_conf</b> — настройки OT\_Dispatcher;  
<b>resolver_conf</b> — настройки для класса Resolver из пакета parsers/OTL\_resolver;  
<b>user_conf</b> — параметры, касающиеся пользователей;  
</pre>

Список методов класса **JobManager** :

- **make\_job** — создание задания типа MakeJob;
- **load\_job** — создание и запуск задания типа LoadJob;
- **check\_job** - создание и запуск задания типа CheckJob;
- **start** — запуск менеджера;
- **stop** — остановка менеджера;

<br>


**1.3.** Пакет **ot\_simple\_rest/handlers**

В данном пакете содержатся все хэндлеры приложения OT\_REST.

Распределение хэндлеров по назначению осуществляется путём размещения их во вложенных пакетах с соответствующими названиями.

<br>

**3.1** Пакет **ot\_simple\_rest/handlers/jobs**

Содержит модули, имеющие отношение к работе с джобами.

<br>

3.1.1 Модуль **ot\_simple\_rest/handlers/jobs/makejob.py**

Обрабатывает POST - запросы на эндпоинт **/makejob.**  
Перед созданием задания выполняется проверка доступа пользователя к индексам, присутствующим в запросе, поэтому данный эндпоинт требует, чтобы пользователь был авторизован.  
В случае успешного выполнения проверки задание типа MakeJob помещается в очередь и возвращается ответ со статусом.   
<br>
Стоит пояснить, почему задания данного типа помещаются в очередь, а не создаются напрямую.  
Дело в том, что в алгоритме задания MakeJob присутствует вставка в базу данных. Одновременное выполнение одинаковых запросов на /makejob может привести к ситуации, когда из-за одновременных вставок одних и тех же данных в БД произойдёт конфликт (данные должны быть уникальны).  
<br>
В связи с этим задания MakeJob помещаются в очередь, из которой они извлекаются по мере поступления и запускаются. Это позволяет избежать проблем при вставке записей в БД.  
<br>
Ещё одна особенность, вытекающая из использования очереди - возникновение задержек между отправкой запроса на /makejob и непосредственным запуском самого задания MakeJob.  
В связи с этим запрос на /checkjob может какое-то время возвращать статус "notfound". Поэтому **важно обрабатывать эту ситуацию**, понимая её причину.


Варианты response:
<pre>
{"status": "fail", "error": "User has no access to index"}
{"status": "success", "timestamp": "2020-02-10 12:00:00"}  
</pre>

<br>

3.1.2 Модуль **ot\_simple\_rest/handlers/jobs/checkjob.py**

Обрабатывает GET - запросы на эндпоинт **/checkjob.**  
Данный тип заданий служит для проверки статуса запущенного джоба.   
В результате создаётся и запускается задание типа LoadJob, результат которого возвращается в виде response. В ответе содержится статус джоба, сообщение об ошибке (если произошла), идентификатор кэша (если завершился успешно).  

Варианты response:

<pre>
{"status": "nocache", 'error': 'No cache for this job'} — кэш не найден  
{"status": "new" / "running"} — новое/запущенное задание  
{"status": "failed / "canceled", "error": msg} — задание завершилось с ошибкой, либо отменено  
{"status": "notfound", "error": "Job is not found"} — задание не найдено в БД  
{"status": "success", "cid": 12} - задание успешно завершено
</pre>

<br>

3.1.3 Модуль **ot\_simple\_rest/handlers/jobs/loadjob.py**

Обрабатывает GET - запросы на эндпоинт **/loadjob.**  
Отличие данного типа заданий от /checkjob заключается в том, что в ответе помимо статуса возвращаются и результаты проверяемого джоба (в случае его успешного завершения).  
В результате запроса создаётся и запускается задание типа LoadJob, результат которого возвращается в виде response. В ответе содержится статус задания, сообщение об ошибке (если произошла), результат выполнения джоба (если завершился успешно).

Варианты response:

<pre>
{"status": "nocache", 'error': 'No cache for this job'} — кэш не найден  
{"status": "new" / "running"} — новое/запущенное задание  
{"status": "failed / "canceled", "error": msg} — задание завершилось с ошибкой, либо отменено  
{"status": "notfound", "error": "Job is not found"} — задание не найдено в БД  
{"status": "success", "schema": "%s, events: {…}} - задание успешно завершено
{"status": "running", "notifications": [{"code": int, "value": value or None}, ...]} - ключ notification появляется только в том случае, когда есть о чем уведомлять пользователя. {"code": int, "value": value or None} - один элемент и списка уведомлений, где code - код уведомления, а value - дополнительная информация или None, если она не нужна.
</pre>

<br>

3.1.4 Модуль **ot\_simple\_rest/handlers/jobs/getresult.py**

Обрабатывает GET - запросы на эндпоинт **/getresult.**  
Принимает на вход параметр cid - идентификатор кэша, полученный в результате успешного /checkjob.  
Результатом является список относительных http-ссылок на статические файлы, которые отдаются веб-сервером NGINX (или другим средством отдачи статики).

Варианты response:

<pre>
{'status': 'failed', 'error': 'No cache with id=cid — кэш не найден в файловой системе  
{"status": "success", "data_urls": [url1, url2, ...]} - успешное выполнение   
{"status": "success", "schema": "%s", "events": {...}} - успешное выполнение (без использования NGINX, данные отдаются по старой схеме через memcache)  
</pre>

<br>

3.1.5 Модуль **ot\_simple\_rest/handlers/jobs/saveotrest.py**

Обрабатывает POST - запросы на эндпоинт **/otrest.** Предоставляет механизм записи результатов работы приложения OTLunk (ot\_simple) в RAM-кэш.
<br>

Варианты response:

<pre>
{"_time": creating_date, "status": "success", "job_id": cache_id} - успешное выполнение  
{"status": "fail", "error": "Validation failed"} - ошибка валидации  
</pre>
<br>

3.1.6 Модуль **ot\_simple\_rest/handlers/jobs/db\_connector.py**

В данном модуле реализованы классы коннекторов к БД, предоставляющие API для выполнения всех необходимых операций с БД. В качестве входного параметра коннкетор принимает ConnectionPool, избавляющий от необходимости создавать новое подключение при каждом новом обращении к эндпоинтам сервиса.

<br>
<br>

**3.3** Пакет **ot\_simple\_rest/handlers/eva**

Содержит хэндлеры, связанные с авторизацией, ролевой моделью и дашбордингом.  

3.3.1 Модуль **ot\_simple\_rest/handlers/eva/base.py**  

Содержит класс базового хэндлера, от которого наследуется большинство хэндлеров, требующих авторизации.  
Здесь реализованы методы для валидации JWT-токена и декодирования JSON-содержимого запроса.    


3.3.2 Модуль **ot\_simple\_rest/handlers/eva/auth.py**  

Обрабатывает POST - запросы на /auth/login.  

Механизм авторизации использует сессии, хранящиеся в БД. В сессию включается JWT-токен, в котором содержится информация о пользователе.  
Если в запросе на авторизацию есть cookie с токеном, выполняется проверка наличия сессии с таким токеном в БД.  
Если в запросе отсутствует токен, либо в БД уже нет соответствующей сессии, генерируется новый токен и сессия, которая записывается в БД, после чего клиенту возвращается cookie с токеном.  

Пример запроса:

<pre>
POST json_data = {"username": "user", "password": "8-symbol"}
Response: {'status': 'success'}
</pre>
<br>

3.3.3 Модуль **ot\_simple\_rest/handlers/eva/dashs.py** 

- **DashboardHandler**: Содержит методы для работы с объектом дашборда.  
Методы: создание (POST), удаление (DELETE), получение (GET) и изменение (PUT) дашборда.  

Примеры запросов:  
<pre>
POST json_data = {"name": "dash_name", "body": "{json_data}", "groups": ["group1, group2, ..."]}
Response: {'id': dash_id, 'modified': modified}

PUT json_data = {"id": dash_id, "name": "dash_name", "body": "{json_data}", "groups": ["group1, group2, ..."]}
Response: {'id': dash_id, 'name': name, 'modified': modified}

GET params = {"id": dash_id}
Response: {'data': dash_data, 'groups': all_groups}

DELETE params = {"id": dash_id}
Response: {'id': dash_id}
</pre>

- **DashboardsHandler**: Содержит метод для получения списка дашбордов (GET).  

Примеры запросов:  
<pre>
GET params = {"id": group_id, "names_only": 1}
Response: {'data': dashs_data_for_this_group}

GET without_params
Response: {'data': all_dashs_data, "names_only": 1}
</pre>
Если указан параметр names_only, вернутся только названия дашбордов.
<br>

3.3.4 Модуль **ot\_simple\_rest/handlers/eva/db_connector.py**  

В данном модуле реализован класс коннектора к БД, предоставляющий API для выполнения всех необходимых операций с БД.  
В качестве входного параметра коннкетор принимает ConnectionPool, избавляющий от необходимости создавать новое подключение при каждом новом обращении к эндпоинтам сервиса.  
<br>

3.3.5 Модуль **ot\_simple\_rest/handlers/eva/logs.py**

Хэндлер для передачи логов с клиентской части на серверную.  
Данные записываются на сервере в отдельно сформированный файл для каждого клиента.  

Примеры запросов:  
<pre>
POST json_data = {"logs": "text_of_logs"}
Response: {'status': 'success'}
</pre>
<br>

3.3.6 Модуль **ot\_simple\_rest/handlers/eva/role\_model.py**

В данном модуле собраны следующие хэндлеры:  
- **UserHandler**: Содержит методы для работы с объектом пользователя.  
Методы: создание (POST), удаление (DELETE), получение (GET) и изменение (PUT) пользователя.  

Примеры запросов:  
<pre>
POST json_data = {"name": "user_name", "password": "user_pass"}
Response: {'id': user_id}

PUT json_data = {"id": user_id, "name": "new_user_name", "password": "new_user_pass", "roles": ["role1, role2, ..."], "groups": ["group1, group2, ..."]}
Response: {'id': user_id}

GET params = {"id": user_id}
Response: {'data': user_data}

DELETE params = {"id": user_id}
Response: {'id': user_id}
</pre>
<br>

- **UsersHandler**: Содержит метод для получения списка пользователей (GET).  

Примеры запросов:  
<pre>
GET params = {"names_only": 1}
Response: {'data': users_data}
</pre>
Если указан параметр names_only, вернутся только имена пользователей.  
Список пользователей в ответе зависит от прав доступа пользователя, от имени которого был отправлен запрос.  
<br>

- **RoleHandler**: Содержит методы для работы с объектом роли.  
Методы: создание (POST), удаление (DELETE), получение (GET) и изменение (PUT) роли.  

Примеры запросов:  
<pre>
POST json_data = {"name": "role_name", "users": ["user_1", "user_2", ...], "permissions": ["permission_1", "permission_2", ...]}
Response: {'id': role_id}

PUT json_data = {"id": role_id, "name": "new_role_name", "users": ["user_1", "user_2", ...], "permissions": ["permission_1", "permission_2", ...]}
Response: {'id': role_id}

GET params = {"id": role_id}
Response: {'data': role_data, 'users': all_users, 'permissions': all_permissions}

DELETE params = {"id": role_id}
Response: {'id': role_id}
</pre>
<br>

- **RolesHandler**: Содержит метод для получения списка ролей (GET).  

Примеры запросов:  
<pre>
GET params = {"id": 1, "names_only": 1}
Response: {'data': roles_data}
</pre>
\* id - идентификатор пользователя, для которого нужно получить список ролей.  

Если указан параметр names_only, вернутся только названия ролей.  
Список ролей в ответе зависит от прав доступа пользователя, от имени которого был отправлен запрос.  
<br>

- **GroupHandler**: Содержит методы для работы с объектом групп.  
Методы: создание (POST), удаление (DELETE), получение (GET) и изменение (PUT) группы.  

Примеры запросов:  
<pre>
POST json_data = {"name": "group_name", "color": "color_code" "users": ["user_1", "user_2", ...], "dashs": ["dash_1", "dash_2", ...], "indexes": ["index_1", "index_2", ...]}
Response: {"id": group_id}

PUT json_data = {"id": group_id, "name": "group_name", "color": "color_code" "users": ["user_1", "user_2", ...], "dashs": ["dash_1", "dash_2", ...], "indexes": ["index_1", "index_2", ...]}
Response: {"id": group_id}

GET params = {"id": group_id}
Response: {"data": group_data, "users": all_users, "indexes": all_indexes, "dashs": all_dashs}

DELETE params = {"id": group_id}
Response: {"id": group_id}
</pre>
<br>

- **GroupsHandler**: Содержит метод для получения списка групп (GET).  

Примеры запросов:  
<pre>
GET params = {"id": 1, "names_only": 1}
Response: {'data': groups_data}
</pre>
\* id - идентификатор пользователя, для которого нужно получить список групп.  

Если указан параметр names_only, вернутся только названия групп.  
Список групп в ответе зависит от прав доступа пользователя, от имени которого был отправлен запрос.  
<br>

- **PermissionHandler**: Содержит методы для работы с объектом привилегии.  
Методы: создание (POST), удаление (DELETE), получение (GET) и изменение (PUT) привелигий.  

Примеры запросов:  
<pre>
POST json_data = {"name": "permission_name", "roles": ["role_1", "role_2", ...]}
Response: {"id": permission_id}

PUT json_data = {"id": permission_id, "name": "permission_name", "roles": ["role_1", "role_2", ...]}
Response: {"id": permission_id}

GET params = {"id": permission_id}
Response: {"data": permission_data, "roles": all_roles}

DELETE params = {"id": permission_id}
Response: {"id": permission_id}
</pre>
<br>

- **PermissionsHandler**: Содержит метод (GET) для получения списка привилегий.  

Примеры запросов:  
<pre>
GET params = {"id": 1, "names_only": 1}
Response: {'data': permissions_data}
</pre>
\* id - идентификатор пользователя, для которого нужно получить список привилегий.  

Если указан параметр names_only, вернутся только названия привилегий.   
<br>

- **IndexHandler**: Содержит методы для работы с объектом индекса.  
Методы: создание (POST), удаление (DELETE), получение (GET) и изменение (PUT) индексов.  

Примеры запросов:  
<pre>
POST json_data = {"name": "index_name", "groups": ["group_1", "group_2", ...]}
Response: {"id": index_id}

PUT json_data = {"id": index_id, "name": "index_name", "groups": ["group_1", "group_2", ...]}
Response: {"id": index_id}

GET params = {"id": index_id}
Response: {"data": index_data, "groups": all_roles}

DELETE params = {"id": index_id}
Response: {"id": index_id}
</pre>
<br>

- **IndexesHandler**: Содержит метод для получения списка индексов (GET).  

Примеры запросов:  
<pre>
GET params = {"id": 1, "names_only": 1}
Response: {'data': indexes_data}
</pre>
\* id - идентификатор пользователя, для которого нужно получить список индексов.  

Если указан параметр names_only, вернутся только названия индексов.   
<br>

- **UserPermissionsHandler**: Содержит метод (GET) для получения списка привилегий пользователя.  

Примеры запросов:  
<pre>
GET params = {}
Response: {'data': ["permission_1", "permission_2", ...]}
</pre>
<br>

- **UserGroupsHandler**: Содержит метод (GET) для получения списка групп пользователя.  

Примеры запросов:  
<pre>
GET params = {"user_id": 1, "names_only": 1}
Response: {'data': user_groups_data}
</pre>
<br>

- **UserDashboardsHandler**: Содержит метод (GET) для получения списка дашбордов пользователя.  

Примеры запросов:  
<pre>
GET params = {"names_only": 1}
Response: {'data': user_dashs_data}
</pre>
Результат запроса будет зависеть от привилегий пользователя.  
Так, для администратора вернутся все существующие дашборды.  
<br>

- **GroupDashboardsHandler**: Содержит метод (GET) для получения списка дашбордов, принадлежащих группе.  

Примеры запросов:  
<pre>
GET params = {"id": group_id}
Response: {'data': group_dashs_data}
</pre>
<br>

**3.3** Пакет **ot\_simple\_rest/handlers/service**

Содержит вспомогательные хэндлеры, не имеющие прямого отношения к работе с джобами.

3.2.1 Модуль **ot\_simple\_rest/handlers/service/makedatamodels.py**

Обрабатывает POST - запросы на эндпоинт **/makedatamodels.** Реализует механизм создания моделей данных в БД.

Варианты response:

<pre>
{"status": "ok"}
</pre>
<br>

3.2.2 Модуль **ot\_simple\_rest/handlers/service/makerolemodels.py**

Обрабатывает POST - запросы на эндпоинт **/makerolemodels.** Реализует механизм создания ролевых моделей в БД.

Варианты response:

<pre>
{"status": "ok"}
</pre>
<br>

3.3.3 Модуль **handlers/service/pingpong.py**

Обрабатывает GET/POST - запросы на эндпоинт **/ping** , позволяя проверить работоспособность сервера. Отвечает на запросы простым текстовым сообщением.

Варианты response:

<pre>
{'response': 'pong'}
</pre>
<br>
<br>

**4.** Пакет **parsers**

Содержит код, так или иначе связанный с парсингом текста OTL - запросов и его преобразованиями.

**4.1** Пакет **OTL\_resolver**

4.1.1 Модуль **OTL\_resolver/Resolver.py**

Содержит класс Resolver, предоставляющий методы для осуществления преобразований оригинального OTL - запроса к виду, необходимому для выполнения обработки в OT\_Dispatcher.

Класс **Resolver** принимает следующие аргументы при создании экземпляра:

<pre>
<b>indexes</b> — список индексов;  
<b>tws</b> — начало временного окна;  
<b>twf</b> — конец временного окна;  
<b>db</b> — объект коннектора к БД;  
<b>sid</b> — идентификатор запроса;  
<b>src_ip</b> — IP-адрес клиента;  
<b>no_subsearch_commands</b> — список команд, содержащих подзапросы, которые не нужно запускать;
</pre>

Список методов класса **Resolver** :

- _**create\_subsearch**_ — вычленяет подзапросы и выполняет преобразование вида:

<pre>
any_command [subsearch] -&gt; any_command subsearch=subsearch_id;
</pre>

- _**create\_otrest**_ — выполняет преобразование вида:

<pre>
| otrest endpoint=/any/path/to/api/ -&gt; | otrest subsearch=subsearch_id;
</pre>

- _**hide\_subsearch\_before\_read(query)**_ — выполняет замену subsearch в query на пустую строку и возвращает пару query, subsearch;
- _**create\_read\_graph**_ — Находит в запросе конструкции вида &quot;search \_\_fts\_query\_\_&quot; и выполняет преобразование вида:

<pre>
search __fts_query__ -&gt; read "{__fts_json__}";
</pre>

- _**create\_otstats\_graph**_ — Находит в запросе конструкции вида &quot;search \_\_fts\_query\_\_&quot; и выполняет преобразование вида:

<pre>
search __fts_query__ -&gt; otstats "{__fts_json__}";
</pre>

- _**create\_filter\_graph**_ — Находит в запросе конструкции вида &quot;search \_\_filter\_query\_\_&quot; и выполняет преобразование вида:

<pre>
search __filter_query__ -&gt; filter "{__filter_json__}";
</pre>

- _**create\_inputlookup\_filter**_ — Находит в запросе конструкции вида &quot;search \_\_otinputlookup\_query\_\_&quot; и выполняет преобразование вида:

<pre>
search __otinputlookup_query__ -&gt; otinputlookup "{__otinputlookup_json__}";
</pre>

- _**create\_datamodels**_ — выполняет преобразование вида:

<pre>
"| otfrom datamodel __NAME__" to "| search (index=__INDEX__ source=__SOURCE__)";
</pre>

- _**create\_otloadjob\_id**_ — выполняет преобразование вида:

<pre>
"| otloadjob __SID__" to "| otloadjob subsearch="subsearch___sha256__";
</pre>

- _**create\_otloadjob\_OTL**_ — выполняет преобразование вида:

<pre>
| otloadjob OTL="__OTL__" ___token___="__TOKEN__" ___tail___="__OTL__"
</pre>

to

<pre>
| otloadjob subsearch="subsearch___sha256__";
</pre>

<br>
<br>


**4.2** Пакет **OTL\_to\_sparksql**

4.2.1 Модуль **OTL\_to\_sparksql/internal/expressions/baseEvalExpression.py**

Содержит класс **BaseEvalExpression** с набором методов для преобразования синтаксиса OTL-выражений в SQL-выражения.

Список методов класса **BaseEvalExpression:**

- _**OTL\_replace\_case**_ — преобразует все логические выражения в UpperCase;
- _**OTL\_replace\_ws\_with\_and**_ — заменяет пробелы логическим AND;
- _**OTL\_preprocess\_request**_ — вызывает _OTL\_replace\_case &amp; OTL\_replace\_ws\_with\_and;_
- _**remove\_index**_ — удаляет из OTL индексы и сохраняет информацию об индексах в переменной;
- _**transform\_equal**_ - преобразует выражения равенства;
- _**transform\_not\_equal**_ — преобразует выражения неравенства;
- _**transform\_end**_ — преобразует выражения AND;
- _**transform\_or**_ — преобразует выражения OR;
- _**transform\_not**_ — преобразует выражения NOT;
- _**transform\_comparison**_ — преобразует выражения сравнения;
- _**transform\_quotes**_ — преобразует выражения в кавычках;
- _**transform\_brackets**_ — преобразует выражения в скобках;
- _**transform\_comma**_ — преобразует выражения, разделённые запятой;
- _**return\_value**_ — преобразует значение с необязательным регулярным выражением в OTL формат;
- _**return\_string**_ — преобразует строку с необязательными подстроками _′\_raw like′_ или _′\_raw rlike′_

<br>

4.2.2 Модуль **OTL\_to\_sparksql/internal/grammar.py**

В этом модуле описана LALR грамматика для синтаксического разбора OTL-запросов.

<br>

4.2.3 Модуль **OTL\_to\_sparksql/internal/timerange.py**

Здесь содержится класс **Timerange** , имеющий два метода:

- _get\_timestamp(time) —_ извлекает из строки time информацию о дате-времени и преобразует её в значение unix timestamp;
- _removetime(OTL, tws, twf) —_ удаляет из строки OTL информацию о дате-времени и возвращает преобразованную строку и значения tws, twf, преобразованные в unix timestamp;

<br>

4.2.4 Модуль **OTL\_to\_sparksql/OTLunk\_parser.py**

Содержит класс **OTLtoSQL** , выполняющий парсинг и преобразование OTL-запросов в SQL-запросы. Имеются два метода для парсинга запросов двух типов:

- _parse\_read(OTL, av\_indexes, tws, twf)_ — функция для парсинга запросов типа read. Возвращает JSON-объект, в котором ключами являются индексы (в строковом представлении), а значениями словари вида {′query′:…, ′tws′:…, ′twf′:…};
- _parse\_filter(OTL)_ — функция для парсинга запросов типа filter. Возвращает словарь с полями query (строка с SQL-запросом) и fields (список полей);
<br>
<br>
---
<br>

**4. Эндпоинты**  

**/api/ping —** ′pong′ проверка работоспособности сервера;  
**/api/makejob —** создание нового задания;  
**/api/loadjob (\*deprecated) —** проверка статуса созданного задания и получение результатов выполнения;  
**/api/checkjob —** проверка статуса созданного задания;  
**/api/getresult —** получение результатов выполнения задания;  
**/api/otrest —** сохранение результатов в RAM-cache;  
**/api/makedatamodel —** добавление моделей данных в БД;  
**/api/makerolemodel —** добавление ролевых моделей в БД;  
  
**/api/auth/login —** авторизация;  
**/api/logs/save —** сохранения логов от клиента;  

**/api/users —** получение списка пользователей;  
**/api/user -** операции с пользователем;  
**/api/user/groups -** получение списка групп пользователя;  
**/api/user/roles -** получение списка ролей пользователя;
**/api/user/permissions -** получение списка привилегий пользователя;  
**/api/user/dashs -** получение списка дашбородов пользователя;  
**/api/user/users -** получение списка пользователей;  
**/api/user/indexes -** получение списка индексов, доступных пользователю;  

**/api/groups -** получение списка групп;
**/api/group -** операции с группой;
**/api/group/dashs -** получение дашбордов, относящихся к группе;  

**/api/roles -** получение списка ролей;
**/api/role -** операции с ролью;  

**/api/permissions -** получение списка привилегий;  
**/api/permission -** операции с привилегией;

**/api/indexes -** получение списка индексов;  
**/api/index -** операции с индексом;  

**/api/dashs -** получение списка дашбордов;  
**/api/dash -** операции с дашбордом;  
<br>

**5. Ролевая модель в БД**

<pre>
CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL UNIQUE,
    password VARCHAR(512) NOT NULL
);

CREATE TABLE role (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE dash (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    body TEXT,
    modified TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE permission (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE "group" (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    color VARCHAR(100) NOT NULL
);

CREATE TABLE index (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE session (
    id SERIAL PRIMARY KEY,
    token TEXT UNIQUE,
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    expired_date TIMESTAMPTZ NOT NULL
);

CREATE TABLE user_role (
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    role_id INT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
    CONSTRAINT user_role_id UNIQUE(user_id, role_id)
);

CREATE TABLE index_group (
    index_id INT NOT NULL REFERENCES index(id) ON DELETE CASCADE,
    group_id INT NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    CONSTRAINT index_group_id UNIQUE(index_id, group_id)
);

CREATE TABLE user_group (
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    group_id INT NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    CONSTRAINT user_group_id UNIQUE(user_id, group_id)
);

CREATE TABLE dash_group (
    dash_id INT NOT NULL REFERENCES dash(id) ON DELETE CASCADE,
    group_id INT NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    CONSTRAINT dash_group_id UNIQUE(dash_id, group_id)
);

CREATE TABLE role_permission (
    permission_id INT NOT NULL REFERENCES permission(id) ON DELETE CASCADE,
    role_id INT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
    value BOOLEAN,
    CONSTRAINT role_permission_id UNIQUE(permission_id, role_id)
);
</pre>