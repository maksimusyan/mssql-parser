// Скрипт парсинга дамп-файла базы MS SQL
'use strict';

// Устанавливаем константы
const
    dbFile = 'db/mssql.sql', // дамп-файл с данными из MS SQL
    fs = require('fs'), // модуль работы с файловой системой
    db = require('./pg_db'), // модуль работы с PostgreSQL
    v8 = require('v8'), // модуль движка V8
    xml2js = require('xml2js').parseString // модуль преобразования XML в объект JS
;

// Устанавливаем максимально допустимое использование памяти в мегабайтах
v8.setFlagsFromString('--max_old_space_size=4096');


let 
    // Текущий индекс в массиве xml-тестов
    currentTestId = 0,
    // Маркер начала склеивания xml-строк
    joinStarted = false,
    // Регулярка для поиска INSERT для TestAnswers
    patternSearch = /INSERT \[dbo\]\.\[TestAnswers\]/,
    // Регулярка для поиска начала xml-строк
    patternStart = /<Test xmlns/,
    // Регулярка для поиска окончания xml-строк
    patternEnd = /<\/Test>/,
    // Маркер начала поиска xml-строк, в случае, если встретился INSERT для TestAnswers
    searchActive = false,
    // Все найденные xml-тесты
    xmls = [],
    // Клиент подключения к базе
    dbClient = null,
    // Функция разбора дамп-файла на строки и парсинга данных
    parseFile = function(){
        // Читаем дамп-файл асинхронно
        fs.readFile(dbFile, { encoding: 'utf8' }, function (err, data) {
            if (err) throw err; // Выкидываем исключение в случае ошибки
            // Прочитанный файл разбиваем на строки и прогоняем в цикле каждую строку
            data.split('\n').forEach(function (line) {
                if (searchActive === false) {
                    // Если нужный INSERT ещё не найден, то находим и включаем поиск xml-структуры
                    if (patternSearch.exec(line) !== null) {
                        searchActive = true;
                    }
                } else {
                    // Если склеивание xml-строк не началось, то ищем начало xml-структуры
                    if (joinStarted === false) {
                        if (patternStart.exec(line) !== null) {
                            joinStarted = true;
                            // Добавляем первую строку в текущий индекс массива xml-тестов
                            xmls[currentTestId] = line;
                        }
                    } else { // Склеивание xml-строк началось
                        // Если найден конец xml-структуры
                        if (patternEnd.exec(line) !== null) {
                            // Дописываем последнюю строку в xml-структуру
                            xmls[currentTestId] += '</Test>';
                            // Прерываем склеивание строк
                            joinStarted = false;
                            // Прерываем поиск xml, чтобы искать следующий INSERT
                            searchActive = false;
                            // Автоикремент текущего индекса массива xml-тестов
                            currentTestId++;
                        } else {
                            // Добавляем текущую строку в текущий индекс массива xml-тестов
                            xmls[currentTestId] += line;
                        }
                    }

                }

            });
            // Готовые данные отправляем на экспорт
            //xmlToJsObject(xmls);
            xmlToJsObject([xmls[0]]);
            //console.log(xmls.length);
        });
    },
    xmlToJsObject = function (xmls){
        // Если массив xml-тестов пустой
        if (xmls.length === 0) return false;
        // Проходим в цикле каждый Тест
        for (let index in xmls) {
            // Преобразуем xml-строку в объект JS
            xml2js(xmls[index], function (err, result) {
                // Атрибуты корневого элемента доступны через ключ $
                let
                    // Название Исследования
                    testName = result.Test.$.name,
                    // Код Исследования
                    testCode = result.Test.$.code,
                    // Какой-то username Исследования
                    testUsername = result.Test.$.username,
                    // Запрос на получение ID исследования и ID сценария
                    getResearchIdQuery = `
                        SELECT 
                            r.id, r.organisation_id, rs.scenario_id
                        FROM 
                            researches r 
                        LEFT JOIN 
                            researches_scenarios rs ON r.id=rs.research_id 
                        WHERE r.password='${testCode}';
                    `
                ;

                // Если массив вопросов существует и он не пустой
                if (typeof result.Test.Questions !== 'undefined' && result.Test.Questions.length > 0) {
                    // Ищем в базе ID исследования по кодовому слову
                    dbClient.query(getResearchIdQuery)
                        .then(res => {
                            // Если исследование найдено
                            if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number') {
                                let 
                                    // ID Исследования в базе
                                    researchID = res.rows[0].id,
                                    // ID организации, которой принадлежит исследование
                                    organisationID = res.rows[0].organisation_id,
                                    // ID сценария для выбранного исследования
                                    scenarioID = res.rows[0].scenario_id
                                ;
                                // Создаём сессию
                                dbClient.query(`
                                        INSERT INTO researches_sessions (
                                            research_id,
                                            scenario_id,
                                            user_id,
                                            date_start,
                                            date_finish,
                                            password,
                                            matrix_id,
                                            code_word
                                        ) 
                                        VALUES ($1, $2, $3, $4, $5, researches_sessions_generate_id(), $6, $7)
                                        RETURNING *;`,
                                        [
                                            researchID,
                                            scenarioID,
                                            null,
                                            new Date(),
                                            null,
                                            null,
                                            null,

                                        ]
                                    )
                                    .then(res => {
                                        // Если сессия создана
                                        if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                                            // Параметры текущего исследования
                                            let researchKeys = {
                                                researchID: researchID,
                                                organisationID: organisationID,
                                                scenarioID: scenarioID,
                                                sessionID: res.rows[0].id
                                            };
                                            // Вызываем функцию обработки вопросов
                                            setQuestionsData(result.Test.Questions, researchKeys);
                                        }
                                    })
                                    .catch(err => {
                                        // Освобождаем пул соединений от нашего клиента
                                        dbClient.release();
                                        console.log(err.message);
                                    })

                                //console.log(res.rows[0]);
                            }
                        })
                        .catch(err => {
                            // Освобождаем пул соединений от нашего клиента
                            dbClient.release();
                            console.log(err.message);
                        })
                }
            });
        }
    },
    setQuestionsData = function (questions, researchKeys){
        if (typeof questions !== 'object' || questions.length === 0){
            return false;
        }
        // Проходим в цикле каждый Вопрос
        for (let iQ in questions) {
            // question - это массив с одгним элементом, где:
            // question[0].$ - объект корневых атрибутов (значения, выбранные пользователем)
            // question[0].Answers - массив доступных вопросов
            let question = questions[iQ].Question;

            // Предполагаю, что каких-то данных не существует. Проверяем:
            if (typeof question[0] === 'object' && typeof question[0].Answers === 'object') {
                // Если ответа нет или он пустой, то присваиваем пустую строку
                let srcAnswer;
                if (typeof question[0].$.answer !== 'undefined' && question[0].$.answer !== '') {
                    srcAnswer = question[0].$.answer.split('@#@');
                } else {
                    srcAnswer = ['', ''];
                }
                let
                    // ???
                    questMinimum = typeof question[0].$.minimum !== 'undefined' ? question[0].$.minimum : null,
                    // ???
                    questMaximum = typeof question[0].$.maximum !== 'undefined' ? question[0].$.maximum : null,
                    // ???
                    questIsSecret = typeof question[0].$.isSecret !== 'undefined' ? question[0].$.isSecret : null,
                    // Значение выбранного ответа
                    resultAnswerValue = srcAnswer[0],
                    // ID выбранного ответа
                    resultAnswerId = srcAnswer[1],
                    // Тип вопроса
                    questType = question[0].$.type,
                    // Позиция вопроса
                    questPosition = typeof question[0].$.position !== 'undefined' ? question[0].$.position : 0,
                    // Текст вопроса
                    questText = question[0].$.text,
                    // Текст вопроса, подготовленный для поиска в базе
                    questTextModify = questText.replace(/<br\/>/g, '\\n').replace(/&lt;br\/&gt;/g, '\\n')
                ;

                // Если варианты ответов существуют
                if (typeof question[0].Answers[0].Answer === 'object' && question[0].Answers[0].Answer.length > 0) {
                    for (let iAns in question[0].Answers[0].Answer) {
                        let ansData = question[0].Answers[0].Answer[iAns],
                            // ???
                            ansValue = ansData._,
                            // ???
                            ansPosition = ansData.$.position,
                            // ???
                            ansKeyto = ansData.$.keyto
                            ;
                        console.log(ansValue);
                    }
                }
            }
        }
    }
;

// Подключаемся к пулу клиентов PG
db.pool.connect()
    // Если подключение прошло успешно, то пул вернёт нам Клиента
    .then(client => {
        // Передаём клиента базы в глобальную переменную
        dbClient = client;
        // Запускаем парсер
        parseFile();
    }).catch(() => {
        console.log('Error connect');
    });
