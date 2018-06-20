// Скрипт парсинга дамп-файла базы MS SQL
// node --max-old-space-size=8192 app.js
'use strict';

// Подключение переменных окружения из конфигурационного файла
require('dotenv').config();

// Устанавливаем константы
const
    dbFile = 'db/mssql.sql', // дамп-файл с данными из MS SQL
    fs = require('fs'), // модуль работы с файловой системой
    db = require('./pg_db'), // модуль работы с PostgreSQL
    v8 = require('v8'), // модуль движка V8
    xml2js = require('xml2js').parseString // модуль преобразования XML в объект JS
    ;

// Устанавливаем максимально допустимое использование памяти в мегабайтах
v8.setFlagsFromString('--max_old_space_size=8192');

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
    // Массив выбранных анкет
    testXmls = [],
    // Клиент подключения к базе
    dbClient = null,
    // Клиент подключения к базе
    dbQueryCounter = 0,
    // Общее кол-во успешных запросов сохранения данных
    allQuerySuccess = 0,
    // Общее кол-во не удачных запросов сохранения данных
    allQueryErrors = 0,
    // Общее кол-во не найденных вопросов
    allQuestionsNotFound = 0,
    // Массив секретных вопросов, на которые ответил пользователь
    secretQuestions = {},
    // Массив кодовых имён исследований
    researchCodes = {},
    // Массив разрешённых кодовых имён исследований
    researchCodesEnabled = {
        'moex2016-1': 1,
        'SDM': 1,
        'TOPSDM': 1,
        'SDM_F': 1,
        'RNB': 1,
        'WSCB': 1,
        'WSCB_01': 1,
        'IskraUralTel': 1,
        'moex2016': 1,
        'beeline2016': 1
    },
    // Если включен режим тестирования кода
    isTest = false,
    // Количество тестируемых Исследований, где: 0 - все анкеты
    testCount = 8200,
    // Сколько анкет пропустить
    testOffset = 0,
    // Функция разбора дамп-файла на строки и парсинга данных
    parseFile = function () {
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

            // Если есть ограничение на кол-во или пропуск анкет
            if (testCount > 0 || testOffset > 0) {
                // Сколько можно забрать анкет
                let maxCount = testCount > 0 ? testCount : xmls.length - testOffset;
                // Прогоняем все анкеты в цикле
                for (let i = 1; i <= xmls.length; i++) {
                    // Если анкета есть в массиве
                    if (typeof xmls[i] !== 'undefined') {
                        // Если номер больше отступа и макc.кол-во не превышено
                        if (i > testOffset && maxCount > 0) {
                            // Добавляем анкету в новый массив
                            testXmls.push(xmls[i]);
                            // Убавляем максимальное кол-во
                            maxCount--;
                        } else {
                            // В противном случае освобождаем память от анкеты
                            delete xmls[i];
                        }
                    }
                }
            } else {
                testXmls = xmls;
            }

            console.log('Всего анкет: ' + testXmls.length)
            // Готовые данные отправляем на экспорт
            xmlToJsObject(testXmls);
        });
    },
    xmlToJsObject = function (xmlsArray) {
        // Если массив xml-тестов пустой
        if (xmlsArray.length === 0) return false;
        // Проходим в цикле каждый Тест
        for (let index in xmlsArray) {
            // Преобразуем xml-строку в объект JS
            xml2js(xmlsArray[index], function (err, result) {
                // Атрибуты корневого элемента доступны через ключ $
                let
                    // Название Исследования
                    testName = result.Test.$.name,
                    // Код Исследования
                    testCode = result.Test.$.code,
                    // Какой-то username Исследования
                    testUsername = result.Test.$.username,
                    // Запрос на получение вопросов исследования и сопутствующих данных по кодовому слову
                    getResearchQuestionsQuery = `
                        SELECT 
                            r.id as research_id, 
                            r.organisation_id, 
                            rs.scenario_id, 
                            sq.id AS sq_id, 
                            sq.text AS sq_text, 
                            sq.secret AS sq_secret, 
                            sq.position AS sq_position
                        FROM 
                            researches r 
                        INNER JOIN 
                            researches_scenarios rs ON r.id=rs.research_id 
                        INNER JOIN 
                            scenarios_questions sq ON rs.scenario_id=sq.scenario_id 
                        WHERE r.password='${testCode}';
                    `
                    ;

                // Если массив вопросов существует и он не пустой
                if (typeof researchCodesEnabled[testCode] !== 'undefined'
                    && typeof result.Test.Questions !== 'undefined'
                    && typeof result.Test.Questions[0] !== 'undefined'
                    && typeof result.Test.Questions[0].Question !== 'undefined') {

                    // Добавляем кодовое слово в общий список
                    researchCodes[testCode] = 1;

                    if (isTest) {
                        // ЗАПУСК ТЕСТА ДАННЫХ
                        let reData = { research_id: 0, organisation_id: 0, scenario_id: 0, session_date: new Date(), questions: [] };
                        setQuestionsData(result.Test.Questions[0].Question, reData);
                    } else {
                        dbClient.query(getResearchQuestionsQuery)
                            .then(res => {
                                // Если вопросы исследования найдены
                                if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].research_id === 'number') {

                                    // EXAMPLE
                                    //res.rows[0] = 
                                    //{
                                    //research_id: 1,
                                    //organisation_id: 1,
                                    //scenario_id: 1,
                                    //sq_id: 1,
                                    //sq_text: 'Текст вопроса',
                                    //sq_secret: false,
                                    //sq_position: 1
                                    //}
                                    //

                                    let
                                        // Параметры текущего исследования
                                        reData = {
                                            // ID Исследования в базе
                                            research_id: res.rows[0].research_id,
                                            // ID организации, которой принадлежит исследование
                                            organisation_id: res.rows[0].organisation_id,
                                            // ID сценария для выбранного исследования
                                            scenario_id: res.rows[0].scenario_id,
                                            // Время старта сессии
                                            session_date: new Date(),
                                            // Все вопросы текущего исследования
                                            questions: res.rows
                                        }
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
                                            reData.research_id,
                                            reData.scenario_id,
                                            null,
                                            reData.session_date,
                                            null,
                                            null,
                                            null,

                                        ]
                                    )
                                        .then(res => {
                                            // Если сессия создана
                                            if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                                                //console.log('session start: '+res.rows[0].id);
                                                // В массив данных исследования помещаем ID текущей сессии
                                                reData.session_id = res.rows[0].id;
                                                // Создаём ключ текущей сессии для массива секретных вопросов
                                                secretQuestions[res.rows[0].id] = {};
                                                // Вызываем функцию обработки вопросов
                                                let curNumber = parseInt(index) + 1;
                                                setQuestionsData(result.Test.Questions[0].Question, reData, curNumber);
                                            }
                                        })
                                        .catch(err => {
                                            // Освобождаем пул соединений от нашего клиента
                                            dbClient.release();
                                            console.log(err.message);
                                        })
                                }
                            })
                            .catch(err => {
                                console.log(err.message);
                            })
                    }
                }
            });
        }
    },
    setQuestionsData = function (questions, reData, curNumber) {
        if (typeof questions !== 'object' || questions.length === 0) {
            console.log('Questions is empty');
            return false;
        }
        let successInsert = 0,
            errorInsert = 0;
        // Проходим в цикле каждый Вопрос
        for (let iQ in questions) {
            // question - это массив с одгним элементом, где:
            // question[iQ].$ - объект корневых атрибутов (значения, выбранные пользователем)
            // question[iQ].Answers - массив доступных вопросов

            let counter = parseInt(iQ),
                question = questions[counter];

            // Предполагаю, что каких-то данных не существует. Проверяем:
            if (typeof question === 'object') {
                // Массив с ответами пользователя
                let resultAnswers = [];
                // Если ответ существует
                if (typeof question.$.answer !== 'undefined') {
                    // Разбиваем строку ответа на разделители множественного ответа (типа SOFT)
                    let srcAnswer = question.$.answer.split('@%@');
                    for (let iSa in srcAnswer) {
                        // Отделяем текст ответа от позиции
                        let ansText = srcAnswer[iSa].split('@#@');
                        resultAnswers.push(ansText[0]);
                    }
                } else {
                    resultAnswers[0] = '';
                }
                let
                    // ???
                    questMinimum = typeof question.$.minimum !== 'undefined' ? question.$.minimum : null,
                    // ???
                    questMaximum = typeof question.$.maximum !== 'undefined' ? question.$.maximum : null,
                    // ???
                    questIsSecret = typeof question.$.isSecret !== 'undefined' && question.$.isSecret === 'true' ? true : false,
                    // Тип вопроса
                    questType = question.$.type,
                    // Позиция вопроса
                    questPosition = typeof question.$.position !== 'undefined' ? parseInt(question.$.position) : 0,
                    // Текст вопроса
                    questText = question.$.text.trim()
                    ;

                // Если в файле с дампом существуют варианты ответов
                if (typeof question.Answers !== 'undefined' && typeof question.Answers[0].Answer === 'object' && question.Answers[0].Answer.length > 0) {
                    for (let iAns in question.Answers[0].Answer) {
                        let ansData = question.Answers[0].Answer[iAns],
                            // Текст варинта ответа
                            ansValue = ansData._,
                            // Позиция варианта ответа
                            ansPosition = ansData.$.position,
                            // Ссылка на секретный вопрос
                            ansKeyto = typeof ansData.$.keyto !== 'undefined' ? parseInt(ansData.$.keyto) : 0
                            ;
                        // Тут мы ищем секретный вопрос, потому что цикл JS идёт быстрее чем запрос к базе
                        if (ansKeyto > 0 && isTest === false) {
                            for (let iFA in resultAnswers) {
                                // Если текст ответа пользователя совпадает с текстом ответа в вариантах
                                if (resultAnswers[iFA] === ansValue) {
                                    secretQuestions[reData.session_id][ansKeyto] = ansKeyto;
                                }
                            }
                        }
                    }
                }
                if (isTest) {
                    //console.log(questText);
                    //console.log('\n');
                } else {
                    // Если вопрос не секретный или открыт конкретный секретный вопрос
                    if (questIsSecret === false || secretQuestions[reData.session_id][questPosition] === questPosition) {
                        // Ищем ID вопроса по Тексту и Позиции вопроса
                        let questionID = 0;
                        for (let iQdb in reData.questions) {
                            if (questionID === 0 && reData.questions[iQdb].sq_text.trim() === questText && reData.questions[iQdb].sq_position === questPosition) {
                                questionID = reData.questions[iQdb].sq_id;
                            }
                        }
                        if (parseInt(questionID) === 0) {
                            allQuestionsNotFound++;
                            //console.log('ВОПРОС НЕ НАЙДЕН:#' + questText + '#');
                        }
                        // Ищем варианты ответов в базе по ID вопроса если текст вопроса найден в базе
                        if (parseInt(questionID) > 0) {
                            dbClient.query(`
                                SELECT 
                                    *
                                FROM 
                                    scenarios_questions_answers
                                WHERE 
                                    question_id='${questionID}';
                            `)
                                .then(res => {
                                    let
                                        answerResultValue = '', // Итоговые значения ответов пользователя (через запятую)
                                        answerResultIds = ''; // Итоговые ID ответов пользователя (через запятую)
                                    // Если варианты ответов найдены
                                    if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                                        // Ищем в ответах из базы тот, который выбрал пользователь
                                        for (let iAns in res.rows) {
                                            let curDbAnswer = res.rows[iAns];
                                            for (let irA in resultAnswers) {
                                                // Если текст ответа пользователя совпадает с текстом ответа в базе
                                                if (resultAnswers[irA] === curDbAnswer.text) {
                                                    if (answerResultValue === '') {
                                                        answerResultValue = curDbAnswer.value;
                                                    } else {
                                                        answerResultValue += ',' + curDbAnswer.value;
                                                    }
                                                    if (answerResultIds === '') {
                                                        answerResultIds = curDbAnswer.id;
                                                    } else {
                                                        answerResultIds += ',' + curDbAnswer.id;
                                                    }
                                                }
                                            }
                                        }
                                        if (answerResultIds === '') {
                                            answerResultIds = null;
                                        }
                                    }

                                    // Сохраняем ответ пользователя
                                    dbClient.query(`
                                        INSERT INTO researches_data (
                                            session_id,
                                            question_id,
                                            answer,
                                            answers_ids,
                                            date_create
                                        ) 
                                        VALUES ($1, $2, $3, $4, $5)
                                        RETURNING *;`,
                                        [
                                            reData.session_id,
                                            questionID,
                                            answerResultValue,
                                            answerResultIds,
                                            reData.session_date
                                        ]
                                    )
                                        .then(res => {
                                            // Если данные успешно сохранены
                                            if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                                                successInsert++;
                                                allQuerySuccess++;
                                            }
                                            // Если цикл завершился, то закрываем сессию
                                            if (counter === questions.length - 1) {
                                                console.log('--------------')
                                                console.log('SESSION: ' + reData.session_id);
                                                console.log('Success: ' + successInsert);
                                                console.log('Error: ' + errorInsert);
                                                console.log('Number: ' + curNumber + '/' + testXmls.length);
                                                console.log('ВОПРОСОВ НЕ НАЙДЕНО: ' + allQuestionsNotFound);
                                                sessionClose(reData);
                                            }
                                        })
                                        .catch(err => {
                                            console.log(err.message);
                                            errorInsert++;
                                            allQueryErrors++;
                                            // Если цикл завершился, то закрываем сессию
                                            if (counter === questions.length - 1) {
                                                console.log('--------------')
                                                console.log('SESSION: ' + reData.session_id);
                                                console.log('Success: ' + successInsert);
                                                console.log('Error: ' + errorInsert);
                                                console.log('Number: ' + curNumber + '/' + testXmls.length);
                                                console.log('ВОПРОСОВ НЕ НАЙДЕНО: ' + allQuestionsNotFound);
                                                sessionClose(reData);
                                            }
                                        })
                                })
                                .catch(err => {
                                    console.log(err.message);
                                })
                        }
                    }
                }
            }
        }

        //console.log('ОБРАБОТАНЫ КОДОВЫЕ СЛОВА:');
        //console.log(researchCodes);

    },
    sessionClose = function (reData) {
        let sessionEndDate = reData.session_date;
        sessionEndDate.setSeconds(sessionEndDate.getSeconds() + 1);
        // Удаляем секретные вопросы текущей сессии
        delete secretQuestions[reData.session_id];
        // Обновляем данные сессии
        dbClient.query(`
            UPDATE researches_sessions
            SET date_finish = $1
            WHERE id = $2`, [sessionEndDate, reData.session_id]
        )
            .then(res => {
                dbQueryCounter++;
                if (dbQueryCounter >= testXmls.length) {
                    console.log('=========  RESULT  ========');
                    console.log('ALL_success: ' + allQuerySuccess);
                    console.log('ALL_error: ' + allQueryErrors);
                    console.log('ВОПРОСОВ НЕ НАЙДЕНО: ' + allQuestionsNotFound);
                    console.log('ОБРАБОТАНЫ КОДОВЫЕ СЛОВА:');
                    console.log(researchCodes);
                    // Освобождаем пул соединений от нашего клиента
                    dbClient.release();
                }
            })
            .catch(err => {
                console.log(err.message);
                dbQueryCounter++;
                if (dbQueryCounter >= testXmls.length) {
                    console.log('=========  RESULT  ========');
                    console.log('ALL_success: ' + allQuerySuccess);
                    console.log('ALL_error: ' + allQueryErrors);
                    console.log('ВОПРОСОВ НЕ НАЙДЕНО: ' + allQuestionsNotFound);
                    console.log('ОБРАБОТАНЫ КОДОВЫЕ СЛОВА:');
                    console.log(researchCodes);
                    // Освобождаем пул соединений от нашего клиента
                    dbClient.release();
                }
            })
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