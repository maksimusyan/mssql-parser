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
    // Массив секретных вопросов, на которые ответил пользователь
    secretQuestions = {},
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
                if (typeof result.Test.Questions !== 'undefined' && typeof result.Test.Questions[0] !== 'undefined' && typeof result.Test.Questions[0].Question !== 'undefined') {
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
                                    scenarioID = res.rows[0].scenario_id,
                                    // Время старта сессии
                                    sessionStartDate = new Date()
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
                                            sessionStartDate,
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
                                                research_id: researchID,
                                                organisation_id: organisationID,
                                                scenario_id: scenarioID,
                                                session_id: res.rows[0].id,
                                                session_date: sessionStartDate
                                            };
                                            // Создаём ключ текущей сессии для массива секретных вопросов
                                            secretQuestions[res.rows[0].id] = {};
                                            // Вызываем функцию обработки вопросов
                                            setQuestionsData(result.Test.Questions[0].Question, researchKeys);
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
                    for (let iSa in srcAnswer){
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
                    questPosition = typeof question.$.position !== 'undefined' ? question.$.position : '0',
                    // Текст вопроса
                    questText = question.$.text,
                    // Текст вопроса, подготовленный для поиска в базе
                    questTextModify = questText.replace(/<br\/>/g, '\\n').replace(/&lt;br\/&gt;/g, '\\n')
                ;

                // Если вопрос не секретный или открыт конкретный секретный вопрос
                if (questIsSecret === false || secretQuestions[researchKeys.session_id][questPosition] === questPosition){
                    // Ищем вопрос и варианты ответов в базе по тексту вопроса
                    dbClient.query(`
                        SELECT 
                            sq.id, sqa.id as answer_id, sqa.text as answer_text, sqa.value as answer_value
                        FROM 
                            scenarios_questions sq
                        LEFT JOIN 
                            scenarios_questions_answers sqa ON sqa.question_id=sq.id 
                        WHERE 
                            sq.scenario_id='${researchKeys.scenario_id}' 
                        AND 
                            sq.text='${questTextModify}'
                    `)
                    .then(res => {
                        // Если вопрос найден
                        if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                            let 
                                questionID = res.rows[0].id, // ID вопроса
                                answerResultValue = '', // Итоговые значения ответов пользователя (через запятую)
                                answerResultIds = ''; // Итоговые ID ответов пользователя (через запятую)
                            // Ищем в ответах из базы тот, который выбрал пользователь
                            for (let iAns in res.rows){
                                let curDbAnswer = res.rows[iAns];
                                for (let irA in resultAnswers){
                                    // Если текст ответа пользователя совпадает с текстом ответа в базе
                                    if (resultAnswers[irA] === curDbAnswer.answer_text){
                                        if (answerResultValue === ''){
                                            answerResultValue = curDbAnswer.answer_value;
                                        } else {
                                            answerResultValue += ','+curDbAnswer.answer_value;
                                        }
                                        if (answerResultIds === ''){
                                            answerResultIds = curDbAnswer.answer_id;
                                        } else {
                                            answerResultIds += ',' + curDbAnswer.answer_id;
                                        }
                                    }
                                }
                            }
                            if (answerResultIds === '') {
                                answerResultIds = null;
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
                                    researchKeys.session_id,
                                    questionID,
                                    answerResultValue,
                                    answerResultIds,
                                    researchKeys.session_date
                                ]
                            )
                            .then(res => {
                                // Если данные успешно сохранены
                                if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                                    successInsert++;
                                }
                                // Если цикл завершился, то закрываем сессию
                                if (counter === questions.length - 1) {
                                    console.log(res.rows[0]);
                                    console.log('Success: ' + successInsert);
                                    console.log('Error: ' + errorInsert);
                                    sessionClose(researchKeys);
                                }
                            })
                            .catch(err => {
                                console.log(err.message);
                                errorInsert++;
                                // Если цикл завершился, то закрываем сессию
                                if (counter === questions.length - 1) {
                                    console.log('Success: ' + successInsert);
                                    console.log('Error: ' + errorInsert);
                                    sessionClose(researchKeys);
                                }
                            })
                        }
                    })
                    .catch (err => {
                        console.log(err.message);
                    })
                }
                
                // Если варианты ответов существуют
                if (typeof question.Answers[0].Answer === 'object' && question.Answers[0].Answer.length > 0) {
                    for (let iAns in question.Answers[0].Answer) {
                        let ansData = question.Answers[0].Answer[iAns],
                            // ???
                            ansValue = ansData._,
                            // ???
                            ansPosition = ansData.$.position,
                            // ???
                            ansKeyto = ansData.$.keyto
                        ;
                        if (parseInt(ansKeyto) > 0){
                            for (let iFA in resultAnswers) {
                                // Если текст ответа пользователя совпадает с текстом ответа в вариантах
                                if (resultAnswers[iFA] === ansValue) {
                                    secretQuestions[researchKeys.session_id][ansKeyto] = ansKeyto;
                                    console.log('SECRET: '+ansKeyto);
                                }
                            }
                        }
                    }
                }
                /**/
            }
        }
    },
    sessionClose = function (researchKeys){
        let sessionEndDate = researchKeys.session_date;
        sessionEndDate.setSeconds(sessionEndDate.getSeconds() + 1);
        // Удаляем секретные вопросы текущей сессииы
        delete secretQuestions[researchKeys.session_id];
        // Обновляем данные сессии
        dbClient.query(`
            UPDATE researches_sessions
            SET date_finish = $1
            WHERE id = $2`, [sessionEndDate,researchKeys.session_id]
        )
        .then(res => {
            // Если данные успешно сохранены
            if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number' && res.rows[0].id > 0) {
                console.log('Session closed!');
            }
            // Освобождаем пул соединений от нашего клиента
            dbClient.release();
        })
        .catch(err => {
            console.log(err.message);
            // Освобождаем пул соединений от нашего клиента
            dbClient.release();
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
