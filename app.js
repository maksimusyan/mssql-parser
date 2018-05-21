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
                    // ID Исследования в базе
                    researchID,
                    // ID организации, которой принадлежит исследование
                    organisationID,
                    // ID сценария для выбранного исследования
                    scenarioID,
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
                    //getResearchIdQuery = `
                        //SELECT 
                            //sq.scenario_id, s.name
                        //FROM 
                            //scenarios_questions sq 
                        //LEFT JOIN 
                            //scenarios s ON s.id=sq.scenario_id 
                        //WHERE sq.text LIKE 'Я считаю справедливым%';
                    //`
                ;
                /**
                    1. По кодовому слову определяешь id сценария
                    Цикл 1, создание сессий
                    1.5. В researches_sessions вставляешь новую строку, получаешь id сессии
                    Цикл 2, перебор вопросов анкеты
                    2. По тексту вопроса определяешь id вопроса
                    3. Выбираешь варианты ответов на вопрос, если они есть, по id вопроса
                    4. По атрибуту answer определяешь текст и позицию варианта ответа, а по ним id варианта в новой базе
                    5. Делаешь вставку в researches_data данных ответа на текущий вопрос
                    Конец цикла 2
                    6. Обновляешь запись о сессии, добавляя время окончания
                    Конец цикла 1
                */

                // Ищем в базе ID исследования по кодовому слову
                dbClient.query(getResearchIdQuery)
                    .then(res => {
                        if (typeof res.rows[0] !== 'undefined' && typeof res.rows[0].id === 'number'){
                            researchID = res.rows[0].id;
                            organisationID = res.rows[0].organisation_id;
                            scenarioID = res.rows[0].scenario_id;
                            //console.log(res.rows[0]);
                        }
                    })
                    .catch(err => {
                        // Освобождаем пул соединений от нашего клиента
                        dbClient.release();
                        console.log(err.message);
                    })

                // Если массив вопросов существует и он не пустой
                if (typeof result.Test.Questions !== 'undefined' && result.Test.Questions.length > 0) {
                    // Проходим в цикле каждый Вопрос
                    for (let iQ in result.Test.Questions) {
                        // question - это массив с одгним элементом, где:
                        // question[0].$ - объект корневых атрибутов (значения, выбранные пользователем)
                        // question[0].Answers - массив доступных вопросов
                        let question = result.Test.Questions[iQ].Question;

                        // Предполагаю, что каких-то данных не существует. Проверяем:
                        if (typeof question[0] === 'object' && typeof question[0].Answers === 'object') {
                            // Если ответа нет или он пустой, то присваиваем пустую строку
                            let srcAnswer;
                            if (typeof question[0].$.answer !== 'undefined' && question[0].$.answer !== ''){
                                srcAnswer = question[0].$.answer.split('@#@');
                            } else {
                                srcAnswer = ['',''];
                            }
                            let
                                // ???
                                questMinimum = question[0].$.minimum,
                                // ???
                                questMaximum = question[0].$.maximum,
                                // ???
                                questIsSecret = question[0].$.isSecret,
                                // Значение выбранного ответа
                                resultAnswerValue = srcAnswer[0],
                                // ID выбранного ответа
                                resultAnswerId = srcAnswer[1],
                                // Тип вопроса
                                questType = question[0].$.type,
                                // Позиция вопроса
                                questPosition = question[0].$.position,
                                // Текст вопроса
                                questText = question[0].$.text
                            ;

                            // Если варианты ответов существуют
                            if (typeof question[0].Answers[0].Answer === 'object' && question[0].Answers[0].Answer.length > 0){
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
            });
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
