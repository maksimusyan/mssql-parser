// Скрипт парсинга дамп-файла базы MS SQL
'use strict';

// Устанавливаем константы
const
    dbFile = 'db/test.sql', // дамп-файл с данными из MS SQL
    fs = require('fs'), // модуль работы с файловой системой
    pg = require('pg'), // модуль работы с PostgreSQL
    xml2js = require('xml2js').parseString // модуль преобразования XML в объект JS
;

let 
    // Строка из файл для парсинга с убранными переносами строки
    clearStr,
    // Загружаем файл для парсинга
    file = fs.readFileSync(dbFile, 'utf8'),
    // Строка, относящаяся к таблице TestAnswers
    match,
    // Регулярка для поиска строк TestAnswers
    pattern = /(INSERT \[dbo\]\.\[TestAnswers\].*?<\/Test>)/gi,
    // Все найденные xml-тесты
    xmls = []
;

// Очищаем строку от \n
clearStr = file.replace(/\n/g,' ');

// Ищем все INSERT для TestAnswers
while (match = pattern.exec(clearStr)) {

    // Внутри INSERT ищем xml-структуру
    let subMatch = /(<Test.*?<\/Test>)/gi,
        xml = match[0].match(subMatch);

    // Добавляем xml-структуру в общий массив xml-тестов
    xmls.push(xml[0]);
}

// Если массив xml-тестов не пустой
if(xmls.length > 0){
    // Проходим в цикле каждый Тест
    for(let index in xmls){
        // Преобразуем xml-строку в объект JS
        xml2js(xmls[index], function (err, result) {
            // Атрибуты корневого элемента доступны через ключ $
            let 
                // ID в базе данных при сохранении Теста
                testID,
                // Название Теста
                testName = result.Test.$.name,
                // Код Теста
                testCode = result.Test.$.code,
                // Какой-то username Теста
                testUsername = result.Test.$.username
            ;
            
            // TODO: Тут сделать подключение к базе и сделать INSERT для Теста
            // testID = 1;

            // Если массив вопросов существует и он не пустой
            if(typeof result.Test.Questions !== 'undefined' && result.Test.Questions.length > 0){
                // Проходим в цикле каждый Вопрос
                for(let iQ in result.Test.Questions){
                    // question - это массив с одгним элементом, где:
                    // question[0].$ - объект корневых атрибутов (значения, выбранные пользователем)
                    // question[0].Answers - массив доступных вопросов
                    let question = result.Test.Questions[iQ].Question;

                    // Предполагаю, что каких-то данных не существует. Проверяем:
                    if (typeof question[0] === 'object' && typeof question[0].Answers === 'object'){
                        let
                            srcAnswer = question[0].$.answer.split('@#@'),
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


                        //console.log(resultAnswerValue);
                    }



                }
            }


            
        });
    }
}

console.log('OK');