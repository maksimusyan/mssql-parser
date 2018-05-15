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
    for(let index in xmls){
        // Преобразуем xml-строку в объект JS
        xml2js(xmls[index], function (err, result) {
            console.log('======= '+result.Test.$.name+' =========');
            
            if(result.Test.Questions.length > 0){
                for(let iQ in result.Test.Questions){
                    let question = result.Test.Questions[iQ].Question;


                    console.log(question[0].$.answer);

                }
            }


            
        });
    }
}

console.log('OK');