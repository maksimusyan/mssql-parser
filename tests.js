// Скрипт парсинга исследований в дампе базы MS SQL
'use strict';

// Устанавливаем константы
const
    dbFile = 'db/mssql.sql', // дамп-файл с данными из MS SQL
    resultFile = 'db/tests.xml', // файл с итоговыми данными
    fs = require('fs'), // модуль работы с файловой системой
    v8 = require('v8') // модуль движка V8
;

// Устанавливаем максимально допустимое использование памяти в мегабайтах
v8.setFlagsFromString('--max_old_space_size=4096');

// Подключение переменных окружения из конфигурационного файла
require('dotenv').config();

let 
    // Маркер начала склеивания xml-строк
    joinStarted = false,
    // Регулярка для поиска начала xml-строк
    patternStart = /INSERT \[dbo\]\.\[Tests\] \(.*?<Test/,
    // Регулярка для поиска окончания xml-строк
    patternEnd = /<\/Test>/
;

// Читаем дамп-файл асинхронно
fs.readFile(dbFile, { encoding: 'utf8' }, function(err, data){
    if (err) throw err; // Выкидываем исключение в случае ошибки

    // создаем поток
    let writeStream = fs.createWriteStream(resultFile);
    writeStream.write('<?xml version="1.0" encoding="utf-8"?>\n<root>\n');

    // Прочитанный файл разбиваем на строки и прогоняем в цикле каждую строку
    data.split('\n').forEach(function(line){

        // Если склеивание xml-строк не началось, то ищем начало xml-структуры
        if (joinStarted === false) {
            if (patternStart.exec(line) !== null) {
                joinStarted = true;
                // Добавляем первую строку
                let firstLine = line.split('<Test');
                writeStream.write('<Test' + firstLine[1]);
            }
        } else { // Склеивание xml-строк началось
            // Если найден конец xml-структуры
            if (patternEnd.exec(line) !== null) {
                // Дописываем последнюю строку
                writeStream.write('</Test>');
                writeStream.write('\n');
                // Прерываем склеивание строк
                joinStarted = false;
            } else {
                // Добавляем текущую строку
                writeStream.write(line);
            }
        }


    });
    // закрываем поток
    writeStream.write('</root>');
    writeStream.end(); 
});
