/**
 * Модуль подключения к PostgreSQL
 * Использует глобальный пулл соединений к БД
 * Все параметры подключения берутся из переменных окружения
 */
'use strict';
let pg = require('pg'),
    config = {
        database: process.env.KEYHABITS_DB_DB, // база данных
        host: process.env.KEYHABITS_DB_ADDR, // адрес БД
        idleTimeoutMillis: process.env.KEYHABITS_DB_CTO, // время жизни соединения в простое
        max: process.env.KEYHABITS_DB_MAXC, // максимальное количество соединений
        password: process.env.KEYHABITS_DB_PWD, // пароль пользоватлея
        port: process.env.KEYHABITS_DB_PORT, // порт
        user: process.env.KEYHABITS_DB_USER, // имя пользователя
    },
    pgPool = new pg.Pool(config); // инстанс

// хэндлер ошибок пулера
pgPool.on('error', function (err) {
    console.error('db: idle client error', err.message, err.stack);
});

module.exports = exports = {
    /**
     * функция генерации даты/времени для вставки в бд
     * входной формат даты дд.мм.гггг чч:мм:сс
     */
    createFormatedDateTime: (d) => {
        let fd = d.split(' ')[0].split('.');

        /* eslint-disable no-magic-numbers */
        return `${fd[2]}-${fd[1]}-${fd[0]} ${d.split(' ')[1]}`;
        /* eslint-enable no-magic-numbers */
    },

    // Выполнение SQL запроса с опциональными параметрами
    execute: (text, values) => pgPool.query(text, values),

    // Пул соединений приложения
    pool: pgPool

};