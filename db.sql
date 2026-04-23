DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users_audit;

-- Создание таблицы users

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы users_audit

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- Функция логирования изменений по трем полям

CREATE OR REPLACE FUNCTION log_user_audit()
RETURNS TRIGGER AS $$
BEGIN 
    IF OLD.name IS DISTINCT FROM NEW.name THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;

    IF OLD.email IS DISTINCT FROM NEW.email THEN 
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;

    IF OLD.role IS DISTINCT FROM NEW.role THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
    END IF;

    IF OLD.* IS DISTINCT FROM NEW.* THEN
        NEW.updated_at = CURRENT_TIMESTAMP;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера на таблицу users

CREATE TRIGGER trigger_log_user_audit
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_audit();

-- Добавление данных в таблицу users

INSERT INTO users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'admin'),
('Anna Petrova', 'anna@example.com', 'user');

-- Изменение данных в таблице users

UPDATE users
SET email = 'ivan.new@example.com'
WHERE name = 'Ivan Ivanov';

UPDATE users 
SET role = 'admin'
WHERE name = 'Ivan Ivanov';

UPDATE users 
SET role = 'user'
WHERE name = 'Ivan Ivanov';

UPDATE users
SET email = 'anna.new@example.com',
role = 'admin'
WHERE name = 'Anna Petrova';    

-- Установка расширения pg_cron

CREATE EXTENSION IF NOT EXISTS pg_cron;
  
-- Функция экспорта данных за вчерашний день из таблицы users_audit

CREATE OR REPLACE FUNCTION export_logs_to_csv()
RETURNS void AS $$
DECLARE
    yesterday_date TEXT;
    output_file TEXT;
BEGIN
    yesterday_date := TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYYMMDD');
    output_file := '/tmp/users_audit_export_' || yesterday_date || '.csv';
    EXECUTE FORMAT(
        'COPY (
            SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
            FROM users_audit
            WHERE DATE(changed_at) = CURRENT_DATE - INTERVAL ''1 day''
        ) TO ''%s'' WITH (FORMAT CSV, HEADER, DELIMITER '','')',
        output_file
    );
END;
$$ LANGUAGE plpgsql;

-- Настройка планировщика

SELECT cron.schedule(
    'run-export-logs-to-csv',
    '0 3 * * *',
    'SELECT export_logs_to_csv();'
);

-- Просмотр запущенных job

SELECT * FROM cron.job;

