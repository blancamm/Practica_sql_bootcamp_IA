
-- Crear la tabla ADRESSES
CREATE TABLE adresses (
    adress_id SERIAL PRIMARY KEY,
    type_road VARCHAR(255),
    name_road VARCHAR(450),
    number_floor INT,
    staircase VARCHAR(255),
    letter VARCHAR(10),
    zip_code VARCHAR(20),
    city VARCHAR(255),
    country VARCHAR(255)
);

-- Crear la tabla ADMINISTARTIVES
CREATE TABLE administratives (
    administrative_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    surname VARCHAR(255) NOT NULL,
    nationality VARCHAR(255),
    telephone VARCHAR(20),
    email VARCHAR(255) UNIQUE NOT NULL,
    adress_id INT,
    rol VARCHAR (500),
    sector VARCHAR (500),
    start_contract DATE NOT NULL,
    end_contract DATE,
    type_contract VARCHAR (255) CHECK (LOWER(type_contract) IN ('temporal', 'indefinido', 'practicas', 'externo')), -- si se añadiese otro tipo de contracto habria que hacer un ALTER TABLE ADD CONTRAINT...
    end_reason VARCHAR (500),
    FOREIGN KEY (adress_id) REFERENCES adresses (adress_id)
);

-- Crear la tabla STUDENTS
CREATE TABLE students (
	student_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    surname VARCHAR(255) NOT NULL,
    nationality VARCHAR(255),
    telephone VARCHAR(20),
    email VARCHAR(255) UNIQUE NOT NULL,
    adress_id INT,
    enrolment_id INT,
    FOREIGN KEY (adress_id) REFERENCES adresses (adress_id)
);
 --se añadirá despues la contraint de que enrolment_id como FK, al ser una conexion circular (necesitamos que ambas tablas estén creadas pues la PK de una es la FK de la otra)

-- Crear la tabla TEACHERS
CREATE TABLE teachers (
	teacher_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    surname VARCHAR(255) NOT NULL,
    nationality VARCHAR(255),
    telephone VARCHAR(20),
    email VARCHAR(255) UNIQUE NOT NULL,
    adress_id INT,
    module_id INT,
    start_contract DATE NOT NULL,
    end_contract DATE,
    type_contract VARCHAR (255),
    end_reason VARCHAR (500),
    FOREIGN KEY (adress_id) REFERENCES adresses (adress_id)
    --se añadirá despues la constraint de que module_id como FK, al ser una conexion circular (necesitamos que ambas tablas estén creadas pues la PK de una es la FK de la otra)
);

CREATE TABLE bootcamps (
	bootcamp_id SERIAL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	number_editions INT, -- se ha quitado en set not null porque se podria crear el bootcamp y que todavía no haya empezado ninguna edición
	first_edition_date DATE,
	last_edition_date DATE,
	min_number_students INT NOT NULL,
	max_number_students INT,
	duration_months DECIMAL,
	duration_hours DECIMAL NOT NULL, -- que este organizado por lo menos cuantas horas serán al crear el bootcamp (la duración de meses puede variar según cuando empiece)
	number_lessons DECIMAL,
	number_modules INT NOT NULL --al crear bootcamp ya creas cuántos modulos se estudiarán
);

CREATE TABLE editions (
	edition_id SERIAL PRIMARY KEY,
	bootcamp_id INT,
	number_edition INT,
	start_date DATE,
	end_date DATE,
	price_edition DECIMAL NOT NULL, --seria el precio estandar
	total_amount_students INT,
	FOREIGN KEY (bootcamp_id) REFERENCES bootcamps(bootcamp_id)
);

CREATE TABLE enrolments (
	enrolment_id SERIAL PRIMARY KEY,
	student_id INT,
	bootcamp_id INT,
	edition_id INT,
	price_payed DECIMAL, -- puede ser distinto al estandar porque tenga una beca, o descuento por ser segundo bootcamp estudiado..-
	type_payment VARCHAR(255),
	type_discount VARCHAR(255),
	amount_discount DECIMAL,
	has_passed BOOLEAN,
	FOREIGN KEY (student_id) REFERENCES students(student_id),
	FOREIGN KEY (bootcamp_id) REFERENCES bootcamps(bootcamp_id),
	FOREIGN KEY (edition_id) REFERENCES editions (edition_id)
);

CREATE TABLE modules(
	module_id SERIAL PRIMARY KEY,
	name VARCHAR (255),
	type_module VARCHAR (100) CHECK (LOWER(type_module) IN ('transversal', 'specialised', 'separate_course')), --los dos primeros pertenecen exclusivamente a los que hacen bootcamp y la tercera opción corresponde a cursos grabados de la plataforma
	creation_date DATE,
	elimination_date DATE,
	is_recorded BOOLEAN,
	number_classes_or_videos INT,
	duration_hours DECIMAL,
	teacher_id INT,
	bootcamp_id INT, -- seria null si es un separate course
	FOREIGN KEY (teacher_id) REFERENCES teachers(teacher_id),
	FOREIGN KEY (bootcamp_id) REFERENCES bootcamps(bootcamp_id)
);

CREATE TABLE marks (
	mark_id SERIAL PRIMARY KEY,
	note VARCHAR (100), --Puedes poner apto y no apto y podrás poner también un número
	second_try_note VARCHAR(100), --por si es no apto en el primer intento
	student_id INT,
	module_id INT,
	enrolment_id INT,
	FOREIGN KEY (student_id) REFERENCES students(student_id),
	FOREIGN KEY (module_id) REFERENCES modules(module_id),
	FOREIGN KEY (enrolment_id) REFERENCES enrolments(enrolment_id)
);

ALTER TABLE students
ADD CONSTRAINT enrolment_id FOREIGN KEY (enrolment_id) REFERENCES enrolments(enrolment_id);

ALTER TABLE teachers
ADD CONSTRAINT module_id FOREIGN KEY (module_id) REFERENCES modules(module_id);

