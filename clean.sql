
DELETE FROM orders WHERE id NOT IN (
    "CGJGJHVIU60B182A2K2G",
    "CGJGBANIU60B182A2K1G",
    "CGQUSN3Q199D9CM25CF0",
    "CGQO88JQ199D9CM25CC0",
    "CH4BJKRQ199FIAO93KI0",
    "CH4PQLJQ199FIAO93KJ0",
    "CH62FKJQ199FIAO93KL0",
    "CHL4H9RPOR5D9NS43LP0",
    "CHR3P63POR5D9NS43LS0",
    "CHV0JCO8R5FT9L6H3HB0",
    "CI5GU808R5FTINMCSK5G",
    "CI5GO308R5FTINMCSK50",
    "CI8QB088R5FTINMCSK90",
    "CII3NQO8R5FSQ31HHFHG",
    "CIM3G7UUGOMDMK168M0G",
    "CIMNSC6UGOMDMK168M1G",
    "CJ4IS26UGOMFJ5N1AC40",
    "CGQO88JQ199D9CM25CBG",
	"CGQUSN3Q199D9CM25CEG",
	"CHL4H9RPOR5D9NS43LOG",
	"CHR3P63POR5D9NS43LRG",
	"CHV0JCO8R5FT9L6H3HAG",
	"CI5GO308R5FTINMCSK4G",
	"CI8QB088R5FTINMCSK8G",
	"CII3NQO8R5FSQ31HHFH0",
	"CJ4IS26UGOMFJ5N1AC3G"
);

DELETE FROM transactions WHERE order_id NOT IN (
    "CGJGJHVIU60B182A2K2G",
    "CGJGBANIU60B182A2K1G",
    "CGQUSN3Q199D9CM25CF0",
    "CGQO88JQ199D9CM25CC0",
    "CH4BJKRQ199FIAO93KI0",
    "CH4PQLJQ199FIAO93KJ0",
    "CH62FKJQ199FIAO93KL0",
    "CHL4H9RPOR5D9NS43LP0",
    "CHR3P63POR5D9NS43LS0",
    "CHV0JCO8R5FT9L6H3HB0",
    "CI5GU808R5FTINMCSK5G",
    "CI5GO308R5FTINMCSK50",
    "CI8QB088R5FTINMCSK90",
    "CII3NQO8R5FSQ31HHFHG",
    "CIM3G7UUGOMDMK168M0G",
    "CIMNSC6UGOMDMK168M1G",
    "CJ4IS26UGOMFJ5N1AC40",
    "CGQO88JQ199D9CM25CBG",
	"CGQUSN3Q199D9CM25CEG",
	"CHL4H9RPOR5D9NS43LOG",
	"CHR3P63POR5D9NS43LRG",
	"CHV0JCO8R5FT9L6H3HAG",
	"CI5GO308R5FTINMCSK4G",
	"CI8QB088R5FTINMCSK8G",
	"CII3NQO8R5FSQ31HHFH0",
	"CJ4IS26UGOMFJ5N1AC3G"
);

DELETE FROM natural_person_documents
WHERE natural_person_id IN (
    SELECT np.id
    FROM users u
             LEFT JOIN natural_persons np ON np.user_id = u.id
    WHERE u.email like '%yopmail%'
);

DELETE FROM bank_accounts_natural_persons
WHERE natural_person_id IN (
    SELECT np.id
    FROM users u
             LEFT JOIN natural_persons np ON np.user_id = u.id
    WHERE u.email like '%yopmail%'
);

DELETE FROM society_documents
WHERE society_id IN (
    SELECT s.id
    FROM users u
             LEFT JOIN societies s ON s.user_id = u.id
    WHERE u.email like '%yopmail%'
);

DELETE FROM bank_accounts_societies
WHERE society_id IN (
    SELECT s.id
    FROM users u
             LEFT JOIN societies s ON s.user_id = u.id
    WHERE u.email like '%yopmail%'
);

DELETE FROM natural_persons
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM societies
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM orders
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM transactions
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM contacts
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM user_observations
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM user_questions
WHERE user_id IN (
    SELECT u.id
    FROM users u
    WHERE u.email like '%yopmail%'
);

DELETE FROM users WHERE email like '%yopmail%';