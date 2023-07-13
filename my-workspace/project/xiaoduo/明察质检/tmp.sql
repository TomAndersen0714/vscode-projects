CREATE VIEW test.view
AS
SELECT *
FROM numbers(10)
WHERE number = { NUMBER :Int32 }