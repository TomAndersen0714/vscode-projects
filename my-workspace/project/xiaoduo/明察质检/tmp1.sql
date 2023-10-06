CREATE TABLE `student` (
    `id` int(11), //学号
    `name` varchar(11) DEFAULT NULL, //学生姓名
    `age` int(11) DEFAULT NULL, //年龄
    `sex` varchar(11) DEFAULT NULL, //性别
    PRIMARY KEY (`id`)
);

CREATE TABLE `score_relation` (
    `id` int(11), //主键id
    `course_no` int(11) DEFAULT NULL, //课程
    `student_no` int(11) DEFAULT NULL,//学号
    `score` int(11) DEFAULT NULL, //成绩
    PRIMARY KEY (`id`)
);

查询至少有一门课与学号为"7"同学所学课程相同的同学, 查询这些学生的学号与姓名，去重后，按照学号升序排列。


SELECT DISTINCT
    `id`, `name`
FROM `student`
WHERE `id` IN (
    SELECT
        student_no
    FROM `score_relation`
    WHERE course_no IN (
        SELECT DISTINCT
            course_no
        FROM `score_relation`
        WHERE student_no = 7
    ) AS course_no_table
) AS student_no_table
ORDER BY `id` ASC


