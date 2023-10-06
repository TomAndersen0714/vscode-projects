create table `score`(
`id` int(11),
`class_no` varchar(11) default null,
`course_name` varchar(11) default null,
`student_no` varchar(11) default null,
`student_score` int(11) default null,
primary key(`id`)
);

insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_a', '1', 80);
insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_b', '1', 80);
insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_c', '1', 80);
insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_b', '2', 80);
insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_c', '2', 90);
insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_b', '3', 80);
insert into `score`(`class_no`, `course_name`, `student_no`, `student_score`) values('1', 'course_c', '3', 90);

SELECT
    class_no,
    student_no,
    score_sum
FROM (
    SELECT
        class_no,
        student_no,
        score_sum,
        dense_rank() OVER(ORDER BY score_sum DESC) AS d_rank
    FROM (
        SELECT
            class_no,
            student_no,
            SUM(student_score) AS score_sum
        FROM `score`
        WHERE course_name = 'course_b' OR course_name = 'course_c'
        GROUP BY class_no, student_no
    ) AS tmp
) AS rank_table
WHERE d_rank = 2;


SELECT
    class_no,
    student_no,
    SUM(student_score) AS score_sum
FROM `score`
WHERE course_name = 'course_b' OR course_name = 'course_c'
GROUP BY class_no, student_no
ORDER BY score_sum DESC
LIMIT 1 OFFSET 1;