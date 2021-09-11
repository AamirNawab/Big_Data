#!/bin/bash
#Check data.tgz is dowloaded
ls -l ~/Downloads/data.tgz

#Create assignment directory
mkdir -p ~/assignment/Aamir_Nawab/impala/

#Move data.tgz to assignment directory and verify it has been moved
cp ~/Downloads/data.tgz ~/assignment/Aamir_Nawab/impala/
ls -l ~/assignment/Aamir_Nawab/impala/data.tgz

#Extract data in assignment directory
tar xzvf ~/assignment/Aamir_Nawab/impala/data.tgz -C ~/assignment/Aamir_Nawab/impala/

#Setup database and tables in mysql
mysql -uroot -pcloudera < ~/Downloads/assignment/db_setup.sql

#PREPROCESSING DATA TO INSERT INTO mysql TABLES

#hearing_evaluation.sql
#filter out records where primary key is violated and null values
cat ~/assignment/Aamir_Nawab/impala/hearing_evaluation.sql | sort -u -t ',' -k 1,2 -s | grep -v 'NULL' | mysql -uroot -pcloudera assignment

#diagnoses.sql
#filtering out null values
cat ~/assignment/Aamir_Nawab/impala/diagnoses.sql  | grep -v 'NULL' | mysql -uroot -pcloudera assignment

#imaging.sql
#filtering out null values
cat ~/assignment/Aamir_Nawab/impala/imaging.sql  | grep -v 'NULL' | mysql -uroot -pcloudera assignment

#USE sqoop to move data into hadoop
#Exit safemode
hdfs dfsadmin -safemode leave

#Import all tables with sqoop fields terminated with "\t" and stored in assignment directory
sqoop import-all-tables --connect jdbc:mysql://localhost/assignment --username root --password cloudera --fields-terminated-by "\t" \
--warehouse-dir /assignment --as-parquetfile --driver com.mysql.jdbc.Driver --fetch-size -2147483648
 

#imaging table to csv including header
mysql -uroot --password=cloudera --database=assignment --execute="SELECT 'patient_id', 'imaging_age', 'modality' UNION SELECT * \
INTO OUTFILE '/tmp/imaging.csv' FIELDS TERMINATED BY ','\
ENCLOSED BY '\"' ESCAPED BY '\\\' LINES TERMINATED BY '\n' FROM imaging;"

#hearing_evaluation table to csv including header
mysql -uroot --password=cloudera --database=assignment --execute="SELECT 'patient_id', 'evaluation_age', 'severity_of_hearing_loss', \
'unilateral_or_bilateral', 'has_conductive_hearing_loss', 'has_sensorineural_hearing_loss' UNION SELECT * \
INTO OUTFILE '/tmp/hearing_evaluation.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\' LINES TERMINATED BY '\n' FROM hearing_evaluation;"

#diagnoses table to csv including header
mysql -uroot --password=cloudera --database=assignment --execute="SELECT 'patient_id', 'diagnosis_code', 'diagnosis_age' UNION SELECT * \
INTO OUTFILE '/tmp/diagnoses.csv' FIELDS TERMINATED BY ',' \
ENCLOSED BY '\"' ESCAPED BY '\\\' LINES TERMINATED BY '\n' FROM diagnoses;"

#move the .csv files from tmp directory to assignment directory
sudo mv /tmp/imaging.csv ~/assignment/Aamir_Nawab/impala/
sudo mv /tmp/hearing_evaluation.csv ~/assignment/Aamir_Nawab/impala/
sudo mv /tmp/diagnoses.csv ~/assignment/Aamir_Nawab/impala/


#TO CREATE assignment DATABASE WITH IMPALA SHELL
impala-shell -q "CREATE DATABASE IF NOT EXISTS assignment;"

#IMAGING TABLE
#CREATE TABLE DYNAMICALLY BY STORING THE OUTPUT OF ls IN A VARIABLE
P1= "$(hdfs dfs -ls /assignment/imaging/*.parquet)"

#SELECT ONLY THE FIRST LINE AND ONLY THE FILENAME AND STORE IN ANOTHER VARIABLE
P2="$(echo "${P1}" | awk -F [/] 'NR==1{print $4}')"

#SHOW PARQUET FILES IN imaging
hdfs dfs -ls /assignment/imaging/

#USE IMPALA-SHELL TO CREATE EXTERNAL TABLE USING THE PARQUET FILE IN imaging
impala-shell -q "USE assignment; CREATE EXTERNAL TABLE imaging LIKE PARQUET '/assignment/imaging/*.parquet' \
STORED AS PARQUET LOCATION '/assignment/imaging/';"

#INVALIDATE METADATA FOR IMAGING TABLE
impala-shell -q "USE assignment; INVALIDATE METADATA imaging"

#SELECT TOP 5 TO SEE THAT IT HAS BEEN LOADED CORRECTLY
impala-shell -q "USE assignment; SELECT * FROM imaging LIMIT 5;"

#HEARING_EVALUATION TABLE
#CREATE TABLE DYNAMICALLY BY STORING THE OUTPUT OF ls IN A VARIABLE
P1="$(hdfs dfs -ls /assignment/hearing_evaluation/*.parquet)"

#SELECT ONLY THE FIRST LINE AND ONLY THE FILENAME AND STORE IN ANOTHER VARIABLE
P2="$(echo "${P1}" | awk -F [/] 'NR==1{print $4}')"

#SHOW PARQUET FILES IN hearing_evaluation
hdfs dfs -ls /assignment/hearing_evaluation/

#USE IMPALA-SHELL TO CREATE EXTERNAL TABLE USING THE PARQUET FILE
impala-shell -q "USE assignment; CREATE EXTERNAL TABLE hearing_evaluation LIKE PARQUET '/assignment/hearing_evaluation/*.parquet' \
STORED AS PARQUET LOCATION '/assignment/hearing_evaluation/';"

#INVALIDATE METADATA FOR HEARING_EVALUATION TABLE
impala-shell -q "USE assignment; INVALIDATE METADATA hearing_evaluation"

#SELECT TOP 5 TO SEE THAT IT HAS BEEN LOADED CORRECTLY
impala-shell -q "USE assignment; SELECT * FROM hearing_evaluation LIMIT 5;"

#DIAGNOSES TABLE
#CREATE TABLE DYNAMICALLY BY STORING THE OUTPUT OF ls IN A VARIABLE
P1="$(hdfs dfs -ls /assignment/diagnoses/*.parquet)"

#SELECT ONLY THE FIRST LINE AND ONLY THE FILENAME AND STORE IN ANOTHER VARIABLE
P2="$(echo "${P1}" | awk -F [/] 'NR==1{print $4}')"

#VIEW PARQUET FILES IN diagnoses
hdfs dfs -ls /assignment/diagnoses/

#USE IMPALA-SHELL TO CREATE EXTERNAL TABLE USING THE PARQUET FILE
impala-shell -q "USE assignment; CREATE EXTERNAL TABLE diagnoses LIKE PARQUET '/assignment/diagnoses/*.parquet' STORED AS PARQUET \
LOCATION '/assignment/diagnoses/';"

#INVALIDATE METADATA FOR DIAGNOSES TABLE
impala-shell -q "USE assignment; INVALIDATE METADATA diagnoses;"

#SELECT TOP 5 TO SEE THAT IT HAS BEEN LOADED CORRECTLY
impala-shell -q "USE assignment; SELECT * FROM diagnoses LIMIT 5;"


###############################DATA ANALYSIS#########################
#Create results directory to store query outputs
mkdir ~/assignment/Aamir_Nawab/impala/queries/

#problem statement 1
impala-shell -B --quiet -q "use assignment; SELECT diagnosis_code, count(diagnosis_code) AS frequency FROM assignment.diagnoses \
GROUP BY diagnosis_code ORDER BY COUNT(diagnosis_code) DESC LIMIT 5;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/queries/statement1.csv

#problem statement 2
 
impala-shell -B --quiet -q "use assignment; SELECT a.diagnosis_code, count(a.diagnosis_code) AS frequency FROM assignment.diagnoses a \
JOIN hearing_evaluation  ON a.diagnosis_age = hearing_evaluation.evaluation_age and a.patient_id=hearing_evaluation.patient_id GROUP BY a.diagnosis_code \
ORDER BY COUNT(a.diagnosis_code) DESC LIMIT 5;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/statement2.csv


#problem statement 3
impala-shell -B --quiet -q "use assignment; SELECT patient_id, count(patient_id) AS highest_diagnosis FROM assignment.diagnoses GROUP BY patient_id \
ORDER BY COUNT(patient_id) DESC LIMIT 1;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/queries/statement3.csv

#problem statement 4

impala-shell -B --quiet -q "USE assignment; SELECT a.item, a.total_hearing_problem, b.total_patient_id, round(100 *(a.total_hearing_problem / b.total_patient_id), 2) \
AS 'percentage(%)' \
FROM (SELECT 'result' AS item, COUNT(DISTINCT(patient_id)) AS 'total_hearing_problem' \
FROM assignment.hearing_evaluation \
WHERE severity_of_hearing_loss IN ('Mild', 'Moderate', 'Moderately Severe', 'Slight', 'Profound', 'Severe')) AS a \
INNER JOIN (SELECT 'result' AS item, COUNT(DISTINCT(patient_id)) AS 'total_patient_id' \
FROM assignment.hearing_evaluation) AS b \
ON b.item = a.item;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/queries/statement4.csv

 
#problem statement 5
impala-shell -B --quiet -q "USE assignment; SELECT A.item, a.total_investigation, b.total_patient_id, \
ROUND(b.total_patient_id / a.total_investigation, 2) AS 'average' \
FROM (SELECT 'result' AS item, COUNT(DISTINCT(h.patient_id)) AS 'total_investigation' \
FROM assignment.hearing_evaluation h INNER JOIN imaging im ON h.patient_id = im.patient_id \
WHERE h.severity_of_hearing_loss IN ('Mild', 'Moderate', 'Moderately Severe', 'Slight', 'Profound', 'Severe')) \
AS A INNER JOIN (SELECT 'result' AS item, COUNT(DISTINCT(patient_id)) AS total_patient_id \
FROM assignment.hearing_evaluation \
WHERE severity_of_hearing_loss IN ('Mild','Slight', 'Moderate', 'Moderately Severe', 'Severe', 'Profound')) AS b ON b.item = A.item;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/queries/statement5.csv


#problem statement 6
impala-shell -B --quiet -q "USE assignment; SELECT COUNT(imaging_age) AS total_no_CT, floor(imaging_age), modality AS type_of_scan \
FROM assignment.imaging WHERE modality IN ('CT') GROUP BY modality, floor(imaging_age) ORDER BY total_no_CT DESC;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/queries/statement6.csv


#problem statement 7
impala-shell -B --quiet -q "USE assignment; CREATE VIEW total_frequency AS \
SELECT FLOOR(diagnosis_age) AS age, diagnosis_code, COUNT(diagnosis_code) AS frequency FROM diagnoses WHERE diagnosis_age > 0 GROUP BY age, \
diagnosis_code ORDER BY frequency DESC;" 

impala-shell -B --quiet -q "USE assignment; CREATE VIEW frequency_by_age AS \
SELECT age, diagnosis_code, frequency AS max_frequency FROM total_frequency AS tf WHERE frequency = (SELECT MAX(frequency) FROM total_frequency \
AS tfmax WHERE tf.age = tfmax.age) ORDER BY age;"

impala-shell -B --quiet -q "USE assignment; SELECT * FROM frequency_by_age ORDER BY age;" \
--output_delimiter=, \
--print_header \
-o /home/cloudera/assignment/Aamir_Nawab/impala/queries/statement7.csv


 



























 
