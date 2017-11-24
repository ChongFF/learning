/* TO-DO
 * 1) To customize PROC IMPORT so that it could handle exception in fields, 5-zip vs 5-4 zip
 * 2) To customize PROC IMPORT so that it would not treat the first row as header / col name
 * 3) Find a way to parse the | delimited topic, check George's sample_history_v2
 */
LIBNAME ldata '/folders/myfolders/LibFM';
%LET work_dir = /folders/myfolders/LibFM;
%LET train_pct = 80;     /* % of dataset used for training */
PROC IMPORT DATAFILE="&work_dir/ratings.dat" DBMS=DLM  REPLACE OUT=foo;
    DELIMITER='::';
RUN;
PROC IMPORT DATAFILE="&work_dir/movies.dat" DBMS=DLM  REPLACE OUT=movies;
    DELIMITER='::';
RUN;
PROC IMPORT DATAFILE="&work_dir/users.dat" DBMS=DLM  REPLACE OUT=users;
    DELIMITER='::';
RUN;

/* Ugly hard-coded data processing pipeline, would prefer if could 
add the list of columns directly when in PROC IMPORT step*/
DATA foo2;
    SET foo;
    DROP _ ;
    RENAME _1=userid_tmp _5=rating _1193=movie_tmp _978300760=timestamp_tmp;
    _978300760=_978300760-956703932; /* minimum value of timestamp as seen 
    in the next step */
    day_tmp=_978300760-INT(_978300760/(3600*24*7))*(3600*24*7);
    day_tmp=INT(day_tmp/3600/24);
RUN;
PROC MEANS DATA=foo2 MIN MAX;
    VAR timestamp_tmp;
RUN;
PROC FREQ DATA=foo2;
    TABLE day_tmp;
RUN;
DATA foo3_train foo3_test;
    SET foo2;
    userid = CAT(userid_tmp, ':', '1');
    movie = CAT(movie_tmp, ':', 1);
    day_of_week = CAT(day_tmp, ':', 1);
    DROP movie_tmp userid_tmp day_tmp timestamp_tmp;
    IF RAND('uniform') < &train_pct/100 THEN OUTPUT foo3_train;
        ELSE OUTPUT foo3_test;
RUN;
PROC EXPORT DATA=foo3_train OUTFILE="&work_dir/train.libfm" DBMS=DLM;
    DELIMITER = ' ';
RUN;
PROC EXPORT DATA=foo3_test OUTFILE="&work_dir/test.libfm" DBMS=DLM;
    DELIMITER = ' ';
RUN;