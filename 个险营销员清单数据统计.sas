%let path=D:\ǰ��\����Ӫ��Ա�嵥����ͳ��\CESϵͳ����Դ;/*��������·��*/
%macro read_tablen(path,table_name,data_name);
proc import datafile="&path.\&table_name..csv" dbms=csv out=&data_name. replace;
getnames = yes;
run;
%mend read_tablen;/*����(2013��excel)��*/

%read_tablen(&path,����ҵ��Ա��Ϣ(����),ces_data);/*����CES�嵥����*/

data target_data1;/*����ʹ�õ��ֶ��������ͼӱ�ǩ*/
set ces_data(rename=(VAR2=sub_branch_old VAR13=sales_code VAR15=status VAR23=contract_date VAR26=departure_date VAR25=end_of_preparation_date VAR39=flag));
label 
	sub_branch_old=��������
	sales_code=Ӫ��Ա����
	status=��ְ״̬
	contract_date=ǩԼʱ��
	departure_date=��˾ʱ��
	end_of_preparation_date=�ﱸ����ʱ��
	flag=Ӫ��Ա����;
run;
data target_data2;/*ɾ��������ֶ�,������������еġ����۹�˾��*/
set target_data1;
sub_branch_old1=tranwrd(sub_branch_old,"ǰ���������۹�˾","");/*�滻*/
sub_branch_old2=tranwrd(sub_branch_old1,"�ֹ�˾","");
sub_branch_old3=tranwrd(sub_branch_old2,"����","");
sub_branch=compress(sub_branch_old3);/*ȥ������ո�*/
keep sub_branch sales_code status contract_date departure_date end_of_preparation_date flag;
label sub_branch=��������;
run;
data target_data;/*����ʱ�䣬ֻ������ݺ��·�*/
set target_data2;
year1=year(contract_date);/*����ǩԼʱ��*/
month1=month(contract_date);
if month1<10 then month2=compress('0'||month1); else month2=month1;/*��С��10���·�ǰ�����0���Ա�Ƚϴ�С*/
contract_date_new=input(compress(year1||month2),best12.);
year1=year(departure_date);/*������˾ʱ��*/
month1=month(departure_date);
if month1<10 then month2=compress('0'||month1); else month2=month1;/*��С��10���·�ǰ�����0���Ա�Ƚϴ�С*/
departure_date_new=input(compress(year1||month2),best12.);
year1=year(end_of_preparation_date);/*����ﱸ����ʱ��*/
month1=month(end_of_preparation_date);
if month1<10 then month2=compress('0'||month1); else month2=month1;/*��С��10���·�ǰ�����0���Ա�Ƚϴ�С*/
end_of_preparation_date_new=input(compress(year1||month2),best12.);
drop year1 month1 month2;/*ɾ���м����*/
run;

/*ͳ�ơ���Ա��*/
proc sql;
create table plus1 as
select
sub_branch,
contract_date_new,
count(a.sales_code) as sales_num
from target_data a 
group by a.sub_branch,contract_date_new;
quit;
proc means data=plus1 noprint;/*���ӻ����ܼ�����*/
var sales_num;
class contract_date_new;
output out=result1 (keep=contract_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*ͳһ������*/
sub_branch="�ܼ�";
run;
data plus2;
set plus1 result2;
run;
proc sort data=plus2;/*ת��ǰ������*/
by sub_branch;
run;
proc transpose data=plus2 out=plus(drop=_name_);/*���·ݱ�Ϊ�б���*/
by sub_branch;
id contract_date_new;
var sales_num;
run;

/*ͳ�ơ����䡱*/
proc sql;
create table minus1 as
select
sub_branch,
departure_date_new,
count(a.sales_code) as sales_num
from target_data a 
where a.status="��ְ"
group by a.sub_branch,departure_date_new;
quit;
proc means data=minus1 noprint;/*���ӻ����ܼ�����*/
var sales_num;
class departure_date_new;
output out=result1 (keep=departure_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*ͳһ������*/
sub_branch="�ܼ�";
run;
data minus2;
set minus1 result2;
run;
proc sort data=minus2;/*ת��ǰ������*/
by sub_branch;
run;
proc transpose data=minus2 out=minus(drop=_name_);/*���·ݱ�Ϊ�б���*/
by sub_branch;
id departure_date_new;
var sales_num;
run;

/*��ְ��Ա�ġ���Ա���롰���䡱*/

/*ͳ�ơ���Ա��*/
proc sql;
create table plus1 as
select
sub_branch,
contract_date_new,
count(a.sales_code) as sales_num
from target_data a 
where a.flag="��ְ��Ա"/*��ʶ��ϢΪ����ְ��Ա��*/
group by a.sub_branch,contract_date_new;
quit;
proc means data=plus1 noprint;/*���ӻ����ܼ�����*/
var sales_num;
class contract_date_new;
output out=result1 (keep=contract_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*ͳһ������*/
sub_branch="�ܼ�";
run;
data plus2;
set plus1 result2;
run;
proc sort data=plus2;/*ת��ǰ������*/
by sub_branch;
run;
proc transpose data=plus2 out=part_time_plus(drop=_name_);/*���·ݱ�Ϊ�б���*/
by sub_branch;
id contract_date_new;
var sales_num;
run;

/*ͳ�ơ����䡱*/
proc sql;
create table minus1 as
select
sub_branch,
departure_date_new,
count(a.sales_code) as sales_num
from target_data a 
where a.status="��ְ" and a.flag="��ְ��Ա"
group by a.sub_branch,departure_date_new;
quit;
proc means data=minus1 noprint;/*���ӻ����ܼ�����*/
var sales_num;
class departure_date_new;
output out=result1 (keep=departure_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*ͳһ������*/
sub_branch="�ܼ�";
run;
data minus2;
set minus1 result2;
run;
proc sort data=minus2;/*ת��ǰ������*/
by sub_branch;
run;
proc transpose data=minus2 out=part_time_minus(drop=_name_);/*���·ݱ�Ϊ�б���*/
by sub_branch;
id departure_date_new;
var sales_num;
run;

/*ָ���·ݵġ��ﱸ��Ա��ͳ��*/
proc sql;
create table ready_sales as
select
sub_branch,
count(a.sales_code) as sales_num
from target_data a 
where a.contract_date_new=201811 and a.end_of_preparation_date_new=201811 and (a.departure_date_new=. or departure_date_new=201812) /*�ڴ����޸�ָ���·ݼ���*/
group by a.sub_branch;
quit;

/*��������*/
data final_data;
set plus minus part_time_plus part_time_minus ready_sales;
run;
PROC EXPORT DATA=final_data OUTFILE="D:\ǰ��\����Ӫ��Ա�嵥����ͳ��\final_data.xlsx" DBMS=EXCEL REPLACE;
RUN;
