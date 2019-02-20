%let path=D:\前海\个险营销员清单数据统计\CES系统数据源;/*基础数据路径*/
%macro read_tablen(path,table_name,data_name);
proc import datafile="&path.\&table_name..csv" dbms=csv out=&data_name. replace;
getnames = yes;
run;
%mend read_tablen;/*读表(2013版excel)宏*/

%read_tablen(&path,个险业务员信息(简含销),ces_data);/*读入CES清单数据*/

data target_data1;/*将拟使用的字段重命名和加标签*/
set ces_data(rename=(VAR2=sub_branch_old VAR13=sales_code VAR15=status VAR23=contract_date VAR26=departure_date VAR25=end_of_preparation_date VAR39=flag));
label 
	sub_branch_old=二级机构
	sales_code=营销员代码
	status=在职状态
	contract_date=签约时间
	departure_date=离司时间
	end_of_preparation_date=筹备结束时间
	flag=营销员种类;
run;
data target_data2;/*删除多余的字段,处理二级机构中的“销售公司”*/
set target_data1;
sub_branch_old1=tranwrd(sub_branch_old,"前海保险销售公司","");/*替换*/
sub_branch_old2=tranwrd(sub_branch_old1,"分公司","");
sub_branch_old3=tranwrd(sub_branch_old2,"销售","");
sub_branch=compress(sub_branch_old3);/*去除多余空格*/
keep sub_branch sales_code status contract_date departure_date end_of_preparation_date flag;
label sub_branch=二级机构;
run;
data target_data;/*处理时间，只保留年份和月份*/
set target_data2;
year1=year(contract_date);/*处理签约时间*/
month1=month(contract_date);
if month1<10 then month2=compress('0'||month1); else month2=month1;/*在小于10的月份前面加上0，以便比较大小*/
contract_date_new=input(compress(year1||month2),best12.);
year1=year(departure_date);/*处理离司时间*/
month1=month(departure_date);
if month1<10 then month2=compress('0'||month1); else month2=month1;/*在小于10的月份前面加上0，以便比较大小*/
departure_date_new=input(compress(year1||month2),best12.);
year1=year(end_of_preparation_date);/*处理筹备结束时间*/
month1=month(end_of_preparation_date);
if month1<10 then month2=compress('0'||month1); else month2=month1;/*在小于10的月份前面加上0，以便比较大小*/
end_of_preparation_date_new=input(compress(year1||month2),best12.);
drop year1 month1 month2;/*删除中间变量*/
run;

/*统计“增员”*/
proc sql;
create table plus1 as
select
sub_branch,
contract_date_new,
count(a.sales_code) as sales_num
from target_data a 
group by a.sub_branch,contract_date_new;
quit;
proc means data=plus1 noprint;/*增加机构总计数量*/
var sales_num;
class contract_date_new;
output out=result1 (keep=contract_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*统一变量名*/
sub_branch="总计";
run;
data plus2;
set plus1 result2;
run;
proc sort data=plus2;/*转置前先排序*/
by sub_branch;
run;
proc transpose data=plus2 out=plus(drop=_name_);/*将月份变为列变量*/
by sub_branch;
id contract_date_new;
var sales_num;
run;

/*统计“脱落”*/
proc sql;
create table minus1 as
select
sub_branch,
departure_date_new,
count(a.sales_code) as sales_num
from target_data a 
where a.status="离职"
group by a.sub_branch,departure_date_new;
quit;
proc means data=minus1 noprint;/*增加机构总计数量*/
var sales_num;
class departure_date_new;
output out=result1 (keep=departure_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*统一变量名*/
sub_branch="总计";
run;
data minus2;
set minus1 result2;
run;
proc sort data=minus2;/*转置前先排序*/
by sub_branch;
run;
proc transpose data=minus2 out=minus(drop=_name_);/*将月份变为列变量*/
by sub_branch;
id departure_date_new;
var sales_num;
run;

/*兼职人员的“增员”与“脱落”*/

/*统计“增员”*/
proc sql;
create table plus1 as
select
sub_branch,
contract_date_new,
count(a.sales_code) as sales_num
from target_data a 
where a.flag="兼职人员"/*标识信息为“兼职人员”*/
group by a.sub_branch,contract_date_new;
quit;
proc means data=plus1 noprint;/*增加机构总计数量*/
var sales_num;
class contract_date_new;
output out=result1 (keep=contract_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*统一变量名*/
sub_branch="总计";
run;
data plus2;
set plus1 result2;
run;
proc sort data=plus2;/*转置前先排序*/
by sub_branch;
run;
proc transpose data=plus2 out=part_time_plus(drop=_name_);/*将月份变为列变量*/
by sub_branch;
id contract_date_new;
var sales_num;
run;

/*统计“脱落”*/
proc sql;
create table minus1 as
select
sub_branch,
departure_date_new,
count(a.sales_code) as sales_num
from target_data a 
where a.status="离职" and a.flag="兼职人员"
group by a.sub_branch,departure_date_new;
quit;
proc means data=minus1 noprint;/*增加机构总计数量*/
var sales_num;
class departure_date_new;
output out=result1 (keep=departure_date_new sales_num_sum)
       sum=sales_num_sum;
run;
data result2;
set result1(rename=(sales_num_sum=sales_num));/*统一变量名*/
sub_branch="总计";
run;
data minus2;
set minus1 result2;
run;
proc sort data=minus2;/*转置前先排序*/
by sub_branch;
run;
proc transpose data=minus2 out=part_time_minus(drop=_name_);/*将月份变为列变量*/
by sub_branch;
id departure_date_new;
var sales_num;
run;

/*指定月份的“筹备人员”统计*/
proc sql;
create table ready_sales as
select
sub_branch,
count(a.sales_code) as sales_num
from target_data a 
where a.contract_date_new=201811 and a.end_of_preparation_date_new=201811 and (a.departure_date_new=. or departure_date_new=201812) /*在此行修改指定月份即可*/
group by a.sub_branch;
quit;

/*汇总数据*/
data final_data;
set plus minus part_time_plus part_time_minus ready_sales;
run;
PROC EXPORT DATA=final_data OUTFILE="D:\前海\个险营销员清单数据统计\final_data.xlsx" DBMS=EXCEL REPLACE;
RUN;
