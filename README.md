# sasboost

Data analysis in human language.

# Talk is cheap, here is the code

Below is a typical data processing process done in sasboost and vanilla SAS.

### sasboost
```sas
/*
Summarize monthly total revenue and balance by customer segments from Jan 2019 to Sept 2020.
*/
* loop yymm variable through months;
%for(1901, 2009, yymm, %nrstr(
    * prepare customer base by segment;
	%customer_segment(&yymm.);
    * operating on customer base;
	%with(segment_&yymm.);
		%freq(, segment);
        * extract revenue and balance data, and merge them to customer base;
		%mkconcat(, &yymm., revenue prod_balance,);
		%p
		%sql
			select segment
				, count(*) as n_cust
				%foreach(var, aum rev bal_fx bal_cash bal_ut bal_sec, %nrstr(
					, sum(&var.) as &var.
				))
			%from
			group by segment
			%quit
		%p(,30)
		%saveas(,smry&yymm.)
))
```

### vanilla SAS
```sas
libname datamart "/home/joy/customer";

%macro summarize(yymm);
    data customer;
        set datamart.customers&yymm.;
        run;

    proc freq data=customer;
        var segment;
        run;
    
    data revenue;
        set datamart.revenue&yymm.;
        run;
    
    data balance;
        set datamart.balance&yymm.;
        run;

    proc sort data=revenue; by cust_id;run;
    proc sort data=balance; by cust_id;run;
    data customer;
        merge customer (ina) revenue (inb) balance (inc);
        by cust_id;
        if ina;
        run;
    proc print data=customer(obs=10);run;

    proc sql;
        create table customer2 as
        select segment
            , count(*) as n_cust
            , sum(aum) as aum
            , sum(rev) as rev
            , sum(bal_fx) as bal_fx
            , sum(bal_cash) as bal_cash
            , sum(bal_ut) as bal_ut
            , sum(bal_sec) as bal_sec
        from customer
        group by segment
        ;quit;

    data customer;
        set customer2;
        run;

    proc delete data=customer2;run;

    proc print data=customer(obs=30);run;

    data smry&yymm.;
        set customer;
        run;
%mend;
%summarize(1901);
%summarize(1902);
%summarize(1903);
%summarize(1904);
%summarize(1905);
%summarize(1906);
%summarize(1907);
%summarize(1908);
%summarize(1909);
%summarize(1910);
%summarize(1911);
%summarize(1912);
%summarize(2001);
%summarize(2002);
%summarize(2003);
%summarize(2004);
%summarize(2005);
%summarize(2006);
%summarize(2007);
%summarize(2008);
%summarize(2009);
```
