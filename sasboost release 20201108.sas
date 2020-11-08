/*
Data analysis in human language.
*/

%let __package__ = sasBoost;
%let __version__ = 0.1;
%let __author__  = Alan H Yue;
%let __email__   = alanhyue@outlook.com;

* sql utilities;

%macro sql;
	proc sql;
		create table &__with__.2 as
%mend;

%macro from;
	from &__with__.
%mend;

%macro quit;
	;quit;
	data &__with__.;
		set &__with__.2;
		run;
	%rm(&__with__.2);
%mend;

%global __with__;
%let __with__ =;
%macro data;
	* wrapper header for data steps;
	data &__with__.;
			set &__with__.
%mend;

%macro drop(din, columns);
	%default_to(din, &__with__.)
	data &din.;
		set &din. (drop=&columns.);
		run;
%mend;

%macro saveas(din, dout);
	%default_to(din, &__with__.)
	data &dout.;
		set &din.;
		run;
%mend;

%macro with(din);
	data &din._tmp;
		set &din.;
		run;
	%global __with__;
	%let __with__=&din._tmp;
%mend;

%macro gbcount(din, by, count);
	/* %gbcount(S2.MAPPING, cust_id, item_id);*/
	%default_to(din, &__with__.)
	title "number of &count. per &by.";
	    proc sql;
	    	create table gbcount as
	        select	&by.
	        , count(&count.) as n_&count.
	        , count(unique(&count.)) as nu_&count.
	        from	 &din.
	        group by &by.
	        order by nu_&count. desc
	        ;quit;
	   %freq(gbcount, n_&count. nu_&count., internal);
	   %p(gbcount, n=10);
	title;
%mend;


%macro add_group(din, var, logic, dout);
	%default_to(din, &__with__.);
	%default_to(dout, &din.);
	PROC SQL;
	 create table _tmp as
	 SELECT *,
		 CASE
		 	&logic.
		 end as &var.
	 FROM &din.;
	QUIT;
	
	data &dout.;
		set _tmp;
		run;
	%rm(_tmp);
%mend;

%macro change_date9(date, interval, step, tovar);
	/* Example
	%let newdate=;
	%change_date9(01jan2019, day, 3, newdate);
	%put &newdate.; *-> 04JAN2019;
	*/
	%local tmp_date;
	%let tmp_date = %sysfunc(inputn(&date.,anydtdte9.));
	%let &tovar.=%sysfunc(intnx(&interval.,&tmp_date.,&step.,b),date9.);
%mend;

%macro date_loop(start, end, interval, step, funcargs);
	/*%date_loop(01jul2015,01aug2015, day, 3, %nrstr(%put &step_begin. -> &step_end.;));*/
	%local step_begin step_end;
	%change_date9(&start., day, 0, step_begin);
	%change_date9(&step_begin., day, &step., step_end);


	%do %while (%sysevalf("&step_end."d < "&end."d));
		*work, work;
		%put &step_begin. -> &step_end.;
		%unquote(&funcargs.)
		%let step_begin = &step_end.;
		%change_date9(&step_end., day, &step., step_end);
	%end;
%mend;

%macro isblank(param);
	%sysevalf(%superq(param)=,boolean)
%mend isblank;

%macro foreach_bar(__var, __list, funcargs, sep=|);
	%local __i funcargs parsed &__var.;
	%let __i = 1;
	%do %while (%isblank(%scan(&__list., &__i., "&sep.")) ne 1);
	    %let &__var. = %scan(&__list., &__i., "&sep.");
        %unquote(&funcargs.)
        %let __i = %eval(&__i. + 1);
    %end;
%mend;



%macro keepif(din, conds, dout);
	%local Sin lines_from lines_to lines_desc i;
	data &dout.;
		set &din.;
		run;
	%foreach_bar(cd, &conds., %nrstr(
		%let Sin=;
		%nobs(&dout., Sin);
		data &dout.;
			set &dout.;
			if &cd.;
			run;
		%let Sout=;
		%nobs(&dout., Sout);
		
		%let lines_from = &lines_from. &Sin.;
		%let lines_to = &lines_to. &Sout.;
		%let lines_desc = &lines_desc. | &cd.;
		
	))
    data result;
        length cond $ 100;
        %do i=1 %to %sysfunc(countw(&lines_from.));
                before = %scan(&lines_from., &i);
                after = %scan(&lines_to., &i);
                cond = "%scan(&lines_desc., &i, |)";
                output;
        %end;
        run;
    data result;
    	retain pct_tot 1;
    	set result;
    	obs = _n_;
    	pct_step = after / before;

    	pct_tot = pct_tot * pct_step;
    	run;
	proc sql;
		select
			obs 
			, before format=bignum.
            , after format=bignum.
            , pct_step format=percent.
            , pct_tot format=percent.
            , cond
        from result
        ;quit;
			
	%rm(result);
%mend;

*==========================================================================================================================;
* ADDED SINCE 2020June30
*==========================================================================================================================;
%macro unstack(din, index, col, value, dout);
	proc transpose data=&din. out=&dout.(drop=_NAME_) prefix=&value._;
		id &col.;
		by &index.;
		var &value.;
		run;
%mend;

%macro gbsum(din, by, value, dout);
    %local bycomma;
    %let bycomma = %prxchange(%nrstr(s/\s+/, /), -1, &by.);
    proc sql;
        create table &dout. as
        select	&bycomma.
            , sum(&value.) as &value.
        from	&din.
        group by &bycomma.
        ;quit;
%mend;

%macro agg(din, index, by, value, dout);
    %local bycomma;
    %gbsum(&din., &index. &by., &value., __tmp_);
    %let bycomma = %prxchange(%nrstr(s/\s+/, /), -1, &by.);
    data __tmp_;
        set __tmp_;
        cat = catx('_', &bycomma.);
        run;
    %sort(__tmp_, &index., nodupkey=0);
    %unstack(__tmp_, &index., cat, &value., dout=&dout.);
    %rm(__tmp_);
%mend;


%macro im(left, right, on, out, cond, validate);
	%default_to(left, &__with__.)
	%default_to(out, &__with__.)
    %local title;
    %let title=1;
    title "Merge &left. + &right. (&on., &cond., &validate.)";
    
    %if "&validate." ne "" %then %do;
        proc sort data=&left. out=_ dupout=_left nodupkey; by &on.;run;
        proc sort data=&right. out=_ dupout=_right nodupkey; by &on.;run;
        %local dupsleft dupsright;
        %nobs(_left, dupsleft); %nobs(_right, dupsright);

        %if "&validate." = "1:1" %then %do;
            * provide more detailed information;
            %local errmsg; %let errmsg =;
            %if &dupsleft. ne 0 and &dupsright. ne 0 %then 
                %let errmsg = Not a &validate. merge %nrstr(,) &left. and &right. have duplicated keys;
            %if &dupsleft. ne 0 and &dupsright. = 0 %then
                %let errmsg = Not a &validate. merge %nrstr(,) &left. has duplicated keys;
            %if &dupsleft. = 0 and &dupsright. ne 0 %then 
                %let errmsg = Not a &validate. merge %nrstr(,) &right. has duplicated keys;
            %if "&errmsg." ne "" %then %do;
            	title2 "Failed";
            	%local key keycomma;
				%let keycomma = %prxchange(%nrstr(s/\s+/, /), -1, &on.);
				title4 "Dups on &left.";
				%dups(&left., %bquote(&keycomma.), title=0);
				title4 "Dups on &right.";
				%dups(&right., %bquote(&keycomma.), title=0);
            	title;
				title2;
				title4;
            	%raise(&errmsg.);
            %end;
		%end;
        %else %if "&validate." = "1:m" %then %do;
            %if &dupsleft. ne 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &left. has duplicated keys);
            %end;
        %else %if "&validate." = "m:1" %then %do;
            %if &dupsright. ne 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &right. has duplicated keys);
            %end;
        %else %if "&validate." ne "m:m" %then
            %raise(%nrstr(Invalid parameter validate=)"&validate.");
		%end; *validate;
	
	%merge(&left., &right., &on., &out., cond=&cond., title=0);
	title;
	title2;
	title4;
%mend;

%macro fillna(var, value);
	if missing(&var.) then &var. = &value.;
%mend;

%macro make_format(name, code, others);
	%default_to(others, 1);
	proc format;
	value &name.
		%unquote(&code.)
		%if &others. = 1 %then %do;
			other = "Others"
		%end;
	;
	run;
%mend;

%global __market__;
%let __market__=amh;
%macro mkconcat(din, yymm, dlist, dout);
	/*
	Feature Template
	----------------
	%macro [&__market__.]_[feature_name](yymm);
		data [feature_name]_&yymm.;
			* include "cust_id" and other features;
			run;
	%mend;
	*/
    %local dexist;
    %local dataparts;
    %local dnameyymm;
	%default_to(din, &__with__.)
	%default_to(dout, &__with__.)

    %foreach(dname, &dlist., %nrstr(
        %let dnameyymm = &dname._&yymm.;
        %let dataparts = &dataparts. &dnameyymm;
        %data_exists(&dnameyymm., rvar=dexist);
        %macro _mkconcatwork;
            %if &dexist. = 0 %then %do;
            	%put Data &dnameyymm. creating..;
	            %&__market__._&dname.(&yymm.);
	        %end;
	        %else %do;
	        	%put Data &dnameyymm. exists.;
	        %end;
        %mend _mkconcatwork; %_mkconcatwork;
    ));
    
    %concat(&din., &dataparts., cust_id, &dout., sort=1);
%mend;


%macro format(value,format);
	%if %datatyp(&value)=CHAR
		%then %sysfunc(putc(&value,&format));
		%else %left(%qsysfunc(putn(&value,&format)));
%mend format;


%macro import(datafile, dbms, dout, replace);
    %default_to(replace, 1);
    proc import datafile="&datafile."
        dbms=&dbms.
        out=&dout.
        %if &replace. = 1 %then %do;
            replace
        %end;
        ;
        getnames=yes;
        run;
%mend;


*==========================================================================================================================;
* ADDED SINCE 2020FEB10
*==========================================================================================================================;
%macro sleep();
	%for(1801, 9999, yymm, %nrstr(
		data a;
			t = sleep(50000*5);
			b = 1;
			run;
		proc print data=a(obs=1);
	));
%mend;

%macro append(list, item, sep=%str( ));
	%let &list. = &&&list..&sep.&item.;
%mend;

%macro concat(_base, _parts, _key, _dout, sort=0);
	* merge a list of data;
	data __mg_tmp;
		set &_base.;
		run;
	%foreach(din, &_parts., %nrstr(
		%macro _concatwork; %if &sort.=1 %then %do; 
			proc sort data=&din. out=_s_&din. nodupkey; by &_key; run;
			%cm(__mg_tmp, _s_&din., &_key., __mg_tmp, cond=(ina));
			%rm(_s_&din.);
		%end; 
		%else %do;
			%cm(__mg_tmp, &din., &_key., __mg_tmp, cond=(ina));
		%end;
		%mend _concatwork; %_concatwork;
	));
	data &_dout.;
		set __mg_tmp (drop=_ina _inb _inboth);
		run;
	%rm(__mg_tmp);
%mend;

%global __DATAMARTS__;
%let __DATAMARTS__=;

* Initialization;
%let CONFIG_FILE=abs-path-to-config.xlsx;
proc import datafile="&CONFIG_FILE."
			out=__config_libs
			dbms=xlsx
			replace;
		getnames=yes;
		sheet='Libs';
	run;


%macro datamarts();
	data _null_;
		set __config_libs;
		_key = upcase(key);
		put _key;
		run;
%mend;

%macro attach(datamart, as=);
	%global __DATAMARTS__;
	%local bFound libname;
	%let libname=;
	%let bFound=0;
	data _null_;
		set __config_libs;
		if upcase("&datamart.") = upcase(key) then do;
			call symput('libname', key);
			if "&as." ne "" then do;
				call symput('libname', "&as.");
			end;
			call execute('%let __DATAMARTS__=&__DATAMARTS__. &libname.;');
			call symput('repl', repl);
			call execute('&repl.');
			call symput('bFound', 1);
		end;
		run;
	%if &bFound.=0 %then %raise(Datamart &datamart. is not found.);
%mend;


%macro detach(datamart);
	libname &datamart. clear;
%mend;

%macro detach_all();
	%foreach(data, &__datamarts__., %nrstr(
		%detach(&data.);
	));
	%let __datamarts__ =;
%mend;
*==========================================================================================================================;
* BEFORE 2020FEB10
*==========================================================================================================================;

* Debugging========================;

%macro set_debug(status);
	/* Set debugging status on/off.
	In debugging mode, macro debugging related options are enabled. This 
	writes more information of macro excution to the log, but can be 
	verbose in production.

	Parameters
	==========
	status : on / off
		on 	- enables mlogic, mprint
		off - disables mlogic, mprint
		
	Example
	=======
	%set_debug(on); -> turn on debugging
	%set_debug(off);-> turn off debugging
	*/
	%global __DEBUG__;
	%if "&status." = "on" %then %do;
		%let __DEBUG__ = 1;
		* Enable macro debugging options;
		option mlogic mprint symbolgen;
	%end;
	%else %do;
		%if "&status." = "off" %then %do;
			%let __DEBUG__ = 0;
			* Disable macro debugging options;
			option nomlogic nomprint nosymbolgen;
		%end;
		%else %do;
			%raise(set_debug: invalid argument &status.);
		%end;
	%end;
%mend;


%macro set_lobs(status);
	/* Set lobs status on/off.

	Example
	=======
	%set_lobs(on); -> turn on limit obs
	%set_lobs(off);-> turn off limit obs
	*/
	%global __LOBS__;
	%if "&status." = "on" %then %do;
		%let __LOBS__ = 1;
	%end;
	%else %do;
		%if "&status." = "off" %then %do;
			%let __LOBS__ = 0;
		%end;
		%else %do;
			%raise(set_lobs: invalid argument &status.);
		%end;
	%end;
%mend;

%macro is_debug;
	/* Return the debugging status. Default value is 0. */
	%if %symexist(__DEBUG__) %then %do; 
		&__DEBUG__.
	%end;
	%else %do;
		0
	%end;
%mend;

%macro is_lobs;
	%if %symexist(__LOBS__) %then %do; 
		&__LOBS__.
	%end;
	%else %do;
		0
	%end;
%mend;

%macro ifdebug(funcargs);
	/* Execute the code if in debugging mode.
	User should use %nrstr to quote special characters in SAS code.*/
	%if %is_debug %then %do;
		%unquote(&funcargs.)
	%end;
%mend;

%macro lobs(n=1000);
	/* Limit number of observations if debugging mode is on, otherwise it will not
	not limit the observations.

	This is the debugging-aware version of data(obs=1000).

	Example
	=======
	data sample;
		set sashelp.africa (%lobs);
		run;
	*/
	%if %is_debug or %is_lobs %then %do;
		obs=&n
	%end;
%mend;

%macro data_exists(din, rvar=none);
	%let __tmpvar = %sysfunc(exist(&din.));
	%if "&rvar." = "none" %then %do;
		&__tmpvar.
	%end;
	%else %do;
		%let &rvar. = &__tmpvar.;
	%end;
%mend;
* UTILS============================;

%macro MAE(var_base, var_event, var_mae);
	%default_to(var_mae, mae);
	__year_evt = int(&var_event. / 100);
	__month_evt = &var_event. - __year_evt * 100;
	__year_base = int(&var_base. / 100);
	__month_base = &var_base. - __year_base * 100;
	&var_mae. = ((__year_evt * 12) +  __month_evt)
		- ((__year_base * 12) +  __month_base)
		;
	if &var_mae. >= 0 then &var_mae. = &var_mae. + 1;		
	drop __year_evt __month_evt __year_base __month_base;
%mend;


%macro gbsize(din, base_gb, add_gb, metric, id=cust_key, type=did, dout="");
	data __tmp;
		set &din.;
		ind = 0;
		%if "&type." = "did" %then %do;
			if missing(&metric.) then delete;
		%end;
		%else %do;
		%end;%if "&type." = "could" %then %do;
			if missing(datamonth) or datamonth < ntbmonth then delete;
		%end;
		%else %if "&type." = "all" %then %do;
			if missing(&id.) then delete;
		%end;
		run;
	proc sql;
		create table __base as
		select &base_gb., count(distinct(cust_key)) as n_cust
		from __tmp
		group by &base_gb.
		order by &base_gb.
		;quit;
	%with_title(base, %nrstr(
		proc print data=__base;run;
	));
	proc sql;
		create table __metric as
		select	&base_gb., &add_gb.
				, count(*) as n_txn format=bignum.
				, sum(&metric.) as tot_&metric. format=bignum.
		from	__tmp
		group by &base_gb., &add_gb.
		order by &base_gb.
		;quit;
	%let keys = %prxchange(%nrstr(s/,/ /), -1, &base_gb.);
	%merge(__metric, __base, %str(&keys.), __mg, cond=(ina));
	data __result;
		set __mg;
		per_cust = tot_&metric. / n_cust;
		ts = tot_&metric. / n_txn;
		format per_cust bignum.;
		format ts bignum.;
		drop _:;
		proc sort; by descending tot_&metric. ;
		run;
	%if &dout. ne "" %then %do;
		data &dout.;
			set __result;
			run;
	%end;
	%else %do;
		proc print data=__result;run;
	%end;
	%rm(__tmp);
	%rm(__base);
	%rm(__metric);
	%rm(__mg);
	%rm(__result);
%mend;

%macro gbsize_simple(din, gb, sizing, id=cust_key, dout="");
	proc sql;
		%if &dout. ne "" %then %do;
		create table &dout. as
		%end;
		select	&gb.
				, count(distinct(&id.)) as n_cust format=bignum.
				, count(*) as n_txn format=bignum.
				, sum(&sizing.) as tot_&sizing. format=bignum.
				, calculated tot_&sizing. / calculated n_cust as per_cust format=bignum.
				, calculated tot_&sizing. / calculated n_txn as ts format=bignum.
		from	&din.
		group by &gb.
		order by tot_&sizing. DESC
		;quit;
%mend;

%macro hist_concat(start, end, data_stmt, dbase, key, dout);
	%for(&start., &end., yymm, %nrstr(
	    data __hist_tmp_&yymm.;
	        %unquote(&data_stmt.)
	        run;
	    %cm(&dbase., __hist_tmp_&yymm., &key., __hist_mg_&yymm., cond=(ina));
	    %rm(__hist_tmp_&yymm.);
	));
	data &dout.;
	    set %for(&start., &end., yymm, %nrstr(
	        __hist_mg_&yymm.
	    ));
    run;
    %for(&start., &end., yymm, %nrstr(
	    %rm(__hist_mg_&yymm.);
	));
%mend;

%macro event_view(
    devent,
    evt_date_var,
    dmetric,
    merge_key,
    startyymm,
    endyymm,
    dout,
    monthly_summary=none
    );
    %for(&startyymm., &endyymm., yymm, %nrstr(
        %macro _work;
			data container&yymm.;
				set &devent.;
				datamonth = &yymm.;
				run;
            %checkmerge(container&yymm., &dmetric.&yymm., &merge_key., mt&dmetric.&yymm., cond=(ina));
            %if "&monthly_summary." ne "none" %then %do;
                %let from_=mt&dmetric.&yymm.;
                %let to_=smry&dmetric.&yymm.;
                %unquote(&monthly_summary.)
                %let from_=;
                %let to_=;
            %end;
			%rm(container&yymm.);
        %mend _work; %_work;
    ));

    data &dout.;
        set 
            %if "&monthly_summary." ne "none" %then %do;
                %for(&startyymm., &endyymm., yymm, %nrstr(
                    smry&dmetric.&yymm.
                ));
            %end;
            %else %do;
                %for(&startyymm., &endyymm., yymm, %nrstr(
                    mt&dmetric.&yymm.
                ));
            %end;
        run;

    %if "&monthly_summary." = "none" %then %do;
        data &dout.;
            set &dout.;
            %MAE(&evt_date_var., datamonth);
            run;
    %end;
    
%mend;


%macro date_to_yymm(var);
	/* convert SAS date to numerical YYMM format */
	input(put(&var., yymmn4.), 4.)
%mend;

%macro stop_on_error;
	%if &syserr. ne 0 %then %do;
		%raise(An error has occurred.);
	%end;
%mend stop_on_error;

%macro with_title(title, funcargs);
	title "&title.";
	%unquote(&funcargs.)
	title "SAS Program";
%mend;

%macro peek(din, n);
	%default_to(n, 10);
	proc print data=&din. (obs=&n.);run;
%mend;
%macro p(din, n);
	%default_to(din, &__with__.)
	%peek(&din., n=&n.);
%mend;

%macro p2(n);
	%default_to(n, 10);
	proc print data=&__with__. (obs=&n.);run;
%mend;

%macro sample(din, n=10);
	/* Randomly draw a sample of *n* observations*/
	proc surveyselect data=&din. noprint
		method=srs n=&n. out=__temp;
	run;
	title "Sample of &din.";
	proc print data = __temp;run;
	title;
	proc delete data=__temp;run;
%mend;

%macro raise(s);
	/* Write an error message to the log and stop executing
	submitted SAS code. */
	%put ERROR: &s.;
	* stop executing SAS code;
	%abort cancel;
%mend;

%macro print(s);
	/* Print a string to the HTML output. */
	data _null_;
	file print;
	put &s.;
	run;
%mend;

%macro rm(dataset);
	/* Remove a dataset. 
	This is a shortcut for proc delete.
	*/
	proc delete lib=work data=&dataset.;run;
%mend;

%macro levels(din, key, n=10);
	proc sql;
		create table __tmp as
		select &key.
				, count(*) as N
		from &din.
		group by &key.
		order by n desc
		;
		create table __tmp2 as
		select &key., n
				, n / sum(n) * 100 as pct format=3.0
		from __tmp
		;quit;
	%peek(__tmp2, n=&n.);
	%rm(__tmp);
	%rm(__tmp2);
%mend;

%macro nobs(dsn, tovar);
	/* Get the number of observations in a dataset. 
	https://communities.sas.com/t5/SAS-Communities-Library/Determining-the-number-of-observations-in-a-SAS-data-set/ta-p/475174
	*/
	%local __nobs __dsnid;
	%let __nobs=.;
	
	%* Open the data set of interest;
	%let __dsnid = %sysfunc(open(&dsn));
	
	%* If the open was successful get the;
	%* number of observations and CLOSE &dsn;
	%if &__dsnid %then %do;
		%let __nobs=%sysfunc(attrn(&__dsnid,nlobs));
		%let rc  =%sysfunc(close(&__dsnid));
	%end;
	%else %do;
		%raise(Unable to open &dsn - %sysfunc(sysmsg()));
	%end;
	
	%let &tovar. = &__nobs.;
%mend nobs;
* =================================;

%macro plus(tovar, __n);
    /* Equivalent to &tovar. += &n. */
    %let &tovar.=%eval(&&&tovar..+&__n.);
%mend;

%macro inc_(tovar, __n);
	%plus(&tovar., &__n.);
%mend;

%macro mod(value, __n);
	/* Equivalent to mod(10,3) -> 1 */
	%sysfunc(mod(&value., &__n.))
%mend;

%macro ismissing(param);
  %sysevalf(%superq(param)=,boolean)
%mend ismissing;

%macro default_to(mvar, value);
	%if %ismissing(&&&mvar..) %then %let &mvar. = &value.;
%mend;

%macro freq(data, tables, order);
	%default_to(data, &__with__.)
	%default_to(order, freq);
	proc freq data=&data. order=&order.;
		table &tables.
			/ list missing;
		run;
%mend;

%macro sort(data, by, nodupkey, out);
	%local data_stmt dup_stmt out_stmt;
	%default_to(nodupkey, 1);
	%let data_stmt=;
	%if %ismissing(&data.) %then %do;
		%let data_stmt=;
	%end;%else %do;
		%let data_stmt=%str(data=&data.);
	%end;
	%let dup_stmt = nodupkey ;
	%if &nodupkey. = 0 %then %do;
		%let dup_stmt =;
	%end;
	%let out_stmt = %str(out=&out.) ;
	%if %ismissing(&out.) %then %do;
		%let out_stmt = ;
	%end;
	proc sort &data_stmt. &dup_stmt. &out_stmt.; by &by.;
%mend;

%macro missing(din, var);
	title "Missing Values of &din. -> &var.";
	proc sql;
		create table _tot as
		select count(*) as Total
		from &din.
		;
		create table _notmissing as
		select count(*) as Not_Missing
		from &din.
		where not missing(&var.)
		;
		select a.Total format=bignum.
			, b.Not_Missing format=bignum.
			, (a.Total - b.Not_Missing) as Missing format=bignum.
		from _tot as a , _notmissing as b
		;quit;
	title;
	%rm(_tot); %rm(_notmissing);
%mend;

%macro prxchange(pat, n, str);
	/* Apply the regex on the string and return the result.
	Example
	=======
	%put %prxchange(s/a/xxx/, -1, halo); --> hxxxlo

	Parameters
	==========
	pat : perl-regular-expression.
		No quotation mark.
	n : int.
		Number of occurances to replace. -1 to replace all occurances.
	str : string.
		String to apply regex. No quotation mark.
	
	Returns
	=======
	Result string.
	*/
	%sysfunc(PRXCHANGE(&pat., &n., &str.))
%mend;

%macro logfile(path);
	/* 
	Example:
	%logfile(testlog.log); */
	filename _logfile "&path.";
	proc printto log=_logfile new;run;
%mend;

%macro logstd();
	/* Direct log to console (standard output) */
	proc printto;run;
%mend;

%macro imerge(left, right, on, out, validate=, how=left, title=1);
	/* Merge data sets; Report Merge statistics.

    Parameters
    ==========
    how      : str in [left, right, inner]
    validate : optional, str in [1:1, 1:m, m:1, m:m]
	*/
    %local cond;
    %let cond=.;
    %if "&how." = "left" %then 
        %let cond=ina;
    %else %if "&how." = "right" %then
        %let cond=inb;
    %else %if "&how." = "inner" %then
        %let cond=ina and inb; 
    %else
        %raise(%nrstr(Invalid parameter how=)"&how.");

    %if "&validate." ne "" %then %do;
        proc sort data=&left. out=_ dupout=_left nodupkey; by &on.;run;
        proc sort data=&right. out=_ dupout=_right nodupkey; by &on.;run;
        %local dupsleft dupsright;
        %nobs(_left, dupsleft); %nobs(_right, dupsright);

        %if "&validate." = "1:1" %then %do;
            * provide more detailed information;
            %if &dupsleft. ne 0 and &dupsright. ne 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &left. and &right. have duplicated keys);
            %if &dupsleft. ne 0 and &dupsright. = 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &left. has duplicated keys);
            %if &dupsleft. = 0 and &dupsright. ne 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &right. has duplicated keys);
		%end;
        %else %if "&validate." = "1:m" %then %do;
            %if &dupsleft. ne 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &left. has duplicated keys);
            %end;
        %else %if "&validate." = "m:1" %then %do;
            %if &dupsright. ne 0 %then 
                %raise(Not a &validate. merge %nrstr(,) &right. has duplicated keys);
            %end;
        %else %if "&validate." ne "m:m" %then
            %raise(%nrstr(Invalid parameter validate=)"&validate.");
		%end; *validate;

	data &out.;
		merge &left. (in=ina)
			&right. (in=inb)
			;
		by &on;
		if &cond.;
		_ina=.;
		_inb=.;
		_inboth=.;
		if ina then _ina=1;
		if inb then _inb=1;
		if ina and inb then _inboth=1;
	run;

	%if &title.=1 %then %do;
		%if "&validate." ne "" %then %do;
			title "Merge (&validate.)";
		%end;
		%else %do;
			title "Merge";
		%end;
		title2 "&cond.";
		title3 "&left. + &right. (&on.) -> &out.";
	%end;
	
	proc sql;
		select (select count(*) from &left.) as obs_in_left format bignum.,
			(select count(*) from &right.) as obs_in_right format bignum.,

			count(_inboth) as obs_matched format bignum.,
			count(*) as obs_in_result format bignum.,
			count(_inboth) / calculated obs_in_left as pct_left_matched format percent10.1,
			count(_inboth) / calculated obs_in_right as pct_right_matched format percent10.1
		from &out.
		;quit;
	%if &title.=1 %then %do;
		title;
		title2;
		title3;
	%end;
%mend;

%macro merge(master, joiner, key, out, title=1, cond=(ina or inb));
	/* Merge data sets; Report Merge statistics.

	cond=(ina or inb ) => full  join
	cond=(ina        ) => left  join
	cond=(inb        ) => right join
	cond=(ina and inb) => inner join
	*/
	%put invoking mymerge function...;
	%put master table ---> &master;
	%put joiner table ---> &joiner;
	%put output table ---> &out;
	data &out.;
		merge &master. (in=ina)
			&joiner. (in=inb)
			;
		by &key;
		if &cond.;
		_ina=.;
		_inb=.;
		_inboth=.;
		if ina then _ina=1;
		if inb then _inb=1;
		if ina and inb then _inboth=1;
	run;

	%if &title.=1 %then %do;
		title "Merge";
		title2 "&cond.";
		title3 "&master. + &joiner. (&key.) -> &out.";
	%end;
	
	proc sql;
		select (select count(*) from &master.) as obs_in_master format bignum.,
			(select count(*) from &joiner.) as obs_in_joiner format bignum.,
			sum(_ina) as left_matched format bignum.,
			count(_inboth) as both_matched format bignum.,
			sum(_inb) as right_matched format bignum.,
			count(*) as obs_in_result format bignum.,
			count(_inboth) / calculated obs_in_master as pct_master_matched format percent10.1,
			count(_inboth) / calculated obs_in_joiner as pct_joiner_matched format percent10.1
		from &out.
		;quit;
	%if &title.=1 %then %do;
		title;
		title2;
		title3;
	%end;
%mend;

%macro fulljoin(master, joiner, key, out);
	* for backward compatibility;
	%merge(&master., &joiner., &key., &out.);
%mend;

%macro dups(din, keys, title=1);
	/* Print statistics of duplicates on *keys*. Multiple keys are separated by comma.*/
	%if &title.=1 %then %do;
		title3 "Dups on &din (&keys)";
	%end;
	proc sql;
		create table _dupcount as
		select &keys., count(*)-1 as N
		from &din
		group by &keys.
		;
		select 	sum(N) + count(*) as num_obs format bignum.,
			count(*) as num_levels format bignum.,
			(select count(*) from _dupcount where N=0) / calculated num_levels as pct_unique_level format percent10.1,
			sum(N) as total_dups format bignum.,
			calculated total_dups / calculated num_obs as pct_dup format percent10.1,
			calculated total_dups / (calculated num_levels * (1-calculated pct_unique_level)) as avg_dup_cnt
		from _dupcount
		;quit;

	%if &title.=1 %then %do;
	title3;
	%end;
%mend;

%macro dup_stats(din, keys);
	/*backward compatibility. */
	%dups(&din., &keys.);
%mend;


%macro checkmerge(master, joiner, key, out, cond=(ina or inb), sort=0);
	/* Report duplicated keys in both `master` and `joiner` data; Merge the data; 
	and Report Merged Statistics. */
	%local key keycomma;
	%let keycomma = %prxchange(%nrstr(s/\s+/, /), -1, &key.);

	%if &sort. = 1 %then %do;
		proc sort data=&master.; by &key.;run;
		proc sort data=&joiner.; by &key.;run;
	%end;
	
	title "CheckMerge";
	title2 "type: &cond.";
	title3 "&master. + &joiner. (&key.) -> &out.";
	title4 "Dups on &master.";
	%dups(&master., %bquote(&keycomma.), title=0);
	title;
	title2;
	title3;
	title4 "Dups on &joiner.";
	
	%dups(&joiner., %bquote(&keycomma.), title=0);
	title4 "Merged to &out.";
	%merge(&master., &joiner., &key, &out, title=0, cond=&cond.);
	title4;
%mend;
%macro cm(master, joiner, key, out, cond=(ina or inb), sort=0);
	%checkmerge(&master., &joiner., &key., &out., cond=&cond., sort=&sort.);
%mend;

%macro move_yymm(yymm, n, tovar);
	/* Move from `yymm` by `n` months, store results in `tovar`.

	Implemented purely in macro so it can be invoked anywhere in 
	the SAS program.

	Example 
	=======
	%local newyymm;
	%move_yymm(1801,-2, newyymm);
	%put &newyymm.; => 1711
	*/
	%local yymm n i cur mod;
	%let cur = &yymm.;
	%if &n. > 0 %then %do;
		%do i=1 %to &n;
			%let cur = %eval(&cur.+1);
			%let mod = %sysfunc(mod(&cur.,100));
			%if &mod.>12 %then %do;
				%let cur = %eval(&cur. + 88);
			%end;
		%end;
	%end;
	%else %do;
		%let n = %eval(-&n.);
		%do i=1 %to &n;
			%let cur = %eval(&cur.-1);
			%let mod = %sysfunc(mod(&cur.,100));
			%if &mod.=0 %then %do;
				%let cur = %eval(&cur. - 88);
			%end;
		%end;
	%end;
	%let &tovar.=&cur.;
%mend;


%macro loopfunc(func, yymm, start, end);
	/* Example
	%loopfunc(print, 1810, -3, 0);
	>>>
		%print(1807);
		%print(1808);
		%print(1809);
		%print(1810);
	*/
	%local func yymm start end _tempyymm i;
	%do i=&start. %to &end.;
		%move_yymm(&yymm., &i., _tempyymm);
		%&func.(&_tempyymm.);
	%end;
%mend;

%macro loopfunc_yymm(func, start, end);
	/* Example
	%loopfunc_yymm(print, 1710, 1801);
	>>> %print(1710);
		%print(1711);
		%print(1712);
		%print(1801);
	*/
	%local func start end _tempyymm _nextyymm;
	%let _tempyymm = &start.;
	%do %while (&_tempyymm. <= &end.);
		%&func.(&_tempyymm.);
		%move_yymm(&_tempyymm., 1, _nextyymm);
		%let _tempyymm = &_nextyymm.;
	%end;
%mend;

%macro foreach(__var, __list, funcargs);
	%local __i funcargs parsed &__var.;
	%let __i = 1;
	%do %while (%scan(&__list., &__i., ' ') ne );
	    %let &__var. = %scan(&__list., &__i., ' ');
        %unquote(&funcargs.)
        %let __i = %eval(&__i. + 1);
    %end;
%mend;


%macro forzip(var1, var2, list1, list2, funcargs, delimiter1=" ", delimiter2=" ");
	/* Loop through two zipped lists.
	
	list1, list2 must have same number of elements.
	
	Example
	========
	%forzip(k,v, 1 2 5, a b e, %nrstr(
		%put &k. = &v.;
	));
	*/
	%local __i funcargs parsed &var1. &var2.;
	%let __i = 1;
	%do %while (%qscan(&list1., &__i., &delimiter1.) ne );
	    %let &var1. = %qscan(&list1., &__i., &delimiter1.);
	    %let &var2. = %qscan(&list2., &__i., &delimiter2.);
        %unquote(&funcargs.)
        %let __i = %eval(&__i. + 1);
    %end;
%mend;

%macro for(start, end, yymmvar, funcargs, autostop=1);
	/* Execute programs in `funcargs` by assigning month (yymm format)
	values from `start` to `end` to the `yymmvar`. 

	Example
	=======
	%for(1401, 1402, yymm, %nrstr(
		%put &yymm.;
	));
	*/
    %local funcargs start end parsed &yymmvar. _nextyymm;
    %let &yymmvar. = &start.;
    %do %while (&&&yymmvar.. <= &end.);
        %unquote(&funcargs.)
        %let a = %move_yymm(&&&yymmvar.., 1, _nextyymm);
        %let &yymmvar. = &_nextyymm.;
		%if &autostop. = 1 %then %do;
			%stop_on_error
		%end;
    %end;
%mend;


%macro loopyymm(funcargs, start, end);
	/* Take a piece of SAS code (funcargs), resolve the '&yymm.' within the code
	to each month between the `start` and `end`, respectively. 

	Example
	=======
	%loopyymm(%nrstr(%print(&yymm.);),
		1801,
		1803);

	Is equivalent of writing:

	%print(1801);
	%print(1802);
	%print(1803);
	*/
	%local funcargs start end parsed yymm _nextyymm;
	%let yymm = &start.;
	%do %while (&yymm. <= &end.);
		%unquote(&funcargs.)
		%let a = %move_yymm(&yymm., 1, _nextyymm);
		%let yymm = &_nextyymm.;
	%end;
%mend;

%macro diff_yymm(__start, __end, tovar);
	/*Returns number of months between the start and end, both 
	in yymm format. 
	
	Example
	=======
	%local diff;
	%diff_yymm(1802,1710, diff);
	%put &diff.; => -4
	*/
    %local step n;
    %if &__start. <= &__end. %then %do;
        %let step=1;
    %end;
    %else %do;
        %let step=-1;
    %end;

    %let n=0;
    %do %while (&__start. ne &__end.);
        %let n=%eval(&n.+&step.);
        %move_yymm(&__start., &step., __start);
    %end;
    %let &tovar.=&n.;
%mend;

%macro diff_month(__start, __end, tovar);
	%put DeprecationWarning: 'diff_month' is deprecated and will be removed;
	%put in the future. Use 'diff_yymm' instead.;
	%diff_yymm(&__start., &__end., &tovar.);
%mend;


* TEST UTILITIES============================;
%macro assertEQ(valueA, valueB, msg);
    %if &valueA. = &valueB. %then %do;
        * pass;
    %end;
    %else %do;
        %raise([AssertionError] &msg.);
    %end;
%mend;


%macro runtests;
	/* Run all tests */
	%set_debug(off);

	%put ==============================================;
	%put |          TEST SUITE of MACROS              |;
	%put ==============================================;

	* Test diff_yymm ;
	%local a b c diff;
	%diff_yymm(1401, 1402, diff);
	%assertEQ(&diff., 1, %nrstr(diff_yymm(1401, 1402, diff) failed.));
	%diff_yymm(1411, 1711, diff);
	%assertEQ(&diff., 36, %nrstr(diff_yymm(1411, 1711, diff) failed.));
	%diff_yymm(1411, 1312, diff);
	%assertEQ(&diff., -11, %nrstr(diff_yymm(1411, 1312, diff) failed.));

	* Test move_yymm ;
	%local todate;
	%move_yymm(1510, 3, todate);
	%assertEQ(&todate., 1601, %nrstr(move_yymm(1510, 3, todate) failed.));
	%move_yymm(1904, -12, todate);
	%assertEQ(&todate., 1804, %nrstr(move_yymm(1904, -12, todate) failed.));
	%move_yymm(1308, 0, todate);
	%assertEQ(&todate., 1308, %nrstr(move_yymm(1308, 0, todate) failed.));

	* Test for ;
	%local todate collect;
	%for(1810, 1902, yymm, %nrstr(
		%let diff = '';
		%diff_yymm(&yymm., 1810, diff);
		%let collect = &collect. &diff.;
		%let diff = '';
	), autostop=0);
	%assertEQ("&collect.", "0 -1 -2 -3 -4", %nrstr(%for(1810, 1902, yymm, [cmd]) failed.));

	* Test mod;
	%local x; %let x = %mod(10,3);
	%assertEQ(&x., 1, %nrstr(%mod(10,3) failed.));
	%assertEQ(%mod(517,10), 7, %nrstr(%mod(517,10) failed.));

	* Test plus;
	%local x; %let x=7;
	%plus(x, 51);
	%assertEQ(&x., 58, %nrstr(%plus(x, 51); failed.));
	%put ==============TESTS FINISHED==================;
%mend;

* Run the test when the library is loaded, it only takes a split
second anyway.;
%runtests;


%put Package &__package__.;
%put Version &__version__.;