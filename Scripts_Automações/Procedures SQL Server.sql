
CREATE SCHEMA [u] AUTHORIZATION [dbo]
GO


if object_id ( 'u.getNumSQLExec1more', 'FN' ) is not null
  drop function u.getNumSQLExec1more
GO

create function u.getNumSQLExec1more
(
  @prm_database       nvarchar(32) = NULL
) returns int as

begin

  declare @NumSessExec      integer = -1;

  select @NumSessExec = count(distinct req.session_id)
    from sys.dm_exec_requests req
   where req.session_id <> @@SPID and
        (@prm_database is null or req.database_id = db_id(@prm_database)) and
         req.command not in ('AWAITING COMMAND', 'WAITFOR') and 
		 req.status in ('suspended','running') and
	     cast(isnull(datediff(ss,req.start_time , getdate()),0) as decimal(10,0)) between 60 and 599

  return @NumSessExec
end

GO


if object_id ( 'u.getNumSQLExec10more', 'FN' ) is not null
  drop function u.getNumSQLExec10more
GO

create function u.getNumSQLExec10more
(
  @prm_database       nvarchar(32) = NULL
) returns int as

begin

  declare @NumSessExec      integer = -1;

  select @NumSessExec = count(distinct req.session_id)
    from sys.dm_exec_requests req
   where req.session_id <> @@SPID and
        (@prm_database is null or req.database_id = db_id(@prm_database)) and
         req.command not in ('AWAITING COMMAND', 'WAITFOR') and 
		 req.status in ('suspended','running') and
	     cast(isnull(datediff(ss,req.start_time , getdate()),0) as decimal(10,0)) > 600

  return @NumSessExec
end

GO

if object_id ( 'u.getNumSQLBlocked', 'FN' ) is not null
  drop function u.getNumSQLBlocked
GO

create function u.getNumSQLBlocked
(
  @prm_database       nvarchar(32) = NULL
) returns int as

begin

  declare @NumSessBlocked   integer = -1;

  select @NumSessBlocked = count(distinct owt2.session_id) 
   from sys.dm_os_waiting_tasks owt2
          join sys.dm_exec_sessions ses on
               ses.session_id = owt2.session_id
  where owt2.blocking_session_id is not null and 
        owt2.blocking_session_id <> owt2.session_id and
       (@prm_database is null or ses.database_id = db_id(@prm_database));

  return @NumSessBlocked
end

GO


if object_id ( 'u.getNumSQLExec', 'FN' ) is not null
  drop function u.getNumSQLExec
GO

create function u.getNumSQLExec
(
  @prm_database       nvarchar(32) = NULL
) returns int as

begin

  declare @NumSessExec      integer = -1;

  select @NumSessExec = count(distinct req.session_id)
    from sys.dm_exec_requests req
   where req.session_id <> @@SPID and
        (@prm_database is null or req.database_id = db_id(@prm_database)) and
         req.command not in ('AWAITING COMMAND', 'WAITFOR') and 
		 req.status in ('suspended','running')

		 
  return @NumSessExec
end

GO
		 
	
if object_id ( 'u.getNumSQLExecMinTempo', 'FN' ) is not null
  drop function u.getNumSQLExecMinTempo
GO

create function u.getNumSQLExecMinTempo
(
  @prm_database       nvarchar(32) = NULL,
  @prm_minTempo       int = -999
) returns int as

begin

  declare @NumSessExec      integer = -1;

  select @NumSessExec = count(distinct ses.session_id)
     from sys.dm_exec_sessions ses 
	 join sys.dm_exec_requests req on
          ses.session_id = req.session_id
   where ses.session_id <> @@SPID and
        (@prm_database is null or ses.database_id = db_id(@prm_database)) and
         ses.status = 'running' and
         req.command not in ('AWAITING COMMAND', 'WAITFOR') and
	     ses.login_name not in ('svcsbiis','svcsqldiag') and
         datediff(ss, req.start_time, getdate()) > @prm_minTempo;

  return @NumSessExec
end

GO



if object_id ( 'u.getNumSQLExecOrTrans', 'FN' ) is not null
  drop function u.getNumSQLExecOrTrans
GO

create function u.getNumSQLExecOrTrans
(
  @prm_database       nvarchar(32) = NULL
) returns int as

begin

  declare @NumSessExec      integer = -1;

select 
   @NumSessExec = count(ses.session_id)
  from sys.dm_exec_sessions ses
        left outer join sys.dm_tran_session_transactions trs on
             trs.session_id = ses.session_id			 
        left outer join sys.dm_tran_database_transactions trd on
             trd.transaction_id = trs.transaction_id and
			 -- inserido para ajustar duplicacao
			 trd.database_id = ses.database_id  
 where 
     ses.session_id <> @@SPID and
	 (@prm_database is null or ses.database_id = db_id(@prm_database)) and
    (
      (
        (ses.status <> 'sleeping')and
        ses.is_user_process <> 0
      ) or (
      ses.status <> 'sleeping' and
      ses.is_user_process = 0
      ) or (
           trd.transaction_id is not null and
           trd.database_transaction_state <> 3
           )
    ) and
         (
           trd.transaction_id is null or
           trd.database_transaction_state <> 3 or
           trd.database_id not in (32767, db_id('tempdb'))
         );

  return @NumSessExec
end

GO

if object_id ( 'u._sqlresumo', 'FN' ) is not null
  drop function u._sqlresumo
GO

create function u._sqlresumo
(
  @prm_sql            nvarchar(3950),
  @prm_maxparm        int = 100

) returns nvarchar(4000) as

begin
  declare @pos               integer
  declare @posCmd            integer
  declare @sql1              nvarchar(4000)


  if len(@prm_sql) > 1 and substring(@prm_sql, len(@prm_sql), 1) = nchar(0)
    set @sql1 = substring(replace(replace(replace(@prm_sql, char(13), ' '), char(10), ' ') , char(9), ' '), 1, len(@prm_sql)-1)
  else
    set @sql1 = replace(replace(replace(@prm_sql, char(13), ' '), char(10), ' ') , char(9), ' ')


  set @posCmd = 99999

  set @pos = charindex('SELECT', @prm_sql, 1)
  if @pos > 0 and @pos < @posCmd set @posCmd = @pos

  set @pos = charindex('WITH', @prm_sql, 1)
  if @pos > 0 and @pos < @posCmd set @posCmd = @pos

  set @pos = charindex('INSERT', @prm_sql, 1)
  if @pos > 0 and @pos < @posCmd set @posCmd = @pos

  set @pos = charindex('UPDATE', @prm_sql, 1)
  if @pos > 0 and @pos < @posCmd set @posCmd = @pos

  set @pos = charindex('DELETE', @prm_sql, 1)
  if @pos > 0 and @pos < @posCmd set @posCmd = @pos


--  if @posCmd != 99999 and @posCmd >= @prm_maxparm
--    begin
--    set @sql1 = replace(replace(replace(replace(@prm_sql, char(0), ''), char(13), ' '), char(10), ' ') , char(9), ' ')
--    set @sql1 = substring(@sql1, 1, @prm_maxparm) + '[...]   ' + substring(@sql1, @posCmd, 4000)
--    end
--  else
--    begin
--    set @sql1 = replace(replace(replace(replace(@prm_sql, char(0), ''), char(13), ' '), char(10), ' ') , char(9), ' ')
--    end

  if @posCmd != 99999 and @posCmd >= @prm_maxparm
    set @sql1 = substring(@sql1, 1, @prm_maxparm) + '[...]   ' + substring(@sql1, @posCmd, 4000)

  return rtrim(@sql1)
end

GO



if object_id ( 'u.lea2xsql', 'P' ) is not null
  drop procedure u.lea2xsql
GO


create procedure u.lea2xsql as
begin
  set NOCOUNT OFF
  set IMPLICIT_TRANSACTIONS OFF

if object_id(N'tempdb..#session',N'U') is not null
  drop table #session;

if object_id(N'tempdb..#wait',N'U') is not null
  drop table #wait;
  
select 
  ses.session_id as SES_ID,
  substring(db_name(ses.database_id),1,13) as DB_NAME,
  substring (substring (ses.login_name, charindex ('\', ses.login_name) + 1, 15), 1, 18) as LOGIN_NAME,
  substring(ses.status, 1, 15) as SES_STATUS,
  (case when ses.last_request_start_time > ses.last_request_end_time
        then ses.last_request_start_time
        else ses.last_request_end_time
  end) as LAST_OPERATION,
  case when ses.last_request_end_time > dateadd(day,-15,getdate()) or ses.last_request_start_time > dateadd(day,-15,getdate())
       then (cast(isnull(datediff(ms, case when ses.last_request_start_time > ses.last_request_end_time
                                           then ses.last_request_start_time
                                           else ses.last_request_end_time
       end, getdate()), 0) / 1000.0 as decimal(12,3))) 
	     else 0 
  END as LASTOP_SECs,
  ses.login_time as start_session, 		
  case when trs.transaction_id is not null
       then substring(isnull(convert(varchar, trd.database_transaction_begin_time, 20), 'NO LOG') + ' (' + cast(isnull(trs.transaction_id, -1) as varchar) +')', 1, 32)
       else ''
  end as 'TRANSACTION_START',
  case when trd.database_transaction_begin_time is not null
       then right(replicate(' ',11) + cast(cast(isnull(datediff(ms, trd.database_transaction_begin_time, getdate()),0) / 1000.0 as decimal(10,3)) as varchar), 11)
       else ''
  end as TRANS_SECs,
  (case when trd.database_transaction_type = 1     then 'R/W'
        when trd.database_transaction_type = 2     then 'R/O'
        when trd.database_transaction_type = 3     then 'SYS'
        when trd.database_transaction_type is null then ''
        else '???'
  end) as TR_TYPE,
  cast (case when trd.database_transaction_state is null then ''
        when trd.database_transaction_state =  1    then 'Not init '
        when trd.database_transaction_state =  3    then 'No log '
        when trd.database_transaction_state =  4    then 'With log '
        when trd.database_transaction_state =  5    then 'Prepared '
        when trd.database_transaction_state = 10    then 'Commited '
        when trd.database_transaction_state = 11    then 'Rolled back '
        when trd.database_transaction_state = 12    then 'Commiting '
        else ''
  end + isnull ('[' + cast(trd.database_transaction_state as varchar) + ']', '') as varchar(17)) as TRANSACTION_STATE,	  
  substring (ses.program_name, 1, 25) as PROGRAM_NAME,
  substring (ses.host_name, 1, 20) as HOST_NAME,
  ses.host_process_id as HPID,
  coalesce(trd.database_transaction_log_bytes_used/1024,0) AS LogUsedKB
  into #session
from 
  sys.dm_exec_sessions ses
  left outer join sys.dm_tran_session_transactions trs on
             trs.session_id = ses.session_id			 
  left outer join sys.dm_tran_database_transactions trd on
             trd.transaction_id = trs.transaction_id and
             -- inserido para ajustar duplicacao
             trd.database_id = ses.database_id 
where 
  ses.session_id <> @@SPID and
  (
    (
      (ses.status <> 'sleeping')and
      ses.is_user_process <> 0
    ) or 
    (
      ses.status <> 'sleeping' and
      ses.is_user_process = 0
    ) or 
    (
      trd.transaction_id is not null /*and
      trd.database_transaction_state <> 3*/
    )
    ) and
    (
      trd.transaction_id is null or
      trd.database_transaction_state <> 3 or
      trd.database_id not in (32767, db_id('tempdb'))
    )


 
select
  owt.blocking_session_id,
  owt.blocking_exec_context_id,
  owt.session_id
  into #wait
from sys.dm_os_waiting_tasks owt
where		 
  (

    (
      owt.blocking_session_id is not null and 
      owt.blocking_session_id <>  owt.session_id
    )
    or
    (
      owt.blocking_session_id is null and 
     (owt.wait_duration_ms * 1000 + owt.exec_context_id) = (select max(owt2.wait_duration_ms * 1000 + owt2.exec_context_id) from sys.dm_os_waiting_tasks owt2 where owt2.session_id = owt.session_id and owt2.wait_type = owt.wait_type)
    )
    or
    (
      owt.exec_context_id = 0 and
     (owt.blocking_session_id * 1000 + owt.blocking_exec_context_id) = (select min(owt2.blocking_session_id * 1000 + owt2.blocking_exec_context_id) from sys.dm_os_waiting_tasks owt2 where owt2.session_id = owt.session_id and owt2.exec_context_id = 0)
    )
  )



SELECT 
  SES.SES_ID,
  SES.DB_NAME,
  SES.LOGIN_NAME,
  case when req.status is null
       then substring(ses.SES_STATUS, 1, 15)
       else substring(ses.SES_STATUS, 1, 15) + '  (' + substring (rtrim(req.status), 1, 10) + ')' 
  end as "SES/REQ_STATUS",
  substring(coalesce(req.command,'AWAITING COMMAND'),1 , 20) as CMD,	   
  SES.LAST_OPERATION,
  SES.LASTOP_SECs,
  case  when owt.blocking_session_id is not null
        then right(replicate(' ',11) + 
          case when owt.blocking_session_id <> ses.SES_ID
               then cast(owt.blocking_session_id as varchar) + ' / ' + isnull(cast(owt.blocking_exec_context_id as varchar), '-')
               else '[p] / ' + isnull(cast(owt.blocking_exec_context_id as varchar), '-')
               end, 14)
          else 
          case when exists (select 1 from sys.dm_os_waiting_tasks owt2 where owt2.blocking_session_id = ses.SES_ID and owt2.session_id <> ses.SES_ID)
               then replicate(' ',11) + '***'
               else ''
          end
  end as LOCKWAIT_SID,	   
  cast (coalesce(req.wait_time,0) / 1000.0 as decimal(10,3)) as "WAIT_TIME (s)",	
  '',   
  SES.TRANSACTION_START AS "TRANSACTION (START/ID)",
  SES.TRANS_SECs, 
  '',
  SES.TRANSACTION_STATE, 
  CASE WHEN req.wait_type IS NOT NULL 
       THEN substring(req.wait_type,1,20) 
       WHEN req.last_wait_type IS NOT NULL 
       THEN 'last_wait_type: '+ substring(req.last_wait_type,1,20) 
       ELSE 'MISCELLANEOUS'
  END as WAIT_TYPE, 	   
  coalesce(substring (req.wait_resource, 1, 40),'') as WAIT_RESOURCE, 
  SES.PROGRAM_NAME,
  SES.HOST_NAME,
  substring (con.client_net_address + (case when con.client_tcp_port is null then '' else ':' + cast(con.client_tcp_port as char) end), 1, 22) as IP_ADDRESS,
  ses.START_SESSION,
  SES.HPID,
  (select cast(count(*) as smallint) from sys.dm_exec_connections con2 where con2.session_id = ses.SES_ID) as QT_CON,
  coalesce(req.row_count,0) as 'FETCH_ROWS',
  cast (coalesce(req.cpu_time,0) / 1000.0 as decimal(10,3)) as "CPU_TIME (s)",
  coalesce(req.logical_reads,0) as 'LOGICAL_READS',	
  coalesce(req.reads,0) as 'PHYSICAL_READS',	
  coalesce(mem.dop,0) as DOP,
  coalesce(mem.requested_memory_kb,0) as REQUESTED_MEMORY_KB,
  coalesce(mem.granted_memory_kb,0) as GRANTED_MEMORY_KB,
  coalesce(mem.max_used_memory_kb,0) as MAX_USED_MEMORY_KB,
  coalesce(mem.used_memory_kb,0) as USED_MEMORY_KB,
  ses.LogUsedKB as LOGUSED_KB,
  coalesce((tmp.user_objects_alloc_page_count - tmp.user_objects_dealloc_page_count)*8,0) as ACT_USER_TEMP_KB,
  coalesce((tmp.internal_objects_alloc_page_count - tmp.internal_objects_dealloc_page_count)*8,0) as ACT_INT_TEMP_KB,
  isnull(req.statement_start_offset,'-') as STRT_OFFS, 
  isnull(req.statement_end_offset,'-') as END_OFFS,     
  substring(req.sql_handle,1,50) as SQL_HANDLE,	
  substring(req.plan_handle,1,50) as PLAN_HANDLE,	
  CASE  WHEN req.sql_handle is not null 
        THEN substring ((select u._sqlresumo(sql.TEXT, 80) from sys.dm_exec_sql_text((req.sql_handle)) sql) , 1 ,300)
        WHEN (req.sql_handle is null and con.most_recent_sql_handle is not null) 
        THEN 'last_Text: ' + substring ((select u._sqlresumo(sql.TEXT, 80) from sys.dm_exec_sql_text((con.most_recent_sql_handle)) sql) , 1 ,300) 
        ELSE 'NULL' 
  END as SQL_TEXT	   
FROM 
  #session ses
  left outer join sys.dm_exec_requests req on
      req.session_id = ses.SES_ID
  left outer join sys.dm_exec_connections con on
      con.session_id = ses.SES_ID and
      con.parent_connection_id is null
  left outer join #wait owt on	
      owt.session_id      = ses.SES_ID 
  left outer join sys.dm_exec_query_memory_grants mem on
      ses.SES_ID     = mem.session_id and
      req.request_id = mem.request_id
  left outer join sys.dm_db_task_space_usage tmp on 
      req.session_id = tmp.session_id and
      req.database_id = tmp.database_id and
      req.request_id = tmp.request_id	 
order by
  LAST_OPERATION, 
  LASTOP_SECs desc, 
  ses.SES_ID
;

  print ""
  print "Numero de sessoes executando SQLs nesta base                         : " + cast(u.getNumSQLExec(NULL) as varchar(20));
  print "Numero de sessoes executando SQLs nesta instancia                    : " + cast(u.getNumSQLExecOrTrans(NULL) as varchar(20));
  print "Numero de sessoes executando SQLs nesta instancia  + 1 min           : " + cast(u.getNumSQLExec1more(NULL) as varchar(20));
  print "Numero de sessoes executando SQLs nesta instancia  + 10 min          : " + cast(u.getNumSQLExec10more(NULL) as varchar(20));  
  print "Numero de sessoes bloqueadas nesta base (ex: lock)                   : " + cast(u.getNumSQLBlocked(db_name(db_id())) as varchar(20));

  print ''
  set NOCOUNT OFF
  return 0
  
end

GO


if object_id ( 'u.leaqtexec2', 'P' ) is not null
  drop procedure u.leaqtexec2
GO

create procedure u.leaqtexec2 
(
  @prm_db        nvarchar(30) = NULL,
  @prm_minexec   int = 1,
  @prm_detail    int = 2
) as
begin
  set NOCOUNT ON

  declare @datastr          varchar(23)
  declare @maxlastOps       decimal(10,2)
  declare @qtOpsDem         int
  declare @qtOpsNaoSist     int
  declare @qtOpsDemNaoSist  int

  if @prm_db in ('?', '/?', '-?', '-h')
  begin
    print ""
    print "Formato: u.leaqtexec [@prm_minexec] [,@prm_db] [,@prm_detail]"
    print "  @prm_minexec   : numero minimo de execucoes repetidas (default = 1)"
    print "  @prm_db        : filtrar por um database (default = todos)"
    print "  @prm_detail    : 0 = menos detalhes, 1 = mais detalhes, 2 = completo (default)"
    print ""
    return 1
  end

  if @prm_detail = 0
  begin
    select cast(max(master.dbo.fn_varbintohexstr(req.sql_handle))  as varchar(100)) as SQL_HANDLE,
           count(*) as QTD,
           substring((select u._sqlresumo(sql.TEXT, 80) from sys.dm_exec_sql_text(req.sql_handle) sql), 1, 760) as SQL_TEXT
      from sys.dm_exec_sessions ses
             join sys.dm_exec_requests req on
                  req.session_id = ses.session_id
     where ses.session_id <> @@SPID and
          (@prm_db is null or ses.database_id = db_id(@prm_db)) and
           ses.status = 'running'

     group by req.sql_handle
     having count(*) >= @prm_minexec
     order by 2 desc, 1;
  end


    else


  begin
    print ''
    select @datastr = convert(varchar, getdate(), 120);
    print '[' + @datastr + ']'

    if @prm_detail = 1
    begin
      select
        cast(max(master.dbo.fn_varbintohexstr(q1.sql_handle))  as varchar(100)) as SQL_HANDLE,
        max(q1.qtd) as QTD,
        isnull(sum(qst.EXECUTION_COUNT), 0) as 'EXECUTION_COUNT',
        isnull(sum(qst.TOTAL_PHYSICAL_READS), 0) as 'TOT_PHY_READS',
        isnull(cast (((1.0 * sum(qst.TOTAL_WORKER_TIME))  / sum(qst.EXECUTION_COUNT)) / 1000000 as decimal(15,3)), 0) as 'AVG_WORK_TIME (s)',
        isnull(cast (((1.0 * sum(qst.TOTAL_ELAPSED_TIME)) / sum(qst.EXECUTION_COUNT)) / 1000000 as decimal(15,3)), 0) as 'AVG_ELAP_TIME (s)',
        cast(isnull(datediff(ss, min(min_req_last_start_time), getdate()),0) as decimal(12)) as MIN_EXECUTING_S,
        cast(isnull(datediff(ss, max(max_req_last_start_time), getdate()),0) as decimal(12)) as MAX_EXECUTING_S,
        substring(case when max(q1.qtd) = 1 
                       then db_name(min(q1.dbid))
                       else db_name(min(q1.dbid)) + ', ' + db_name(max(q1.dbid2)) + 
                            case when max(q1.qtd) = 2 then '' else '...' end
                  end, 1, 22) as DB_NAME,
        substring((select u._sqlresumo(sql.TEXT, 40) from sys.dm_exec_sql_text(q1.sql_handle) sql), 1, 760) as SQL_TEXT
      from (
        select req.sql_handle as sql_handle,
               count(*) as QTD,
               min(req.start_time) as min_req_last_start_time,
               max(req.start_time) as max_req_last_start_time,
               min(ses.database_id) as dbid,
               max(ses.database_id) as dbid2
          from sys.dm_exec_sessions ses
                 join sys.dm_exec_requests req on
                      req.session_id = ses.session_id
         where ses.session_id <> @@SPID and
              (@prm_db is null or ses.database_id = db_id(@prm_db)) and
               ses.status = 'running'
         group by req.sql_handle
         having count(*) >= @prm_minexec
         ) q1
             left outer join sys.dm_exec_query_stats qst on
                  qst.sql_handle = q1.sql_handle
      group by q1.sql_handle
      order by QTD desc, SQL_HANDLE;
    end


      else


    begin
      select
        cast(max(master.dbo.fn_varbintohexstr(q1.sql_handle))  as varchar(100)) as SQL_HANDLE,
        max(q1.qtd) as QTD,
        substring( ' ' +  case when isnull(max(q1.qtd_blocked), 0) = 0       then ' ' else cast(isnull(max(q1.qtd_blocked), 0) as varchar) end + ' ~ ' + 
                          case when isnull(max(q1.qtd_wres_qc), 0) = 0       then ' ' else cast(isnull(max(q1.qtd_wres_qc), 0) as varchar) end + ' ~ ' + 
                          case when isnull(max(q1.qtd_waiting_grant), 0) = 0 then ' ' else cast(isnull(max(q1.qtd_waiting_grant), 0) as varchar) end
                   ,1 ,18) as 'WAIT BLK/QC/MEM',
        datediff (s, isnull(min(q1.max_wait_grant_t), getdate()), getdate()) as 'MAX_W_MEM(s)',
        isnull(max(q1.sum_granted_memory_kb), -1) as 'T_GRANTED_MEMORY_KB',
        isnull(sum(qst.EXECUTION_COUNT), 0) as 'EXECUTION_COUNT',
        isnull(sum(qst.TOTAL_PHYSICAL_READS), 0) as 'TOT_PHY_READS',
        isnull(cast (((1.0 * sum(qst.TOTAL_WORKER_TIME))  / sum(qst.EXECUTION_COUNT)) / 1000000 as decimal(15,3)), 0) as 'AVG_WORK_TIME (s)',
        isnull(cast (((1.0 * sum(qst.TOTAL_ELAPSED_TIME)) / sum(qst.EXECUTION_COUNT)) / 1000000 as decimal(15,3)), 0) as 'AVG_ELAP_TIME (s)',
        cast(isnull(datediff(ss, min(min_req_last_start_time), getdate()),0) as decimal(12)) as MIN_EXECUTING_S,
        cast(isnull(datediff(ss, max(max_req_last_start_time), getdate()),0) as decimal(12)) as MAX_EXECUTING_S,
        sum(q1.tot_fetch) as 'TOT_FETCH',
        substring(case when max(q1.qtd) = 1 
                       then db_name(min(q1.dbid))
                       else db_name(min(q1.dbid)) + ', ' + db_name(max(q1.dbid2)) + 
                            case when max(q1.qtd) = 2 then '' else '...' end
                  end, 1, 22) as DB_NAME,
        isnull(min(q1.min_sid), -1) as 'MIN_SID',
        isnull(max(q1.max_sid), -1) as 'MAX_SID',
        substring((select u._sqlresumo(sql.TEXT, 40) from sys.dm_exec_sql_text(q1.sql_handle) sql), 1, 760) as SQL_TEXT
      from (
        select isnull(req.sql_handle, req.sql_handle) as sql_handle,
               count(*) as QTD,
               sum(qmg.granted_memory_kb) as sum_granted_memory_kb,
               sum(case when qmg.grant_time is null and qmg.request_time is not null then 1 else 0 end) as qtd_waiting_grant,
               min(case when qmg.grant_time is null and qmg.request_time is not null then qmg.request_time else null end) as max_wait_grant_t,
               sum(case when req.blocking_session_id <> 0 then 1 else 0 end) as qtd_blocked,
               sum(case when req.wait_type = 0x011B then 1 else 0 end) as qtd_wres_qc,
               min(req.start_time) as min_req_last_start_time,
               max(req.start_time) as max_req_last_start_time,
               sum(req.row_count) as tot_fetch,
               min(ses.database_id) as dbid,
               max(ses.database_id) as dbid2,
               min(ses.session_id) as min_sid,
               max(ses.session_id) as max_sid
          from sys.dm_exec_sessions ses
                 join sys.dm_exec_requests req on
                      req.session_id = ses.session_id
                 left outer join sys.dm_exec_query_memory_grants qmg on
                      qmg.session_id = ses.session_id
         where ses.session_id <> @@SPID and
              (@prm_db is null or ses.database_id = db_id(@prm_db)) and
               ses.status = 'running'
         group by isnull(req.sql_handle, req.sql_handle)
         having count(*) >= @prm_minexec
         ) q1
             left outer join sys.dm_exec_query_stats qst on
                  qst.sql_handle = q1.sql_handle
      group by q1.sql_handle
      order by QTD desc, SQL_HANDLE;
    end



    select 
      @maxlastOps = max(cast(isnull(datediff(ss, case when ses.last_request_start_time > ses.last_request_end_time
                                                      then ses.last_request_start_time
                                                      else ses.last_request_end_time
                                                 end, getdate()),0) as decimal(12))),

      @qtOpsDem   = sum(case when isnull(datediff(ss, case when ses.last_request_start_time > ses.last_request_end_time
                             then ses.last_request_start_time
                             else ses.last_request_end_time
                             end, getdate()),0) > 20 then 1 else 0 end),

      @qtOpsDemNaoSist  = sum(case when isnull(datediff(ss, case when ses.last_request_start_time > ses.last_request_end_time
                             then ses.last_request_start_time
                             else ses.last_request_end_time
                             end, getdate()),0) > 20 and
                             not (
                                   ses.login_name in ('SAJ', 'SAJAPP', 'SAJBANCO') and
                                   ses.host_name like 'DTC%' and
                                   (
                                     ses.program_name like '___Servidor%.exe' or 
                                     ses.program_name like 'Microsoft JDBC Driver%' or
                                     ses.program_name like 'Microsoft SQL Server JDBC%' or
                                     ses.program_name like 'JDBC%' or
                                     ses.program_name in ('spBalanceador.exe', 'spMedidor.exe', 'sajpss5app.exe')
                                   )
                                 ) then 1 else 0 end),

      @qtOpsNaoSist    = sum(case when 
                               not (
                                   ses.login_name in ('SAJ', 'SAJAPP', 'SAJBANCO') and
                                   ses.host_name like 'DTC%' and
                                   (
                                     ses.program_name like '___Servidor%.exe' or 
                                     ses.program_name like 'Microsoft JDBC Driver%' or
                                     ses.program_name like 'Microsoft SQL Server JDBC%' or
                                     ses.program_name like 'JDBC%' or
                                     ses.program_name in ('spBalanceador.exe', 'spMedidor.exe', 'sajpss5app.exe')
                                   )
                                 ) then 1 else 0 end)

    from
      sys.dm_exec_sessions ses
	left outer join sys.dm_exec_requests req on 
	     ses.session_id = req.session_id
    where
      ses.session_id <> @@SPID and
      ses.status = 'running' and
      req.command not in ('AWAITING COMMAND', 'WAITFOR');

  end



  print ""
  print "Numero de sessoes executando SQLs na instancia       : " + cast(u.getNumSQLExec(null) as varchar(20)) + ' / ' + cast(@qtOpsNaoSist as varchar) +
                                                                    '  (' + cast(@qtOpsDem as varchar) + ' em running a mais de 20s' + 
                                                                    (case when @qtOpsDemNaoSist > 0 then ', sendo que ' + cast(@qtOpsDemNaoSist as varchar) + ' nao parecem ser dos Servidores de Aplicacao do SAJ' else '' end) + ')';
  print "Numero de sessoes bloqueadas na instancia (ex: lock) : " + cast(u.getNumSQLBlocked(null) as varchar(20));
  print "Operacao em running mais antiga                      : " + cast(@maxlastOps as varchar(20)) + 's';
  print ""
  set NOCOUNT OFF
  return 0
end

GO


if object_id ( 'u.klhdl', 'P' ) is not null
  drop procedure u.klhdl
GO


create procedure u.klhdl (
  @prm_sql_handle      varbinary(64)
) as
begin
  set NOCOUNT ON


select 'kill ' + cast(exr.session_id as char(10))
       from sys.dm_exec_requests exr
       where exr.sql_handle = @prm_sql_handle

  print ''
  set NOCOUNT OFF
  return 0
end
GO



if object_id ( 'u._sqltrunc', 'FN' ) is not null
  drop function u._sqltrunc
GO

create function u._sqltrunc
(
  @prm_sql            nvarchar(max),
  @prm_maxsql         int = 8000

) returns nvarchar(max) as

begin
  declare @lensql            integer
  declare @sql1              nvarchar(max)


  set @lensql = len(@prm_sql)

  if @lensql <= @prm_maxsql or @prm_maxsql <= 500
    begin
    set @sql1 = @prm_sql
    end
   else
    begin
    set @sql1 = substring(@prm_sql, 1, @prm_maxsql - 313) + 
                char(13) + char(10) + 
                char(13) + char(10) + 
                '[...]' +
                char(13) + char(10) + 
                char(13) + char(10) + 
                substring(@prm_sql, @lensql - 300, 301)		-- bug do sql server (+1)
    end

  return @sql1
end

GO





if object_id ( 'u.sql', 'P' ) is not null
  drop procedure u.sql
GO


create procedure u.sql (
  @prm_sql_handle      varbinary(64),
  @prm_trunc           int = 1
) as
begin
  set NOCOUNT ON


  select
    (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
    qst.CREATION_TIME,
    qst.STATEMENT_START_OFFSET,
    qst.STATEMENT_END_OFFSET,
    qst.LAST_EXECUTION_TIME,
    qst.EXECUTION_COUNT,
    cast ((1.0 * qst.TOTAL_WORKER_TIME) / 1000000 as decimal(15,3)) as 'WORKER_TIME (s)',
    cast (((1.0 * qst.TOTAL_WORKER_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_WORK_TIME (s)',
    cast ((1.0 * qst.MAX_WORKER_TIME) / 1000000 as decimal(15,3)) as 'MAX_WORK_TIME (s)',
    cast ((1.0 * qst.TOTAL_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'ELAPSED_TIME (s)',
    cast (((1.0 * qst.TOTAL_ELAPSED_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_ELAP_TIME (s)',
    cast ((1.0 * qst.MAX_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'MAX_ELAP_TIME (s)',
    qst.TOTAL_LOGICAL_READS as 'LOGICAL_READS',
    cast (qst.TOTAL_LOGICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_LOGICAL_READS',
    qst.TOTAL_PHYSICAL_READS as 'PHY_READS',
    cast (qst.TOTAL_PHYSICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_PHY_READS',
    qst.PLAN_GENERATION_NUM,
	(select cast (max(master.dbo.fn_varbintohexstr(qst1.PLAN_HANDLE)) as varchar(100))
	   from sys.dm_exec_query_stats qst1
	  where qst1.sql_handle = qst.sql_handle and
	        qst1.plan_handle = qst.plan_handle) as PLAN_HANDLE
    --cast (max(master.dbo.fn_varbintohexstr(qst.PLAN_HANDLE)) as varchar(100)) as PLAN_HANDLE
  from
    sys.dm_exec_query_stats qst
  where
    qst.sql_handle = @prm_sql_handle
  order by
    DBNAME, qst.CREATION_TIME, qst.STATEMENT_START_OFFSET;

  print ''

  select
    (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
    qst.CREATION_TIME,
    qst.STATEMENT_START_OFFSET,
    cast ((1.0 * qst.TOTAL_WORKER_TIME) / 1000000 as decimal(15,3)) as 'TOTAL_WORKER_TIME (s)',
    cast (((1.0 * qst.TOTAL_WORKER_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_WORKER_TIME (s)',
    cast ((1.0 * qst.LAST_WORKER_TIME) / 1000000 as decimal(15,3)) as 'LAST_WORKER_TIME (s)',
    cast ((1.0 * qst.MIN_WORKER_TIME) / 1000000 as decimal(15,3)) as 'MIN_WORKER_TIME (s)',
    cast ((1.0 * qst.MAX_WORKER_TIME) / 1000000 as decimal(15,3)) as 'MAX_WORKER_TIME (s)'
  from
    sys.dm_exec_query_stats qst
  where
    qst.sql_handle = @prm_sql_handle
  order by
    DBNAME, qst.CREATION_TIME, qst.STATEMENT_START_OFFSET;

  print ''

  select
    (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
    qst.CREATION_TIME,
    qst.STATEMENT_START_OFFSET,
    cast ((1.0 * qst.TOTAL_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'TOT_ELAPSED_TIME (s)',
    cast (((1.0 * qst.TOTAL_ELAPSED_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_ELAPSED_TIME (s)',
    cast ((1.0 * qst.LAST_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'LAST_ELAPSED_TIME (s)',
    cast ((1.0 * qst.MIN_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'MIN_ELAPSED_TIME (s)',
    cast ((1.0 * qst.MAX_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'MAX_ELAPSED_TIME (s)',
    '   ',
    cast ((1.0 * qst.TOTAL_CLR_TIME) / 1000000 as decimal(15,3)) as 'TOT_CLR_TIME (s)',
    cast (((1.0 * qst.TOTAL_CLR_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_CLR_TIME',
    cast ((1.0 * qst.LAST_CLR_TIME) / 1000000 as decimal(15,3)) as 'LAST_CLR_TIME (s)',
    cast ((1.0 * qst.MIN_CLR_TIME) / 1000000 as decimal(15,3)) as 'MIN_CLR_TIME (s)',
    cast ((1.0 * qst.MAX_CLR_TIME) / 1000000 as decimal(15,3)) as 'MAX_CLR_TIME (s)'
  from
    sys.dm_exec_query_stats qst
  where
    qst.sql_handle = @prm_sql_handle
  order by
    DBNAME, qst.CREATION_TIME, qst.STATEMENT_START_OFFSET;

  print ''

  select
    (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
    qst.CREATION_TIME,
    qst.STATEMENT_START_OFFSET,
    qst.TOTAL_PHYSICAL_READS,      
    cast (qst.TOTAL_PHYSICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_PHYSICAL_READS',
    qst.LAST_PHYSICAL_READS, qst.MIN_PHYSICAL_READS, qst.MAX_PHYSICAL_READS
  from
    sys.dm_exec_query_stats qst
  where
    qst.sql_handle = @prm_sql_handle
  order by
    DBNAME, qst.CREATION_TIME, qst.STATEMENT_START_OFFSET;

  print ''

  select
    (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
    qst.CREATION_TIME,
    qst.STATEMENT_START_OFFSET,
    qst.TOTAL_LOGICAL_READS,       
    cast (qst.TOTAL_LOGICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_LOGICAL_READS',
    qst.LAST_LOGICAL_READS, qst.MIN_LOGICAL_READS, qst.MAX_LOGICAL_READS,
    qst.TOTAL_LOGICAL_WRITES,      
    cast (qst.TOTAL_LOGICAL_WRITES / qst.EXECUTION_COUNT as bigint) as 'AVG_LOGICAL_READS',
    qst.LAST_LOGICAL_WRITES, qst.MIN_LOGICAL_WRITES, qst.MAX_LOGICAL_WRITES
  from
    sys.dm_exec_query_stats qst
  where
    qst.sql_handle = @prm_sql_handle
  order by
    DBNAME, qst.CREATION_TIME, qst.STATEMENT_START_OFFSET;

  print ''


  select 
    substring(db_name(sql.DBID),1,30) as DB_NAME, 
    sql.DBID, 
    sql.OBJECTID, 
    sql.NUMBER, 
    sql.ENCRYPTED 
  from 
    sys.dm_exec_sql_text(@prm_sql_handle) sql;


  print ''
  print ''

  if @prm_trunc = 0
     select sql.TEXT from sys.dm_exec_sql_text(@prm_sql_handle) sql;
  else
     select u._sqltrunc(sql.TEXT, 8000) as TEXT from sys.dm_exec_sql_text(@prm_sql_handle) sql;


  print ''
  set NOCOUNT OFF
  return 0
end
GO


if object_id ( 'u.topsql', 'P' ) is not null
  drop procedure u.topsql
GO

create procedure u.topsql
(
  @prm_orderby                 varchar(8) = '/?',
  @prm_top                     integer = 100,
  @prm_mincount                integer = 0,
  @prm_hoje                    integer = 0,
  @prm_LastMin                 integer = 0,
  @prm_db                      nvarchar(30) = ''
) as
begin
  set NOCOUNT ON

  declare @cmd1              varchar(8000)
  declare @orderby           varchar(128)

  if @prm_orderby in ('', '?', '/?', '-?', '-h')
    begin
    print ""
    print "Formato: u.topsql <prm_orderby> [, parametro] [, parametro] [, @prm=xyz] [...]"
    print ""
    print "  @prm_orderby = totexec  - ordenar pelo total de execucoes"
    print "                 totwork  - ordenar pelo total de Work Time"
    print "                 maxwork  - ordenar pelo maior tempo individual de Work Time"
    print "                 avgwork  - ordenar pela media de Work Time"
    print "                 totelap  - ordenar pelo total de Elapsed Time"
    print "                 maxelap  - ordenar pelo maior tempo individual de Elapsed Time"
    print "                 avgelap  - ordenar pela media de Elapsed Time"
    print "                 totlr    - ordenar pelo total de Logical Reads"
    print "                 avglr    - ordenar pela media de Logical Reads"
    print "                 totpr    - ordenar pelo total de Physical Reads"
    print "                 avgpr    - ordenar pela media de Physical Reads"
    print ""
    print "  @prm_top     : {1-n} numero de linhas a mostrar (default = 100)"
    print "  @prm_hoje    : {0|1} filtrar somente os planos criados hoje (default = 0)"
    print "  @prm_mincount: {0-n} numero minimo de execucoes para considerar o sql na listagem (default = 0)"
    print "  @prm_LastMin : {minutos}  filtrar somente os planos executados nos ultimos n minutos (default = nao filtrar)"
    print "  @prm_db      : {database} filtrar somente os planos do database indicado (default = nao filtrar)"
    print ""
    return 1
  end

  set @orderby =
    case when @prm_orderby = 'totexec'  then "qst.EXECUTION_COUNT desc"
         when @prm_orderby = 'totwork'  then "qst.TOTAL_WORKER_TIME desc"
         when @prm_orderby = 'maxwork'  then "qst.TOTAL_WORKER_TIME desc"
         when @prm_orderby = 'avgwork'  then "'AVG_WORK_TIME (s)' desc"

         when @prm_orderby = 'totelap'  then "qst.TOTAL_ELAPSED_TIME desc"
         when @prm_orderby = 'maxelap'  then "qst.MAX_ELAPSED_TIME desc"
         when @prm_orderby = 'avgelap'  then "'AVG_ELAP_TIME (s)' desc"

         when @prm_orderby = 'totlr'    then "qst.TOTAL_LOGICAL_READS desc"
         when @prm_orderby = 'avglr'    then "'AVG_LOGICAL_READS' desc"

         when @prm_orderby = 'totpr'    then "qst.TOTAL_PHYSICAL_READS desc"
         when @prm_orderby = 'avgpr'    then "'AVG_PHY_READS' desc"
         else "erro"
    end

  if @orderby = 'erro' 
    begin
    print ""
    print "***** ERRO: O parametro @orderby está incorreto."
    print ""
    print "Valores permitidos: totexec, totwork, maxwork, avgwork, totelap, maxelap, avgelap, totlr, avglr, totpr, avgpr"
    print ""
    return 1
  end

  set @cmd1=
    'select top ' + cast(@prm_top as varchar) + 
"
      (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
       qst.EXECUTION_COUNT,
       cast ((1.0 * qst.TOTAL_WORKER_TIME) / 1000000 as decimal(15,4)) as 'WORKER_TIME (s)',
       cast (((1.0 * qst.TOTAL_WORKER_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,4)) as 'AVG_WORK_TIME (s)',
       cast ((1.0 * qst.MAX_WORKER_TIME) / 1000000 as decimal(15,4)) as 'MAX_WORK_TIME (s)',
       cast ((1.0 * qst.TOTAL_ELAPSED_TIME) / 1000000 as decimal(15,4)) as 'ELAPSED_TIME (s)',
       cast (((1.0 * qst.TOTAL_ELAPSED_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,4)) as 'AVG_ELAP_TIME (s)',
       cast ((1.0 * qst.MAX_ELAPSED_TIME) / 1000000 as decimal(15,4)) as 'MAX_ELAP_TIME (s)',
       qst.TOTAL_LOGICAL_READS as 'LOGICAL_READS',
       cast (qst.TOTAL_LOGICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_LOGICAL_READS',
       qst.TOTAL_PHYSICAL_READS as 'PHY_READS',
       cast (qst.TOTAL_PHYSICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_PHY_READS',
       qst.PLAN_GENERATION_NUM,
       qst.CREATION_TIME,
       qst.LAST_EXECUTION_TIME,
       cast (master.dbo.fn_varbintohexstr(qst.PLAN_HANDLE) as varchar(100)) as PLAN_HANDLE,
       cast (master.dbo.fn_varbintohexstr(qst.SQL_HANDLE) as varchar(100)) as SQL_HANDLE,
       qst.statement_start_offset,
       cast ((select u._sqlresumo(sql.TEXT, 80) from sys.dm_exec_sql_text(qst.SQL_HANDLE) sql) as varchar(500)) as SQL2
     from
       sys.dm_exec_query_stats qst
     where
       (select sql.TEXT from sys.dm_exec_sql_text(qst.SQL_HANDLE) sql) not like 'FETCH API_CURSOR%'
" + 

    case when @prm_hoje = 1 then
         "       and qst.CREATION_TIME >= convert(DateTime, substring(convert(varchar, getdate(), 120), 1, 10), 120)" + char(10)
         else ""
    end +

    case when @prm_LastMin <> 0 then
         "       and qst.LAST_EXECUTION_TIME >= dateadd(mi, -"+ cast(@prm_LastMin as varchar) +", getdate())" + char(10)
         else ""
    end +

    case when @prm_mincount > 0 then
         "       and qst.EXECUTION_COUNT >= " + cast(@prm_minCount as varchar) + char(10)
         else ""
    end +

    case when @prm_db <> '' then
         "       and (select db_name(cast(pattr.value as integer)) from sys.dm_exec_plan_attributes(qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') = '" + @prm_db + "'" + char(10)
         else ""
    end +

    "     order by " + @orderby


  set NOCOUNT OFF
  exec (@cmd1)

  set NOCOUNT ON
  print ''
  set NOCOUNT OFF
  return 0
end

GO


if object_id ( 'u._analisarobj', 'P' ) is not null
  drop procedure u._analisarobj
GO


create procedure u._analisarobj
(
  @prm_obj_name             nvarchar(256),
  @prm_obj_type             char(2) = null output,
  @prm_obj_type_txt         nvarchar(20) = null output,
  @prm_obj_base_id          int = null output,
  @prm_obj_base_name        nvarchar(256) = null output,
  @prm_obj_base_type        char(2) = null output
) as
begin
  declare @obj_id        int
  declare @schema_id     int

  set @obj_id = object_id (@prm_obj_name)
  if @obj_id is NULL return 1

  select @prm_obj_type = obj.type, @schema_id = obj.schema_id  from sys.all_objects obj where obj.object_id = @obj_id;
  set @prm_obj_type_txt = 
    case when @prm_obj_type = 'U'  then 'TABLE'
         when @prm_obj_type = 'SN' then 'SYNONYM'
         when @prm_obj_type = 'V'  then 'VIEW'
         when @prm_obj_type = 'IT' then 'INTERNAL TABLE'
         when @prm_obj_type = 'TF' then 'SQL TABLE FN'
         when @prm_obj_type = 'S'  then 'SYSTEM TABLE'
         when @prm_obj_type = 'FT' then 'CLR TABLE'
         when @prm_obj_type = 'IF' then 'INLINE TABLE'
         else @prm_obj_type
    end

  set @prm_obj_base_id = (select object_id (syn.base_object_name) from sys.synonyms syn where syn.object_id = @obj_id)
  if @prm_obj_base_id is not NULL 
  begin
    set @prm_obj_base_name = schema_name(objectproperty(@prm_obj_base_id, 'SCHEMAID')) + '.' + object_name (@prm_obj_base_id)
    set @prm_obj_base_type = (select obj.type from sys.all_objects obj where obj.object_id = @prm_obj_base_id)
    return 0
  end

  set @prm_obj_base_id   = @obj_id
  set @prm_obj_base_name = @prm_obj_name
  set @prm_obj_base_type = @prm_obj_type
  return 0
end

GO


if object_id ( 'u.tbcol', 'P' ) is not null
  drop procedure u.tbcol
GO

create procedure u.tbcol
(
  @prm_obj_name                nvarchar(256) = null
) as

begin
  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)
  declare @obj_compress_type nvarchar(20)
  declare @obj_replication   nvarchar(20)
  declare @obj_is_replicated bit
  declare @obj_partitioned   nvarchar(20)

  set NOCOUNT ON

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output, 
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end
  
  set NOCOUNT OFF  
  select
    col.column_id as COLUMN_ID,
    substring (col.name, 1, 32) as COLUMN_NAME,
   (select upper(substring (tp.name, 1, 18)) from sys.types tp where tp.user_type_id = col.user_type_id) as DATA_TYPE,
    case when col.precision = 0 and col.user_type_id in (34, 35, 99) then -1
         when col.precision = 0 then col.max_length
         else col.precision
    end as LENGTH,
    col.scale as SCALE,
    case when col.is_nullable = 1 then 'Y' else 'N' end as NULLS,
    isnull (idc.key_ordinal, 0) as KEYSEQ,
    col.is_computed as COMP,
    col.is_identity as ID,
    col.is_filestream as FS,
    isnull ((select cast (dft.definition as char(10)) from sys.default_constraints dft where dft.object_id = col.default_object_id), '-') as DEFAULT_,
   (select count(*) from sys.check_constraints ck where ck.parent_object_id = col.object_id and ck.parent_column_id = col.column_id) as CHECKS,
    cast(isnull(icol.last_value, '') as varchar(30)) as IDENTITY_LAST_VALUE
-- ,col.collation_name
  from
    sys.all_columns col
      left outer join sys.identity_columns icol  on
           icol.object_id = col.object_id and
           icol.column_id = col.column_id
      left outer join sys.indexes idx  on
           idx.object_id      = col.object_id and
           idx.is_primary_key = 1
        left outer join sys.index_columns idc  on
             idc.object_id = idx.object_id and
             idc.index_id  = idx.index_id and
             idc.column_id = col.column_id
  where
    col.object_id = object_id (@obj_base_name)
  order by 1;
  set NOCOUNT ON
  
 
  print ''
  print '-->> Tipo Objeto.: ' + @obj_type_txt + (case when @obj_type = 'SN' then ' (' + @obj_base_name + ')' else '' end)
  
-- Mostra se a tabela eh comprimida ou nao.

  SELECT DISTINCT @obj_compress_type = sp.data_compression_desc
  FROM sys.partitions SP
  INNER JOIN sys.tables ST ON
       st.object_id = sp.object_id
  WHERE sp.data_compression <> 0 and
       st.object_id = object_id(@obj_base_name)

  if @obj_compress_type is not null	
  begin  
  print '-->> Compressao..: Sim (' + @obj_compress_type + ')'
  end

  if @obj_compress_type is null	
  begin  
  print '-->> Compressao..: Nao'
  end

-- Mostra se a tabela eh replicada ou nao.

  SELECT @obj_replication = SCHEMA_NAME(ST.SCHEMA_ID)
  FROM sys.tables ST
  WHERE st.object_id = object_id(@obj_base_name)
  
  SELECT @obj_is_replicated = is_replicated
  FROM sys.tables ST
  WHERE st.object_id = object_id(@obj_base_name)  

  if @obj_replication = 'REPL'	
  begin  
  print '-->> Replicada...: Sim'
  end
  
  if @obj_replication = 'SAJ' and @obj_is_replicated = 1	
  begin  
  print '-->> Replicada...: Sim'
  end  

  if @obj_replication = 'SAJ' and @obj_is_replicated = 0	
  begin  
  print '-->> Replicada...: Nao'
  end

-- Mostra se a tabela eh particionada ou nao.

select distinct @obj_partitioned = OBJECT_NAME(p.[object_id])
from sys.partitions p
inner join sys.indexes i 
   on p.[object_id] = i.[object_id] 
   and p.index_id = i.index_id
inner join sys.data_spaces ds 
   on i.data_space_id = ds.data_space_id
inner join sys.partition_schemes ps 
 on ds.data_space_id = ps.data_space_id
inner JOIN sys.partition_functions pf 
   on ps.function_id = pf.function_id
WHERE p.[object_id] = object_id(@obj_base_name)

  if @obj_partitioned is not null	
  begin  
  print '-->> Particionada: Sim'
  print ''
  end

  if @obj_partitioned is null	
  begin  
  print '-->> Particionada: Nao'
  print ''
  end

  
  return 0
end

GO


if object_id ( 'u.le3', 'P' ) is not null
  drop procedure u.le3
GO


create procedure u.le3 as
begin
  set NOCOUNT OFF

  select 
    ses.session_id as SES_ID,
    substring(db_name(prc.dbid),1,13) as DB_NAME,
    substring (substring (ses.login_name, charindex ('\', ses.login_name) + 1, 15), 1, 18) as LOGIN_NAME,
    substring (ses.status, 1, 15) as STATUS,
    prc.cmd as CMD,
    '',
    case when ses.last_request_start_time > ses.last_request_end_time
         then ses.last_request_start_time
         else ses.last_request_end_time
    end as LAST_OPERATION,

    cast(isnull(datediff(ms, case when ses.last_request_start_time > ses.last_request_end_time
                                  then ses.last_request_start_time
                                  else ses.last_request_end_time
                             end, getdate()),0) / 1000.0 as decimal(10,3)) as LASTOP_SECs,

--  prc.blocked as LOCKWAIT_SID,
    case when owt.blocking_session_id is not null 
               then right(replicate(' ',11) + cast(owt.blocking_session_id as varchar) + ' / ' + 
                          isnull(cast(owt.blocking_exec_context_id as varchar), '-'), 14)
               else ''
    end as LOCKWAIT_SID,

--  cast (prc.waittime / 1000.0 as decimal(10,3)) as "WAIT_TIME (s)",
    case when owt.wait_duration_ms is not null
         then right(replicate(' ',11) + cast(cast(owt.wait_duration_ms / 1000.0 as decimal(10,3)) as varchar), 11)
         else ''
    end as "WAIT_TIME(s)",

    '',
    case when trs.transaction_id is not null
         then substring(isnull(convert(varchar, trd.database_transaction_begin_time, 20), 'NO LOG') + ' (' + cast(isnull(trs.transaction_id, -1) as varchar) +')', 1, 31)
         else ''
    end as 'TRANSACTION (START/ID)',

    case when trd.database_transaction_begin_time is not null
         then right(replicate(' ',11) + cast(cast(isnull(datediff(ms, trd.database_transaction_begin_time, getdate()),0) / 1000.0 as decimal(10,3)) as varchar), 11)
         else ''
    end as TRANS_SECs,

    '',
    substring(case when owt.wait_type is not null
                   then owt.wait_type + ' (e' + cast(owt.exec_context_id as varchar) + ')'
                   else 'LAST = ' + prc.lastwaittype
              end, 1, 32) as "WAIT_TYPE / ECID",

    substring (ses.program_name, 1, 25) as PROGRAM_NAME,
    substring (ses.host_name, 1, 20) as HOST_NAME,
    substring (con.client_net_address + (case when con.client_tcp_port is null then '' else ':' + cast(con.client_tcp_port as char) end), 1, 22) as IP_ADDRESS,
-- (select sum(con2.num_reads)+sum(con2.num_writes) from sys.dm_exec_connections con2 where con2.session_id = ses.session_id) as 'PACKET R+W',

    '',
   (select cast(count(*) as smallint) from sys.dm_tran_database_transactions tr9 where tr9.transaction_id = trs.transaction_id) as TR_QTD,
   (case when trd.database_transaction_type = 1     then 'R/W'
         when trd.database_transaction_type = 2     then 'R/O'
         when trd.database_transaction_type = 3     then 'SYS'
         when trd.database_transaction_type is null then ''
         else '???'
    end) as TR_TYPE,
    cast (case when trd.database_transaction_state is null then ''
               when trd.database_transaction_state =  1    then 'Not init '
               when trd.database_transaction_state =  3    then 'No log '
               when trd.database_transaction_state =  4    then 'With log '
               when trd.database_transaction_state =  5    then 'Prepared '
               when trd.database_transaction_state = 10    then 'Commited '
               when trd.database_transaction_state = 11    then 'Rolled back '
               when trd.database_transaction_state = 12    then 'Commiting '
               else ''
          end + isnull ('[' + cast(trd.database_transaction_state as varchar) + ']', '')
          as varchar(17)) as TRANSACTION_STATE,
    isnull(substring(db_name(trd.database_id),1,13), '') as TR_DBNAME,

    ses.host_process_id as HPID,
   (select cast(count(*) as smallint) from sys.sysprocesses prc2 where prc2.spid = prc.spid) as QT_ECID,
   (select cast(count(*) as smallint) from sys.dm_exec_connections con2 where con2.session_id = ses.session_id) as QT_CON,

--  ses.cpu_time as 'CPU (ms)',
--  ses.logical_reads,
--  ses.row_count,
--  substring (isnull (ses.nt_domain, '') + '\' + isnull (ses.nt_user_name, ''), 1, 30) as OSUSER

    ses.session_id as SES_ID2,
    (select cast (max(master.dbo.fn_varbintohexstr(req1.sql_handle)) as varchar(100))
	   from sys.dm_exec_requests req1 
	 where req1.session_id = req.session_id) as LAST_REQ_SQL_HANDLE,	
--  cast (master.dbo.fn_varbintohexstr(req.sql_handle) as varchar(100)) as LAST_REQ_SQL_HANDLE,
    (select cast (max(master.dbo.fn_varbintohexstr(prc1.sql_handle)) as varchar(100))
	   from sys.sysprocesses prc1
	  where prc1.spid = prc.spid) as LAST_PRC_SQL_HANDLE,
--  cast (master.dbo.fn_varbintohexstr(prc.sql_handle) as varchar(100)) as LAST_PRC_SQL_HANDLE,

    '',
    substring (prc.waitresource, 1, 25) as WAIT_RESOURCE

  from
    sys.sysprocesses prc
      join sys.dm_exec_sessions ses on
           ses.session_id = prc.spid
        left outer join sys.dm_exec_requests req on
             req.session_id = ses.session_id
        left outer join sys.dm_exec_connections con on
             con.session_id = ses.session_id and
             con.parent_connection_id is null
        left outer join sys.dm_tran_session_transactions trs on
             trs.session_id = ses.session_id
          left outer join sys.dm_tran_database_transactions trd on
               trd.transaction_id = trs.transaction_id
--             trd.database_id    = db_id()

        left outer join sys.dm_os_waiting_tasks owt on
             owt.session_id = ses.session_id


  where
    prc.dbid = db_id() and
    prc.spid <> @@SPID and
    prc.ecid = 0 and
    (
      (
      ses.status <> 'sleeping' and
      ses.is_user_process <> 0
      ) or (
      trd.transaction_id is not null and
      trd.database_transaction_state <> 3
      )
    )
  order by
    LAST_OPERATION, LASTOP_SECs desc, ses.session_id
  option (FAST 10);



  print ''
  set NOCOUNT OFF
  return 0
end

GO


if object_id ( 'u.planget', 'P' ) is not null
  drop procedure u.planget
GO


create procedure u.planget (
  @prm_plan_handle      varbinary(64),
  @prm_start_offset     int = 0,
  @prm_end_offset       int = -1
) as
begin
  set NOCOUNT ON

  -- select query_plan from sys.dm_exec_query_plan (@prm_plan_handle)

  select query_plan from sys.dm_exec_text_query_plan (@prm_plan_handle, @prm_start_offset, @prm_end_offset);

  print ''
  print 'Use o MS SQL Management Studio ou spSQL para executar esta procedure e salvar o resultado'
  print 'com a extensao .sqlplan'
  print ''
  set NOCOUNT OFF
  return 0
end
GO


if object_id ( 'u._leColIndex', 'FN' ) is not null
  drop function u._leColIndex
GO

create function u._leColIndex
(
  @prm_object_id      int,
  @prm_index_id       int
) returns nvarchar(4000) as

begin

  declare @idxcols           nvarchar(4000)
  declare @idxcol            nvarchar(133)

  set @idxcols = ''

  declare idxcol_cursor cursor  local forward_only read_only
    for
      select
        col.name +
        case when idc.is_included_column = 0 then (
             case when idc.is_descending_key  = 1 then '(D)'  else '(A)' end
             ) else '(I)' end as idxcol
      from
        sys.index_columns idc
          join sys.all_columns col  on
             col.object_id  = idc.object_id and
             col.column_id  = idc.column_id
      where
        idc.object_id = @prm_object_id and
        idc.index_id  = @prm_index_id
      order by
        (case when idc.key_ordinal = 0 then 10000 + idc.index_column_id else idc.key_ordinal end)

  open idxcol_cursor
  fetch next from idxcol_cursor into @idxcol
  while @@FETCH_STATUS = 0
  begin
    set @idxcols = @idxcols + (case when @idxcols != '' then ' + ' else '' end) + @idxcol
    fetch next from idxcol_cursor into @idxcol
  end
  close idxcol_cursor
  deallocate idxcol_cursor

  return @idxcols
end

GO




if object_id ( 'u.tbidx', 'P' ) is not null
  drop procedure u.tbidx
GO

create procedure u.tbidx
(
  @prm_obj_name                nvarchar(128) = null
) as
begin
  set NOCOUNT ON

  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output, 
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_base_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end


  set NOCOUNT OFF
  select distinct
    substring (idx.name, 1, 40) as INDEX_NAME,
    substring (idx.type_desc, 1, 15) as INDEX_TYPE,
    idx.is_primary_key as PK,
    idx.is_unique as UNIQ,        
    idx.is_unique_constraint as UN_C,
    idx.is_disabled as DISAB,
    idx.is_hypothetical as HYPT,
    idx.ignore_dup_key as IGDUP,
    idx.allow_row_locks as RLK,
    idx.allow_page_locks as PLK,
	isnull(prt.data_compression,0) as COMP,
    idx.fill_factor as FILL,
    (idxs.user_seeks + idxs.user_scans) as Qtd_Acessos_Usr,
    idxs.user_updates as Qtd_Updates_Usr,
    substring (u._lecolindex(idx.object_id, idx.index_id), 1, 200) as INDEX_COLUMNS,
    substring (cast(idx.filter_definition as nvarchar(60)), 1, 80) as FILTER_DEFINITION,
    (select substring(ds.name,1,60) from sys.data_spaces ds where ds.data_space_id = idx.data_space_id) as DATA_SPACE,
    idx.INDEX_ID
  from
    sys.indexes idx
      left outer join sys.dm_db_index_usage_stats idxs on
           idxs.object_id = idx.object_id and
           idxs.index_id  = idx.index_id and
           idxs.database_id = db_id(db_name())
      left outer join sys.partitions prt on
	       idx.object_id = prt.object_id and
		   idx.index_id = prt.index_id
  where
    idx.object_id = object_id(@obj_base_name) and
    idx.index_id != 0
  order by INDEX_NAME;
  set NOCOUNT ON


  print ''
  set NOCOUNT OFF
  return 0
end

GO

if object_id ( 'u.pctdone', 'P' ) is not null
  drop procedure u.pctdone
GO

create procedure u.pctdone
(
  @prm_filtro nvarchar(126) = ''
) as
begin
  set NOCOUNT ON

  declare @cmd1 varchar(8000)

  set @cmd1=
"
  select
    cast(db_name(r.database_id) as varchar(15)) as Databasename,
    session_id as SPID,
    start_time,
    cast(percent_complete as decimal(5,1)) as percent_complete,
    dateadd(second,estimated_completion_time/1000, getdate()) as estimated_completion_time,
    cast(command as varchar(20)) as command,
    cast(replace(replace(a.text, char(13), ''), char(10), '') as varchar(1000)) as query
  from
    sys.dm_exec_requests r
      cross apply sys.dm_exec_sql_text(r.sql_handle) a
  where
    --r.command in ('BACKUP DATABASE','RESTORE DATABASE','BACKUP LOG')
	--r.command in ('SELECT','INSERT','UPDATE','DELETE','DBCC','FOR','LOCK MONITOR','CHECKPOINTLAZY','WRITER')
	session_id<>@@spid 
  " +

  case
    when @prm_filtro <> '' then " and upper(r.command) like '" + upper('%'+@prm_filtro+'%') + "'"
	else " and 1=1"
  end  +

"  			
  order by
    percent_complete
"

  set NOCOUNT OFF
  exec (@cmd1)

  set NOCOUNT ON
  print ''
  set NOCOUNT OFF
  return 0
end

GO


if object_id ( 'u.tbstat', 'P' ) is not null
  drop procedure u.tbstat
GO

create procedure u.tbstat
(
  @prm_obj_name                nvarchar(128) = null
) as
begin
  set NOCOUNT ON

  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output, 
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_base_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end


  set NOCOUNT OFF
  select
    st.stats_id,
    substring (st.name, 1, 60) as STATS_NAME,
    stats_date(st.object_id, st.stats_id) as STATS_DATE,
    st.auto_created,
    st.user_created,
    st.no_recompute,
    case when idx.is_hypothetical is null then 0 else idx.is_hypothetical end as HYPOTHETICAL,
    case when idx.name is null then '' else substring (idx.name, 1, 60) end as INDEX_NAME
  from
    sys.stats st
      left outer join sys.indexes idx on
           idx.object_id = st.object_id and
           idx.index_id  = st.stats_id
  where
    st.object_id = object_id(@obj_base_name)
  order by index_name, 2, 3;
  set NOCOUNT ON

  print ''
  print "Para informacoes mais detalhadas, digite:  dbcc show_statistics ('" + @obj_base_name + "', '<stats-name>')"
  print ''

  print ''
  set NOCOUNT OFF
  return 0
end

GO

if object_id ( 'u.sqlfind', 'P' ) is not null
  drop procedure u.sqlfind
GO


create procedure u.sqlfind (
  @prm_sqltext                 varchar(255),
  @prm_top                     integer = 100,
  @prm_orderby                 varchar(8) = 'totexec',
  @prm_mincount                integer = 0,
  @prm_hoje                    integer = 0,
  @prm_LastMin                 integer = 0,
  @prm_db                      nvarchar(30) = ''
) as
begin
  set NOCOUNT OFF

  declare @cmd1              varchar(8000)
  declare @orderby           varchar(128)
  declare @var_sql_handle    varbinary(64) = 0xFF00FF00

  if substring(@prm_sqltext, 1, 2) = '0x'
     set @var_sql_handle = convert(varbinary(64), @prm_sqltext, 1);

  if @prm_sqltext in ('', '?', '/?', '-?', '-h')
    begin
    print ""
    print "Formato: u.sqlfind {<filtro> | <sql handle>} [, parametro] [, parametro] [, @prm=xyz] [...]"
    print ""
    print "  @prm_top     : {1-n} numero de linhas a mostrar (default = 100)"
    print "  @prm_hoje    : {0|1} filtrar somente os planos criados hoje (default = 0)"
    print "  @prm_mincount: {0-n} numero minimo de execucoes para considerar o sql na listagem (default = 0)"
    print "  @prm_LastMin : {minutos}  filtrar somente os planos executados nos ultimos n minutos (default = nao filtrar)"
    print "  @prm_db      : {database} filtrar somente os planos do database indicado (default = nao filtrar)"
    print ""
    return 1
  end

  set @prm_sqltext = lower(@prm_sqltext)

  set @orderby =
    case when @prm_orderby = 'totexec'  then "qst.EXECUTION_COUNT desc"
         when @prm_orderby = 'totwork'  then "qst.TOTAL_WORKER_TIME desc"
         when @prm_orderby = 'maxwork'  then "qst.TOTAL_WORKER_TIME desc"
         when @prm_orderby = 'avgwork'  then "'AVG_WORK_TIME (s)' desc"

         when @prm_orderby = 'totelap'  then "qst.TOTAL_ELAPSED_TIME desc"
         when @prm_orderby = 'maxelap'  then "qst.MAX_ELAPSED_TIME desc"
         when @prm_orderby = 'avgelap'  then "'AVG_ELAP_TIME (s)' desc"

         when @prm_orderby = 'totlr'    then "qst.TOTAL_LOGICAL_READS desc"
         when @prm_orderby = 'avglr'    then "'AVG_LOGICAL_READS' desc"

         when @prm_orderby = 'totpr'    then "qst.TOTAL_PHYSICAL_READS desc"
         when @prm_orderby = 'avgpr'    then "'AVG_PHY_READS' desc"
         else "erro"
    end

  if @orderby = 'erro' 
    begin
    print ""
    print "***** ERRO: O parametro @orderby está incorreto."
    print ""
    print "Valores permitidos: totexec, totwork, maxwork, avgwork, totelap, maxelap, avgelap, totlr, avglr, totpr, avgpr"
    print ""
    return 1
  end

  set @cmd1=
    'select top ' + cast(@prm_top as varchar) + 
"
    (select cast(db_name(cast(pattr.value as integer)) as varchar(15)) from sys.dm_exec_plan_attributes (qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') as DBNAME,
    qst.EXECUTION_COUNT,
    cast ((1.0 * qst.TOTAL_WORKER_TIME) / 1000000 as decimal(15,3)) as 'WORKER_TIME (s)',
    cast (((1.0 * qst.TOTAL_WORKER_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_WORK_TIME (s)',
    cast ((1.0 * qst.MAX_WORKER_TIME) / 1000000 as decimal(15,3)) as 'MAX_WORK_TIME (s)',
    cast ((1.0 * qst.TOTAL_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'ELAPSED_TIME (s)',
    cast (((1.0 * qst.TOTAL_ELAPSED_TIME) / qst.EXECUTION_COUNT) / 1000000 as decimal(15,3)) as 'AVG_ELAP_TIME (s)',
    cast ((1.0 * qst.MAX_ELAPSED_TIME) / 1000000 as decimal(15,3)) as 'MAX_ELAP_TIME (s)',
    qst.TOTAL_LOGICAL_READS as 'LOGICAL_READS',
    cast (qst.TOTAL_LOGICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_LOGICAL_READS',
    qst.TOTAL_PHYSICAL_READS as 'PHY_READS',
    cast (qst.TOTAL_PHYSICAL_READS / qst.EXECUTION_COUNT as bigint) as 'AVG_PHY_READS',
    qst.PLAN_GENERATION_NUM,
    qst.CREATION_TIME,
    qst.LAST_EXECUTION_TIME,
	
	(select cast (max(master.dbo.fn_varbintohexstr(qst1.PLAN_HANDLE)) as varchar(100))
	   from sys.dm_exec_query_stats qst1
	  where qst1.sql_handle = qst.sql_handle ) as PLAN_HANDLE,
	  
	(select cast (max(master.dbo.fn_varbintohexstr(qst1.SQL_HANDLE)) as varchar(100))
	   from sys.dm_exec_query_stats qst1
	  where qst1.sql_handle = qst.sql_handle ) as SQL_HANDLE,
	
 --   cast (master.dbo.fn_varbintohexstr(qst.PLAN_HANDLE) as varchar(100)) as PLAN_HANDLE,
 --   cast (master.dbo.fn_varbintohexstr(qst.SQL_HANDLE) as varchar(100)) as SQL_HANDLE,
    qst.statement_start_offset,
    qst.statement_end_offset,
--  cast ((sql.TEXT as varchar(700)),char(10), ' '), char(13), ' ') from sys.dm_exec_sql_text(qst.SQL_HANDLE) sql) as varchar(700)) as SQL2
    cast ((select u._sqlresumo(sql.TEXT, 100) from sys.dm_exec_sql_text(qst.SQL_HANDLE) sql) as varchar(700)) as SQL2
  from
    sys.dm_exec_query_stats qst
  where
" +
    case when substring(@prm_sqltext, 1, 2) = '0x' then
         "     qst.sql_handle = " + @prm_sqltext + char(10)
         else
         "     (select lower(replace(replace(sql.TEXT, char(10), ' '), char(13), ' ')) from sys.dm_exec_sql_text(qst.SQL_HANDLE) sql) like """ + @prm_sqltext + """" + char(10)
    end +

    case when @prm_hoje = 1 then
         "       and qst.CREATION_TIME >= convert(DateTime, substring(convert(varchar, getdate(), 120), 1, 10), 120)" + char(10)
         else ""
    end +

    case when @prm_LastMin <> 0 then
         "       and qst.LAST_EXECUTION_TIME >= dateadd(mi, -"+ cast(@prm_LastMin as varchar) +", getdate())" + char(10)
         else ""
    end +

    case when @prm_mincount > 0 then
         "       and qst.EXECUTION_COUNT >= " + cast(@prm_minCount as varchar) + char(10)
         else ""
    end +

    case when @prm_db <> '' then
         "       and (select db_name(cast(pattr.value as integer)) from sys.dm_exec_plan_attributes(qst.PLAN_HANDLE) pattr where pattr.attribute='dbid') = '" + @prm_db + "'" + char(10)
         else ""
    end +

    " order by " + @orderby


  set NOCOUNT OFF
--print @cmd1
  exec (@cmd1)

  set NOCOUNT ON
  print ''
  set NOCOUNT OFF
  return 0
end
GO



if object_id ( 'u._leColFk', 'FN' ) is not null
  drop function u._leColFk
GO

create function u._leColFk
(
  @prm_object_id      int
) returns nvarchar(4000) as

begin

  declare @fkcols           nvarchar(4000)
  declare @fkcol            nvarchar(133)

  set @fkcols = ''

  declare fkcol_cursor cursor  local forward_only read_only
    for
      select
        col.name as fkcol
      from
        sys.foreign_key_columns fkc
          join sys.all_columns col  on
             col.object_id  = fkc.parent_object_id and
             col.column_id  = fkc.parent_column_id
      where
        fkc.constraint_object_id = @prm_object_id
      order by
        fkc.constraint_column_id;


  open fkcol_cursor
  fetch next from fkcol_cursor into @fkcol
  while @@FETCH_STATUS = 0
  begin
    set @fkcols = @fkcols + (case when @fkcols != '' then ' + ' else '' end) + @fkcol
    fetch next from fkcol_cursor into @fkcol
  end
  close fkcol_cursor
  deallocate fkcol_cursor

  return @fkcols
end

GO



if object_id ( 'u.tbfk', 'P' ) is not null
  drop procedure u.tbfk
GO

create procedure u.tbfk
(
  @prm_obj_name                nvarchar(128) = null
) as
begin
  set NOCOUNT ON

  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output,
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_base_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end



  set NOCOUNT OFF
  select
    substring (schema_name(fk1.schema_id), 1, 10) as FK_SCHEMA,
    substring (fk1.name, 1, 30) as FK_NAME,
    substring (schema_name(objectproperty(fk1.referenced_object_id, 'SCHEMAID')), 1, 10) as TB_SCHEMA,
    substring (object_name(fk1.referenced_object_id), 1, 30) as TB_NAME,
    substring (fk1.update_referential_action_desc, 1, 12) as UPDATE_RULE,
    substring (fk1.delete_referential_action_desc, 1, 12) as DELETE_RULE,
    fk1.is_disabled as DISB,
    fk1.is_not_trusted as NTRUS,
    fk1.is_not_for_replication as NREP,
    fk1.is_system_named as SYSN,
    substring (u._leColFk(fk1.object_id) + '  (' + schema_name(objectproperty(idx.object_id,'SCHEMAID')) + '.' + idx.name + ')', 1, 250) as COLS
  from
    sys.foreign_keys fk1
      join sys.indexes idx on
           idx.object_id = fk1.referenced_object_id and
           idx.index_id  = fk1.key_index_id
  where
    fk1.parent_object_id = object_id(@obj_base_name)
  order by 1, 2;
  set NOCOUNT ON


  print ''
  set NOCOUNT OFF
  return 0
end

GO



if object_id ( 'u.tbfkref', 'P' ) is not null
  drop procedure u.tbfkref
GO

create procedure u.tbfkref
(
  @prm_obj_name                nvarchar(128) = null
) as
begin
  set NOCOUNT ON

  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output,
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_base_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end



  set NOCOUNT OFF
  select
    substring (schema_name(fk1.schema_id), 1, 10) as FK_SCHEMA,
    substring (fk1.name, 1, 30) as FK_NAME,
    substring (schema_name(objectproperty(fk1.parent_object_id, 'SCHEMAID')), 1, 10) as TB_SCHEMA,
    substring (object_name(fk1.parent_object_id), 1, 30) as TB_NAME,
    substring (fk1.update_referential_action_desc, 1, 12) as UPDATE_RULE,
    substring (fk1.delete_referential_action_desc, 1, 12) as DELETE_RULE,
    fk1.is_disabled as DISB,
    fk1.is_not_trusted as NTRUS,
    fk1.is_not_for_replication as NREP,
    fk1.is_system_named as SYSN,
    substring (u._leColFk(fk1.object_id) + '  (' + schema_name(objectproperty(idx.object_id,'SCHEMAID')) + '.' + idx.name + ')', 1, 250) as COLS
  from
    sys.foreign_keys fk1
      join sys.indexes idx on
           idx.object_id = fk1.referenced_object_id and
           idx.index_id  = fk1.key_index_id
  where
    fk1.referenced_object_id = object_id(@obj_base_name)
  order by 3, 4, 1, 2;
  set NOCOUNT ON


  print ''
  set NOCOUNT OFF
  return 0
end

GO


if object_id ( 'u.tbidxfrag', 'P' ) is not null
  drop procedure u.tbidxfrag
GO

create procedure u.tbidxfrag
(
  @prm_obj_name                nvarchar(128) = null
) as
begin
  set NOCOUNT ON

  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output, 
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_base_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end


  set NOCOUNT OFF
  select
    substring (idx.name, 1, 60) as INDEX_NAME,
    substring (idx.type_desc, 1, 20) as INDEX_TYPE,
    idx.is_primary_key as PK,
    idx.is_unique as UNIQ,        
    idx.is_unique_constraint as UN_C,
    idx.is_disabled as DISAB,
    idx.is_hypothetical as HYPT,
    idx.ignore_dup_key as IGDUP,
    idx.allow_row_locks as RLK,
    idx.allow_page_locks as PLK,
    idx.fill_factor as FILL,
    idx.IS_PADDED,
    idx.INDEX_ID,
    stats.partition_number as 'Partition.No',
    stats.index_depth,
    stats.index_level,
    cast (stats.avg_fragmentation_in_percent as decimal(7,4)) as 'Avg.Frag(%)',
    stats.fragment_count,
    stats.page_count,
    cast (stats.avg_fragment_size_in_pages as decimal(15,4)) as 'Avg.Frag.Sz(pages)',
   (select substring(ds.name,1,32) from sys.data_spaces ds where ds.data_space_id = idx.data_space_id) as DATA_SPACE,
    substring(stats.alloc_unit_type_desc, 1, 20) as 'Alloc.Type',
    substring (u._lecolindex(idx.object_id, idx.index_id), 1, 255) as INDEX_COLUMNS
  from
    sys.dm_db_index_physical_stats (DB_ID(), object_id(@obj_base_name), NULL, NULL, NULL) as STATS
      join sys.indexes idx on
           idx.object_id = STATS.object_id and
           idx.index_id  = STATS.index_id
  where
    idx.index_id != 0
  order by 1;
  set NOCOUNT ON


  print ''
  set NOCOUNT OFF
  return 0
end

GO


if object_id ( 'u.tbsocupa', 'P' ) is not null
  drop procedure u.tbsocupa
GO

create procedure u.tbsocupa as
begin
  set NOCOUNT ON

  print ''
  print ''
  SELECT
    substring(b.groupname,1,30) as 'File Group',
    substring(a.Name,1,30)      as 'Name',
    substring(a.Filename,1,70) as 'File name',
    CONVERT (Decimal(15,2), ROUND(a.Size/128.000,2)) as 'Allocated (MB)',
    CONVERT (Decimal(15,2), ROUND(FILEPROPERTY(a.Name,'SpaceUsed')/128.000,2)) as 'Used (MB)',
    CONVERT (Decimal(15,2), ROUND((a.Size-FILEPROPERTY(a.Name,'SpaceUsed'))/128.000,2)) as 'Available (MB)'
  FROM 
    sysfilegroups b (NOLOCK) 
      LEFT OUTER JOIN dbo.sysfiles a (NOLOCK) 
        ON a.groupid = b.groupid 
  ORDER BY 
    b.groupname;  
 

  print ''
  print ''
  SELECT  
    SUM(CONVERT (Decimal(15,2),  ROUND(a.Size/128.000,2))) as 'Total alocado (MB)', 
    SUM(ROUND(FILEPROPERTY(a.Name,'SpaceUsed')/128.000,2)) as 'Total utilizado (MB)',   
    SUM(ROUND((a.Size-FILEPROPERTY(a.Name,'SpaceUsed'))/128.000,2)) as 'Total disponivel (MB)'
  FROM 
    dbo.sysfiles a (NOLOCK)  
      JOIN sysfilegroups b (NOLOCK)
        ON a.groupid = b.groupid;

  print ''
  print ''
  set NOCOUNT OFF
  return 0
end
GO

if object_id ( 'u.tbtrg', 'P' ) is not null
  drop procedure u.tbtrg
GO

create procedure u.tbtrg
(
  @prm_obj_name                nvarchar(256) = null
) as

begin
  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  set NOCOUNT ON

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output, 
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end


  set NOCOUNT OFF

  select 
    substring (schema_name(obj.schema_id), 1, 24) as OBJ_SCHEMA,
    substring (obj.name, 1, 60) as TRIGGER_NAME,
    case when objectproperty(obj.object_id, 'ExecIsInsertTrigger')=1 then 'INSERT'
         when objectproperty(obj.object_id, 'ExecIsUpdateTrigger')=1 then 'UPDATE'
         when objectproperty(obj.object_id, 'ExecIsDeleteTrigger')=1 then 'DELETE'
         else '        '
    end as 'RULE',
    objectproperty(obj.object_id, 'ExecIsAfterTrigger') as IS_AFTER,
    objectproperty(obj.object_id, 'ExecIsTriggerNotForRepl') as NOT_FOR_REPLIC,
    objectproperty(obj.object_id, 'ExecIsTriggerDisabled') as DISABLED,
    is_ms_shipped,
    obj.CREATE_DATE,
    obj.MODIFY_DATE
  from
    sys.all_objects obj
  where
    obj.parent_object_id = object_id(@obj_base_name) and
    obj.type = 'TR'
  order by 1, 2;


  set NOCOUNT ON
  return 0
end

GO


if object_id ( 'u.tbsocupadb', 'P' ) is not null
  drop procedure u.tbsocupadb
GO

create procedure u.tbsocupadb as
begin
  set NOCOUNT ON

  print ''
  print ''    
  SELECT 
    database_id,
    substring(name, 1, 15) as db_name,
    create_date,
    (SELECT sum(SIZE * 8.0 / 1024) as SIZE FROM sys.master_files ms where ms.TYPE = 0 AND ms.database_id = db.database_id) as 'DataFileSize (MB)',
    (SELECT sum(SIZE * 8.0 / 1024) as SIZE FROM sys.master_files ms where ms.TYPE = 1 AND ms.database_id = db.database_id) as 'LogFileSize (MB)',
    cast(recovery_model as varchar(10)) + '-' + substring(recovery_model_desc, 1, 10) as recovery_model,
    cast(state as varchar(10)) + '-' + substring(state_desc, 1, 10) as state,
    is_in_standby,
    is_read_only,
    compatibility_level,
    substring(collation_name, 1, 35) as collation_name,
    substring(user_access_desc, 1, 10) as user_access_desc,
    cast(log_reuse_wait as varchar(10)) + '-' + substring(log_reuse_wait_desc, 1, 20) as log_reuse_wait,
    is_auto_close_on,
    is_auto_shrink_on,
    is_cleanly_shutdown,
    is_supplemental_logging_enabled,
    cast(snapshot_isolation_state as varchar(10)) + '-' + substring(snapshot_isolation_state_desc, 1, 10) as snapshot_isolation_state,
    is_read_committed_snapshot_on,
    cast(page_verify_option as varchar(10)) + '-' + substring(page_verify_option_desc, 1, 10) as page_verify_option,
    is_auto_create_stats_on,
    is_auto_create_stats_incremental_on,
    is_auto_update_stats_on,
    is_auto_update_stats_async_on,
    is_ansi_null_default_on,
    is_ansi_nulls_on,
    is_ansi_padding_on,
    is_ansi_warnings_on,
    is_arithabort_on,
    is_concat_null_yields_null_on,
    is_numeric_roundabort_on,
    is_quoted_identifier_on,
    is_recursive_triggers_on,
    is_cursor_close_on_commit_on,
    is_local_cursor_default,
    is_fulltext_enabled,
    is_trustworthy_on,
    is_db_chaining_on,
    is_parameterization_forced,
    is_master_key_encrypted_by_server,
    is_query_store_on,
    is_published,
    is_subscribed,
    is_merge_published,
    is_distributor,
    is_sync_with_backup,
    service_broker_guid,
    is_broker_enabled,
    is_date_correlation_on,
    is_cdc_enabled,
    is_encrypted,
    is_honor_broker_priority_on,
    replica_id,
    group_database_id,
    resource_pool_id,
    cast(default_language_lcid as varchar(10)) + '-' + substring(default_language_name, 1, 20) as default_language,
    cast(default_fulltext_language_lcid as varchar(10)) + '-' + substring(default_fulltext_language_name, 1, 20) as default_fulltext_language,
    is_nested_triggers_on,
    is_transform_noise_words_on,
    two_digit_year_cutoff,
    cast(containment as varchar(10)) + '-' + substring(containment_desc, 1, 20) as containment,
    target_recovery_time_in_seconds,
    cast(delayed_durability as varchar(10)) + '-' + substring(delayed_durability_desc, 1, 20) as delayed_durability,
    is_memory_optimized_elevate_to_snapshot_on,
    is_federation_member,
    is_remote_data_archive_enabled,
    is_mixed_page_allocation_on
  FROM
    sys.databases db
  order by
    database_id;

  print ''
  print ''
  select
    database_id,
	db_name,
	create_date,
    sum(DataFileSize_MB) as 'DataFileSize (MB)',
    sum(LogFileSize_MB) as 'LogFileSize (MB)'
  from
    (
	 SELECT 
       '**********' as database_id,
       'Tamanho total  ' as db_name,
       '****-**-** **:**:**.***' as create_date,
       (SELECT sum(SIZE * 8.0 / 1024) as SIZE FROM sys.master_files ms where ms.TYPE = 0 AND ms.database_id = db.database_id) as 'DataFileSize_MB',
       (SELECT sum(SIZE * 8.0 / 1024) as SIZE FROM sys.master_files ms where ms.TYPE = 1 AND ms.database_id = db.database_id) as 'LogFileSize_MB'
     FROM
      sys.databases db
	) as CalcSumDtb
   group by
    database_id,
	db_name,
	create_date;
	
  print ''
  print ''
  set NOCOUNT OFF
  return 0
end
GO


if object_id ( 'u.tbgrant', 'P' ) is not null
  drop procedure u.tbgrant
GO

create procedure u.tbgrant
(
  @prm_obj_name                nvarchar(256) = null
) as

begin
  declare @obj_type          char(2)
  declare @obj_type_txt      nvarchar(20)
  declare @obj_base_name     nvarchar(256)
  declare @obj_base_type     char(2)

  set NOCOUNT ON

  exec u._analisarobj @prm_obj_name, @prm_obj_type = @obj_type output, @prm_obj_type_txt = @obj_type_txt output, 
                     @prm_obj_base_name = @obj_base_name output, @prm_obj_base_type = @obj_base_type output

  if @obj_type is null or @obj_base_type not in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF')
  begin
    print ''
    print '***** ERRO: A tabela, view ou synonym nao existe.'
    print ''
    return 1
  end

  set NOCOUNT OFF
  select
    substring (U2.name, 1, 20) as GRANTEE,
    substring (U2.type_desc, 1, 20) as TYPE,
    substring (U1.name, 1, 20) as GRANTOR,
    substring (U1.type_desc, 1, 20) as TYPE,
    substring (P.state_desc, 1, 23) as 'GRANT',
    substring(P.permission_name, 1, 38) as PERMISSION,
    case when P.minor_id = 0 then 'ALL'
         else (select substring (col.name, 1, 32) from sys.all_columns COL where COL.object_id = P.major_id and COL.column_id = P.minor_id)
    end as COLUMNS
  from
    sys.database_permissions P
      left outer join sys.database_principals U1 on
           U1.principal_id = P.grantor_principal_id
      left outer join sys.database_principals U2 on
           U2.principal_id = P.grantee_principal_id
  where
    P.major_id = object_id(@prm_obj_name)
  order by
    1, 2, P.minor_id, 5 desc, 6;
  set NOCOUNT ON
 
  print ''
  print 'Tipo do objeto: ' + @obj_type_txt + (case when @obj_type = 'SN' then ' (' + @obj_base_name + ')' else '' end)
  print ''
  return 0
end

GO


if object_id ( 'u.tbfindlistalter', 'P' ) is not null
  drop procedure u.tbfindlistalter
GO

create procedure u.tbfindlistalter
(
  @prm_obj_name_like                nvarchar(256) = '',
  @prm_dias                         int = 7,
  @prm_data1                        varchar(10) = '',
  @prm_data2                        varchar(10) = ''

) as

begin
  declare @data1        datetime;
  declare @data2        datetime;

  if @prm_obj_name_like in ('', '?', '/?', '-?', '-h')
    begin
    print ""
    print "Formato: u.tbfindlistalter <filtro> [, @prm_dias] [, @prm_data1] [, @prm_data2]"
    print ""
    print "  <filtro>     : Filtro no formato 'owner.tabela'. Aceita wildcards."
    print "  @prm_dias    : Listar a partir de 'n' dias anteriores ao dia atual (default = 7)"
    print "                 * este valor é ignorado se alguma data for informada."
    print "  @prm_data1   : Data inicial"
    print "  @prm_data2   : Data final (tem que ser maior ou igual a data inicial)"
    print ""
    print "  Ex: u.tbfindlistalter 'SAJ.E%'"
    print "      u.tbfindlistalter 'SAJ.EFPG%', 15"
    print "      u.tbfindlistalter 'SAJ.%', 3"
    print "      u.tbfindlistalter '%.%'"
    print "      u.tbfindlistalter 'SAJ.%', 0, '2014-06-01', '2014-06-05'"
    print "      u.tbfindlistalter 'SAJ.%', @prm_data1='2014-06-01', @prm_data2='2014-06-05'"
    print ""
    return 1
  end

  if charindex('.', @prm_obj_name_like) <> 0 
    set @prm_obj_name_like = lower(@prm_obj_name_like)
  else
    set @prm_obj_name_like = '%.' + lower(@prm_obj_name_like)

  if right(@prm_obj_name_like, 1) = '.' set @prm_obj_name_like = @prm_obj_name_like + '%'


  if @prm_data1 <> '' or @prm_data2 <> ''
    begin
    if @prm_data1 <> '' and @prm_data2 <> '' 
       begin
       set @data1 = convert(datetime, @prm_data1, 120)
       set @data2 = convert(datetime, @prm_data2, 120) + 1
       end
    else if @prm_data1 <> '' and @prm_data2 = '' 
       begin
       set @data1 = convert(datetime, @prm_data1, 120)
       set @data2 = convert(datetime, @prm_data1, 120) + 1
       end
    else if @prm_data1 = '' and @prm_data2 <> '' 
       begin
       set @data1 = convert(datetime, @prm_data2, 120)
       set @data2 = convert(datetime, @prm_data2, 120) + 1
       end
    end
  else
    begin
      set @data1 = dateadd(dd,0, datediff(dd,0, getDate())) - @prm_dias
      set @data2 = dateadd(dd,0, datediff(dd,0, getDate())) + 1
    end


  select 
    substring (schema_name(obj.schema_id), 1, 24) as OBJ_SCHEMA,
    substring (obj.name, 1, 40) as NAME,
    substring (obj.type_desc, 1, 20) as TYPE,
    obj.MODIFY_DATE,
    obj.CREATE_DATE,
   (select count(*) from sys.indexes idx where idx.object_id = obj.object_id and idx.index_id != 0) as Qtd_Idx,
   (select count(*) from sys.foreign_keys fk1 where fk1.parent_object_id = obj.object_id) as Qtd_FKs,
   (select count(*) from sys.foreign_keys fk1 where fk1.referenced_object_id = obj.object_id) as Qtd_FKREFs,
    tab.large_value_types_out_of_row as LOBS_OUT,
    obj.IS_PUBLISHED as PUBL,
    (
    case when obj.type = 'V' then avw.IS_REPLICATED 
                             else tab.IS_REPLICATED 
    end
    ) as REPL
  from
    sys.all_objects obj
      left outer join sys.tables tab  on
           tab.object_id = obj.object_id
      left outer join sys.all_views avw  on
           avw.object_id = obj.object_id
  where
    obj.MODIFY_DATE >= @data1 and obj.MODIFY_DATE < @data2 and
    lower(schema_name(obj.schema_id) + '.' + rtrim(obj.name)) like @prm_obj_name_like and
    obj.type in ('U', 'V', 'FT', 'IF', 'IT', 'S', 'TF', 'SN')
  order by
    obj.MODIFY_DATE desc, OBJ_SCHEMA, NAME;

end

GO


if object_id ( 'u.tbtbs', 'P' ) is not null
  drop procedure u.tbtbs
GO

create procedure u.tbtbs
(
  @prm_obj_name_like                nvarchar(256) = null
) as

begin
  declare @obj_name_like2 nvarchar(256)


  if charindex('.', @prm_obj_name_like) <> 0 
    begin
    set @prm_obj_name_like = lower(@prm_obj_name_like)
    if lower(substring(@prm_obj_name_like, 1, charindex('.', @prm_obj_name_like)))='saj.'
       set @obj_name_like2 = 'repl'+ lower(substring(@prm_obj_name_like, charindex('.', @prm_obj_name_like), 250))
    else
       set @obj_name_like2 = lower(@prm_obj_name_like)
    end
  else
    begin
    set @prm_obj_name_like = '%.' + lower(@prm_obj_name_like)
    set @obj_name_like2 = @prm_obj_name_like
    end


  select
    substring (schema_name (objectproperty (si.id, 'SchemaId')), 1, 15) as SCHEMANAME,
    substring (object_Name(si.id), 1, 22) as TABLENAME,
    substring (idx.type_desc, 1, 15) as TYPE,
    case when idx.type_desc = 'HEAP' then '' else substring (si.Name, 1, 30) end as INDEXNAME,
    idx.index_id as ID,
    idx.is_primary_key as PK,
    idx.is_unique as UNIQ,        
    idx.is_disabled as DISAB,
    substring (fg.groupname, 1, 22) as FILEGROUP
--  substring (u._lecolindex(idx.object_id, idx.index_id), 1, 255) as INDEX_COLUMNS
  from
    sys.sysindexes si
      join sys.sysfilegroups fg on
           fg.groupid = si.groupid
      join sys.indexes idx on
           idx.object_id = si.id and
           idx.index_id  = si.indid
  where
    (
      lower(schema_name (objectproperty (si.id, 'SchemaId')) +'.'+ rtrim(object_Name (si.id))) like @prm_obj_name_like
      or
      lower(schema_name (objectproperty (si.id, 'SchemaId')) +'.'+ rtrim(object_Name (si.id))) like @obj_name_like2
    )
  order by 1, 2, 3, 4;

end

GO




