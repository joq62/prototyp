%%% -------------------------------------------------------------------
%%% Author  : uabjle
%%% Description : dbase using dets 
%%%
%%% Created : 10 dec 2012
%%% -------------------------------------------------------------------
-module(a).

-define(DNS_INFO(Zone,ServiceId,IpAddr,Port,Time,Schedule),{Zone,ServiceId,IpAddr,Port,Time,Schedule}).
-define(DNS_INFO_ZONE(DnsInfo),element(1,hd([DnsInfo]))).
-define(DNS_INFO_SERVICE(DnsInfo),element(2,hd([DnsInfo]))).
-define(DNS_INFO_IPADDR(DnsInfo),element(3,hd([DnsInfo]))).
-define(DNS_INFO_PORT(DnsInfo),element(4,hd([DnsInfo]))).
-define(DNS_INFO_TIME(DnsInfo),element(5,hd([DnsInfo]))).
-define(DNS_INFO_SCHEDULE(DnsInfo),element(6,hd([DnsInfo]))).

-define(DNS_TABLE,dns_table).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
%%  -include("").
%% --------------------------------------------------------------------
%% External exports
-compile(export_all).



%% ====================================================================
%% External functions
%% ====================================================================
s()->
    init(),
    b().

b()->
    
    io:format("all services, all info  on node zone_a ~p~n",[ets:lookup(?DNS_TABLE,"zone_a")]),
    io:format("all services , ip_addr and port ~p~n",[ets:match(?DNS_TABLE,{'_','$2','$3','$4','_','_'})]),
    io:format("all services , time_stamp ~p~n",[ets:match(?DNS_TABLE,{'$10','$11','_','_','$12','_'})]),
    io:format("whole table  ~p~n",[ets:match(?DNS_TABLE,'$1')]),
    io:format("whole table tab2list ~p~n",[ets:tab2list(?DNS_TABLE)]),

    % remove old >125 
    All=ets:match(?DNS_TABLE,'$1'),
    Keep=[{Zone,Service,IpAddr,Port,Time,Schedule}||[{Zone,Service,IpAddr,Port,Time,Schedule}]<-All,
						 Time<150],
%    Remove=[{Zone,Service,IpAddr,Port,Time,Schedule}||[{Zone,Service,IpAddr,Port,Time,Schedule}]<-All,
%					   Time>150],
    io:format("Keep  ~p~n",[Keep]),
%   io:format("Remove  ~p~n",[Remove]),
 %   [ets:delete_object(?DNS_TABLE,X)||X<-Remove],
    
    remove_expired(150),
    io:format("reduced tabel table ~p~n",[ets:match(?DNS_TABLE,'$1')]),

    io:format("sd zone_c,service_1 ~p~n",[dns("zone_c","service_1")]),
    [{"80.216.3.159",60003}]=dns("zone_c","service_1"),
    []=dns("zone_c","service_2"),

    [{"80.216.3.159",60003},
     {"80.216.3.159",60001},
     {"80.216.3.159",60002}]=dns("service_1"),
    io:format("sd service_1 ~p~n",[dns("service_1")]),

    % update - new
    New={"zone_a","service_3","70.216.3.159",60002,200,na},
    update_dns(New),
    io:format("updated with New table ~p~n",[ets:match(?DNS_TABLE,'$1')]),
    
    % update - existing
    A20={"zone_b","service_1","80.216.3.159",60002,45,na}, 
    update_dns(A20),
    io:format("updated with a20 table ~p~n",[ets:match(?DNS_TABLE,'$1')]),    

    % test update complete dns_table brutal force

    update_all_brutal(ets:tab2list(?DNS_TABLE)),

    case ets:info(?DNS_TABLE) of
	undefined->
	    io:format("ets:info ?DNS_TABLE 1 ~p~n",[undefined]);
	_->
	    io:format("delete ?DNS_TABLE 1 ~p~n",[ets:delete(?DNS_TABLE)])
    end,	   
    case ets:info(?DNS_TABLE) of
	undefined->
	    io:format("ets:info ?DNS_TABLE 2 ~p~n",[undefined]);
	_->
	    io:format("delete ?DNS_TABLE 1 ~p~n",[ets:delete(?DNS_TABLE)])
    end, 
    ok.






%    {time_stamp="not_initiaded_time_stamp",    % un_loaded, started
%	   zone ="zone not initiaded",
%	   service_id = "not_initiaded_service_id",
%	   ip_addr="not_initiaded_ip_addr",
%	   port="not_initiaded_port",
%	   schedule_info="not used"
%	 }

init()->
    ?DNS_TABLE=init_dns_table(),
    A1=?DNS_INFO("zone_a","service_1","80.216.3.159",60001,120,na),   
    A2=?DNS_INFO("zone_b","service_1","80.216.3.159",60002,120,na), 
    A3=?DNS_INFO("zone_c","service_1","80.216.3.159",60003,130,na), 
    B1=?DNS_INFO("zone_a","service_2","70.216.3.159",60001,120,na), 
    B2=?DNS_INFO("zone_z","service_2","70.216.3.159",60002,200,na), 
    
    DnsList=[A1,A2,A3,B1,B2],
    [update_dns(DnsInfo)||DnsInfo<-DnsList],
    ok.
    
%% --------------------------------------------------------------------
%% Function: 
%% Description:
%% Returns: non
%% --------------------------------------------------------------------
init_dns_table()->
    Reply=case ets:info(?DNS_TABLE) of
	      undefined->
		  ets:new(?DNS_TABLE,[bag,named_table,public]);
	      _->
		  ets:delete(?DNS_TABLE),
		  ets:new(?DNS_TABLE,[bag,named_table,public])
	  end, 
    Reply.

update_dns(DnsInfoUpdate)->
    L=ets:tab2list(?DNS_TABLE),    
    L1=[DnsInfoUpdate||DnsInfo<-L,
		       {?DNS_INFO_ZONE(DnsInfoUpdate),?DNS_INFO_SERVICE(DnsInfoUpdate)}=={?DNS_INFO_ZONE(DnsInfo),?DNS_INFO_SERVICE(DnsInfo)}],
    R=case L1 of
	  []->
	      ets:insert(?DNS_TABLE,DnsInfoUpdate);
	  [Remove] ->
	      ets:delete_object(?DNS_TABLE,Remove),
	      ets:insert(?DNS_TABLE,DnsInfoUpdate)
      end,
    R.


dns(ZoneWanted,ServiceIdWanted)->
    L=ets:lookup(?DNS_TABLE,ZoneWanted),
    R=[{?DNS_INFO_IPADDR(DnsInfo),?DNS_INFO_PORT(DnsInfo)}||DnsInfo<-L,
		      ?DNS_INFO_SERVICE(DnsInfo)=:=ServiceIdWanted],
    R.

dns(ServiceIdWanted)->
    L1=ets:match(?DNS_TABLE,{'_','$2','$3','$4','_','_'}),
    R=[{IpAddr,Port}||[ServiceId,IpAddr,Port]<-L1,
		      ServiceId=:=ServiceIdWanted],
    R.

remove_expired(ExpireTime)->
    L=ets:tab2list(?DNS_TABLE),    
    Remove=[DnsInfo||DnsInfo<-L,
		     ?DNS_INFO_TIME(DnsInfo)>ExpireTime],
    [ets:delete_object(?DNS_TABLE,X)||X<-Remove].

update_all_brutal(NewTabList)->
    ?DNS_TABLE=init_dns_table(),
    io:format("empty table ~p~n",[ets:tab2list(?DNS_TABLE)]),
    ets:insert(?DNS_TABLE,NewTabList),
    io:format("updated table ~p~n",[ets:tab2list(?DNS_TABLE)]).

update_all(NewTabList)->
    TabList=ets:tab2list(?DNS_TABLE),
    Equal=equal_dns(TabList,NewTabList),
    {Added,Removed}=diff_dns(TabList,NewTabList),
    [ets:insert(?DNS_TABLE,DnsInfo)||DnsInfo<-Added],
    [ets:delete_object(?DNS_TABLE,DnsInfo)||DnsInfo<-Removed].  % Glurk kanske inte funkar eftersom time_stamp kan vara annorlunda


    
equal_dns(DnsList_1,DnsList_2)->
    [DnsInfo_1||DnsInfo_1<-DnsList_1,DnsInfo_2<-DnsList_2,
		{?DNS_INFO_ZONE(DnsInfo_1),?DNS_INFO_SERVICE(DnsInfo_1)}=:={?DNS_INFO_ZONE(DnsInfo_2),?DNS_INFO_SERVICE(DnsInfo_2)}].

diff_dns(DnsList_1,DnsList_2)->
    Removed=diff_dns(DnsList_1,DnsList_2,[]),
    Added=diff_dns(DnsList_2,DnsList_1,[]),
    {Added,Removed}.

diff_dns([],_,Diff)->    
    Diff;
diff_dns([DnsInfo|T],DnsList_2,Acc) -> 
    R=[X||X<-DnsList_2,
	  {?DNS_INFO_ZONE(DnsInfo),?DNS_INFO_SERVICE(DnsInfo)}=:={?DNS_INFO_ZONE(X),?DNS_INFO_SERVICE(X)}],
    case R of
	[]->
	    NewAcc=[DnsInfo|Acc];
	_ ->
	    NewAcc=Acc
    end,
    diff_dns(T,DnsList_2,NewAcc).
