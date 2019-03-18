%%% -------------------------------------------------------------------
%%% Author  : uabjle
%%% Description : dbase using dets 
%%%
%%% Created : 10 dec 2012
%%% -------------------------------------------------------------------
-module(l).

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
    A1=?DNS_INFO("zone_a","service_1","80.216.3.159",60001,120,na),   
    A2=?DNS_INFO("zone_b","service_1","80.216.3.159",60002,120,na), 
    A3=?DNS_INFO("zone_c","service_1","80.216.3.159",60003,130,na), 
    B1=?DNS_INFO("zone_a","service_2","70.216.3.159",60001,120,na), 
    B2=?DNS_INFO("zone_z","service_2","70.216.3.159",60002,200,na), 
    DnsList_1=[A1,A2,A3,B1,B2],

    A30=?DNS_INFO("zone_c","service_15","80.216.3.159",60003,130,na), 
    B10=?DNS_INFO("zone_a","service_25","70.216.3.159",60010,120,na),

    DnsList_2=[A1,A30,B10,B1,B2],

    %Equal= A1,B1,B2
    % Added=A30,B10,
    % Removed=A2,A3

    Equal=equal_dns(DnsList_1,DnsList_2),

    Equal=equal_dns(DnsList_1,DnsList_2),
    Equal=[{"zone_a","service_1","80.216.3.159",60001,120,na},
	   {"zone_a","service_2","70.216.3.159",60001,120,na},
	   {"zone_z","service_2","70.216.3.159",60002,200,na}],
    
    io:format("Equal ~p~n",[Equal]),
    {Added,Removed}= diff_dns(DnsList_1,DnsList_2),
    Added=[{"zone_a","service_25","70.216.3.159",60010,120,na},
	   {"zone_c","service_15","80.216.3.159",60003,130,na}],

    Removed=[{"zone_c","service_1","80.216.3.159",60003,130,na},
	     {"zone_b","service_1","80.216.3.159",60002,120,na}],

    io:format("Added ~p~n",[Added]),
    io:format("Removed ~p~n",[Removed]),

    % 
    ok.

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
    % Equal 
    % Added 
    % Rwmoved
    ok.
    
