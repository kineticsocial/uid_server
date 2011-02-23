-module(uid_server).
-export([get/0, decode/1, status/0]).
-export([start/0, stop/0, start/1, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-behaviour(gen_server).
-record(uid, {sec, milli_sec, group_id, node_id, seq}).
-ifdef(TEST).
-compile(export_all).
-endif.

-define(DEFAULT_CONFIG, [
		{requires_ntpd, true},                          % true or false, requires ntpd up and running for time sync.
	    {nodes_config_file, "/etc/uid_nodes.conf"},     % server num configuration file
		{num_max_trial_new_unix_time, 128},             % numer of retry times when a clock goes backwards to re-generate an id before returns an error
		{timestamp_file, "/tmp/uid_timestamp.txt"}]).   % File path: the latest time in seconds. It will be updated in every 10 seconds, and used to check if system clock is forwarding

get() ->
	gen_server:call(?MODULE, get).

decode(UniqueId) ->
	<<Sec:32, MilliSec:10, GroupId:4, NodeId:6, Seq:12>> = <<UniqueId:64>>,
	{Sec, MilliSec, GroupId, NodeId, Seq}.

status() ->
	gen_server:call(?MODULE, get_state).

stop() ->
	gen_server:cast(?MODULE, stop).

start() ->
	start(?DEFAULT_CONFIG).
start(Config) ->
	?MODULE:start_link(Config).

start_link(Config) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).


init([ConfigFile]) -> % check everything here, and die if necessary
	Config = get_updated_config(?DEFAULT_CONFIG, ConfigFile), 
	true = is_ntpd_safe(Config), 
	{true, {_HostName, GroupId, NodeId}} = is_nodes_config_safe(Config),
	true = is_clock_safe(Config),
	write_timestamp(Config), % first timestamp writing
	{ok, _TimestampTimer} = timer:apply_interval(10*1000, gen_server, cast, [?MODULE, write_timestamp]), % every 10 seconds timestamp writing
	{Secs, MilliSecs} = unix_time(),
	State = Config ++ [ {last_id, #uid{sec=Secs, milli_sec=MilliSecs, group_id=GroupId, node_id=NodeId, seq=0}} ],
	{ok, State}.

handle_call(get, _From, State) ->  % don't die here, return an error instead.
	{UniqueId, NewState} = try
		{last_id, LastId={uid, LastSec, LastMilliSec, GroupId, NodeId, LastSeq}} = lists:keyfind(last_id, 1, State),
		MaxTrial = lists:keyfind(num_max_trial_new_unix_time, 1, State),
		{Sec, MilliSec, SeqNo} = get_unique_id(LastSec, LastMilliSec, LastSeq, MaxTrial),
		<<LongInt:64>> = <<Sec:32, MilliSec:10, GroupId:4, NodeId:6, SeqNo:12>>,
		{LongInt, lists:keyreplace(last_id, 1, State, {last_id, LastId#uid{sec=Sec, milli_sec=MilliSec, seq=SeqNo}})} 
	catch
		none:_ -> {error, State}
	end,
	{reply,UniqueId,NewState};



handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call(OtherRequest, _From, State) ->
    {stop, {unknown_call, OtherRequest}, State}.

handle_cast(write_timestamp, State) ->
	write_timestamp(State),
	{noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};	
handle_cast(_OtherMsg, State) ->
    {noreply, State}.	

handle_info(_OtherInfo, State) ->
    {noreply, State}.	
terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%
% private functions
%

% returns default config + user defined config.
get_updated_config(DefaultConfig, NewConfigFile) ->
	% if the config is file, read/parse it. If not, use as it is
	Config = case filelib:is_regular(NewConfigFile) of 
		false ->
			erlang:error("Config file, " ++ NewConfigFile ++ ", does not exist");
		true ->
			{ok, [ConfigList]} = file:consult(NewConfigFile),
			ConfigList 
	end,
	% returns updated config from default config by going through the user given config.
	UpdatedConfig = lists:map(fun({DfltKey, DfltValue}) -> 
		case lists:keyfind(DfltKey, 1, Config) of
			false -> {DfltKey, DfltValue}; % if not defined, use default
			Tuple -> Tuple                 % if defined, override it
		end
	end, DefaultConfig),
	error_logger:info_msg("uid_server starting with config: ~p", [UpdatedConfig]),
	UpdatedConfig.


% returns unique time + seq, which is used to generate a unique id(time+node+seq)
get_unique_id(LastSec, LastMilliSec, LastSeq, MaxTrial) ->
	{Sec, MilliSec} = unix_time(),
	{NowSec, NowMilliSec} = if  % if current time is less than previous time(in case clock drift happens)
		Sec*1000+MilliSec < LastSec*1000+LastMilliSec ->
			get_new_unix_time(LastSec, LastMilliSec, _Trial=0, MaxTrial);
		true ->
			{Sec, MilliSec}
	end,
	SeqNumInMilliSec = if
		NowSec > LastSec            -> 0;
		NowMilliSec > LastMilliSec  -> 0;
		LastSeq  >= 4095            -> error;
		true -> LastSeq + 1
	end,
	{NowSec, NowMilliSec, SeqNumInMilliSec}.

% returns unix UTC in seconds, and milliseconds
unix_time() ->
	Now = {_MegaSecs, _Secs, MicroSecs} = now(), 
	UnixTime = calendar:datetime_to_gregorian_seconds(calendar:now_to_universal_time(Now)) - 62167219200,
	MilliSecs=trunc(MicroSecs/1000),
	{UnixTime, MilliSecs}.

% compare current time to the previos time and if current time is not right, retry until get a good one
% this should never called when ntpd is in use, but who knows if ntpd allows time backwarding.
get_new_unix_time(TargetSec, TargetMs, Trial, MaxTrial) ->
	error_logger:warning_msg("uid_server:get_new_unix_time is called, which means time backwarding happend."),
	{CurSec, CurMs} = unix_time(),
	if 
		Trial >= MaxTrial -> 
			{error, clock_drift_adjustment_failed_after_max_trial};
		CurSec*1000+CurMs >= TargetSec*1000+TargetMs -> 
			{CurSec, CurMs};
		true ->
			timer:sleep(1),
			get_new_unix_time(TargetSec, TargetMs, Trial+1, MaxTrial)
	end.

% check if ntpd is up and running, and this is good for test, so that we can stub it.
is_ntpd_up_and_running() ->  
	os:cmd("ps ax | grep bin/ntpd | grep -v grep | awk {'print $1'}") /= [].

% check if ntpd safe by configuration
is_ntpd_safe(Config) ->
	{_,RequiresNtpd} = lists:keyfind(requires_ntpd,1,Config),
	NtpdUpAndRunning = is_ntpd_up_and_running(),
	error_logger:info_msg("ntpd_required is ~p, and ntpd up and running is ~p",[RequiresNtpd, NtpdUpAndRunning]),
	if
		RequiresNtpd == true, NtpdUpAndRunning == false ->
			erlang:error("Uid server requires ntpd up and running");
		true ->
			true
	end.

% write timestamp to a file, so that it can be used when next restart.
% this should happen every given seconds(i.e., 10 seconds)
write_timestamp(Config) ->
	{_, TimestampFile} = lists:keyfind(timestamp_file, 1, Config),
	{ok, FileDescriptor} = file:open(TimestampFile, [write]),
	{Secs, _MilliSecs} = unix_time(),
	io:format(FileDescriptor, "~p.", [Secs]),
	file:close(FileDescriptor). 

% check if time is forwarding by comparing to saved file
% This is called when it is started.
is_clock_safe(Config) ->
	{_, TimestampFile} = lists:keyfind(timestamp_file, 1, Config),
	error_logger:info_msg("Checking timestamp file ~p",[TimestampFile]),
	case filelib:is_regular(TimestampFile) of
		true ->
			{ok, [PrevUnixTime]} = file:consult(TimestampFile),
			error_logger:info_msg("Previously recorded timestamp is ~p",[PrevUnixTime]),
			{NewUnixTime,_} = unix_time(),
			error_logger:info_msg("Current timestamp is ~p",[NewUnixTime]),
			NewUnixTime >= PrevUnixTime;
		false -> 
			true
	end.

% checking uniqueness of host name, group id, and node id
is_nodes_config_safe(Config) ->
	{_, NodesConfigFile} = lists:keyfind(nodes_config_file, 1, Config),
	error_logger:info_msg("Checing nodes config file, ~p",[NodesConfigFile]),
	case filelib:is_regular(NodesConfigFile) of 
		false ->
			erlang:error("Invalid nodes config file, "++ NodesConfigFile ++". File does not exist");
		true ->
			{ok, [NodesConfig]} = file:consult(NodesConfigFile),
			HasUniqueKeys   = length(NodesConfig) == length(lists:usort(proplists:get_keys(NodesConfig))), % check if hostnames(keys) are unique
			HasUniqueValues = length(NodesConfig) == length(lists:usort(lists:map(fun({_,G,N}) -> {G,N} end, NodesConfig))),  % check if values(group_id, node_ids) are unique
			{ok, HostName} = inet:gethostname(),
			NodeConfig = lists:keyfind(HostName, 1, NodesConfig),
			if 
				HasUniqueKeys==false ->
					erlang:error("Invalid nodes config file. Hostnames are not unique");
				HasUniqueValues==false ->
					erlang:error("Invalid nodes config file. Group id and node id are not unique");
				NodeConfig == false ->
					erlang:error("Invalid nodes config file. Hostname, " ++ HostName ++ ", not found in it");
				true -> 
					{HostName, GroupId, NodeId} = NodeConfig,
					if 
						GroupId < 0; GroupId > 15; NodeId < 0; NodeId > 63 ->
							erlang:error("Invalid nodes config file. Group id must be between 0 to 15  and node id must between 0 to 63");
						true ->
							error_logger:info_msg("HostName is ~p, Group Id is ~p, Node id is ~p",[HostName, GroupId, NodeId]),
							{true, {HostName, GroupId, NodeId}}
					end
			end
	end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
setup_test() ->
	error_logger:tty(false),
	{ok, ConfigFile} = file:open("/tmp/uid.conf.test", [write]),                   % Config file
	io:format(ConfigFile, "[ {nodes_config_file, \"/tmp/nodes.conf.test\"} ].", []),
	file:close(ConfigFile), 
	{ok, NodesFile} = file:open("/tmp/nodes.conf.test", [write]),                  % Nodes file
	{ok, HostName} = inet:gethostname(),
	io:format(NodesFile, "[ {\""++HostName++"\",12,1} ].", []),
	file:close(NodesFile). 

%init_ntpd_required_but_not_running_test() ->  TODO: stub is not work properly, need to make it work
    %stub:stop(?MODULE, is_ntpd_up_and_running).
	%stub:start(?MODULE, is_ntpd_up_and_running, fun() -> false end ),
	%?assertException(error, function_clause, init(["/tmp/uid.conf.test"])),
    %stub:stop(?MODULE, is_ntpd_up_and_running).

init_config_file_does_not_exist_test() ->
	?assertException(error, _, init(["this file does not exist"])).

init_duplicate_hostname_in_nodes_config_test() ->
	{ok, NodesFile} = file:open("/tmp/nodes.conf.test", [write]),
	{ok, HostName} = inet:gethostname(),
	io:format(NodesFile, "[ {\""++HostName++"\",12,1}, {\""++HostName++"\",12,1}  ].", []),
	file:close(NodesFile), 
	?assertException(error, _, init(["/tmp/uid.conf.test"])).

init_duplicate_group_id_and_node_id_test() ->
	{ok, NodesFile} = file:open("/tmp/nodes.conf.test", [write]),
	{ok, HostName} = inet:gethostname(),
	io:format(NodesFile, "[ {\""++HostName++"\",12,1}, {\"node2.com\",12,1}  ].", []),
	file:close(NodesFile), 
	?assertException(error, _, init(["/tmp/uid.conf.test"])).

init_timestamp_initialization_test() ->
	{ok, NodesFile} = file:open("/tmp/nodes.conf.test", [write]),                  % Nodes file is good now
	{ok, HostName} = inet:gethostname(),
	io:format(NodesFile, "[ {\""++HostName++"\",12,1} ].", []),
	file:close(NodesFile), 
	file:delete("/tmp/uid_timestamp.txt"),
	init(["/tmp/uid.conf.test"]),
	?assert( filelib:is_regular("/tmp/uid_timestamp.txt")).

init_invalid_timestamp_test() ->
	{ok, FileDescriptor} = file:open("/tmp/uid_timestamp.txt", [write]),
	{Secs, _MilliSecs} = unix_time(),
	io:format(FileDescriptor, "~p.", [Secs+3600]), % future hour
	file:close(FileDescriptor), 
	?assertException(error, _, init(["/tmp/uid.conf.test"])),
	file:delete("/tmp/uid_timestamp.txt").
	
init_valid_timestamp_test() ->
	{ok, FileDescriptor} = file:open("/tmp/uid_timestamp.txt", [write]),
	{Secs, _MilliSecs} = unix_time(),
	io:format(FileDescriptor, "~p.", [Secs]),
	file:close(FileDescriptor), 
	?assertMatch({ok, _}, init(["/tmp/uid.conf.test"])),
	file:delete("/tmp/uid_timestamp.txt").

get_unique_id_with_future_clock_100ms_test() -> 
	{Sec,MS} = unix_time(),
	PastSec=trunc((Sec*1000+MS+100)/1000),
	PastMS =(Sec*1000+MS-100) rem 1000,
	{R1,R2,R3} = get_unique_id(PastSec, PastMS, 0, 128),
	?assert( is_integer(R1) and is_integer(R2) and is_integer(R3) ).

get_unique_id_with_future_clock_an_hour_test() -> 
	{Sec,MS} = unix_time(),
	?assertMatch( {error, _, 0}, get_unique_id(Sec+3600, MS, 0, 128) ).

valid_id_generation_test_with_server_up_and_running_test() ->
	?MODULE:start("/tmp/uid.conf.test"),
	UniqueId = ?MODULE:get(),
	?assert(is_integer(UniqueId)),
	{Sec,MS,GroupId,NodeId,Seq} = ?MODULE:decode(UniqueId),
	?assert(is_integer(Sec) and (MS >= 0) and (MS < 1000)),
	?assert( (GroupId >= 0) and (GroupId < 16)),
	?assert( (NodeId  >= 0) and (NodeId  < 63)),
	?assert( (Seq     >= 0) and (Seq     < 1024)),
	?MODULE:stop().
	
forwarding_id_generation_test_with_server_up_and_running_test() ->
	timer:sleep(200),
	?MODULE:start("/tmp/uid.conf.test"),
	Start = now(),
	Max = 200000,
	Result = repeat_id_generation(0,Max,0),
	NowDiff = timer:now_diff(now(), Start),
	io:format(user, "    ~p ids in ~p seconds, ~p/sec~n", [Max, trunc(NowDiff/1000000), trunc(Max/(NowDiff/1000000))]),
	?assertEqual( ok, Result),
	?MODULE:stop().

repeat_id_generation(Count,Max, LastId) ->
	UniqueId = ?MODULE:get(),
	if
		Count  >  Max -> ok;
		LastId >= UniqueId -> error;
		true ->
			repeat_id_generation(Count+1, Max, UniqueId)
	end.
	
teardown_test() ->
	file:delete("/tmp/uid.conf.test"),                   % Config file
	file:delete("/tmp/nodes.conf.test"),                 % Nodes file
	ok.

-endif. 
