-module(stress_test).
-compile(export_all).

test() ->
	{ok, ConfigFile} = file:open("/tmp/uid.conf.test", [write]),                   % Config file
	io:format(ConfigFile, "[ {nodes_config_file, \"/tmp/nodes.conf.test\"} ].", []),
	file:close(ConfigFile), 
	{ok, NodesFile} = file:open("/tmp/nodes.conf.test", [write]),                  % Nodes file
	{ok, HostName} = inet:gethostname(),
	io:format(NodesFile, "[ {\""++HostName++"\",12,1} ].", []),
	file:close(NodesFile), 
	uid_server:start("/tmp/uid.conf.test"),
	repeat_id_generation(0,now(),0).

repeat_id_generation(Count, Start, LastId) ->
	UniqueId = uid_server:get(),
	Diff = timer:now_diff(now(), Start),
	Secs = trunc(Diff/1000000),
	if
		Count rem 1000000 == 0, Secs /= 0 ->
			io:format(user, "~p ids generated in ~p seconds, ~p/sec ~n", [Count, Secs, trunc(Count/Secs)]),
			repeat_id_generation(Count+1, Start, UniqueId);
		LastId >= UniqueId -> error;
		true ->
			repeat_id_generation(Count+1, Start, UniqueId)
	end.
