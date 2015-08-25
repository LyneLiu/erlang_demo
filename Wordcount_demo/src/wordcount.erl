%% 一个Wordcount的示例：给定一个文本文件，统计这个文本文件中的单词以及该单词出现的次数
-module(wordcount).

-include_lib("stdlib/include/ms_transform.hrl").

-export([start/2]).
-export([worker_execute/3]).
-export([insert_main_ets_table/2]).

-define(BUFSIZE,1048675).
%% 开启文件缓存预读
-define(FILEMODE,[binary,read,{read_ahead,?BUFSIZE}]).
%% 常用分隔符列表
-define(SPLIT_CHAR_LIST,[<<"\n">>,<<" ">>,<<"\"">>,
						<<"!">>,<<"&">>,<<".">>,
						<<",">>,<<":">>,<<"--">>]).

start(_,[])	->
	"NodeList can't is empty!";
start(FileName,NodeList)	->
	%% open the file
	{ok,FD} = file:open(FileName,?FILEMODE),
	%% 创建一个共享的可并发写的mainets表，默认为set（异键类型）
	MainEts = ets:new(mainets,[public,{write_concurrency,true}]),
	TasOwnerNode = erlang:node(),
	TaskResList = spawn_worker_process(file:read_line(FD),FD,NodeList,
										TasOwnerNode,MainEts,[]), 	
	[begin
		{WorkerNode,WorkPid} = task:await(TaskRef),
		io:format("** One Task return it's result from Node:~p,worker pid:~p",
			[WorkerNode,WorkPid]),
		ok
	end	|| TaskRef <- TaskResList],
	Ms = ets:fun2ms(fun({_,Times} = WordCount) when Times > 10 -> WordCount end),
	FinalResult = ets:select(MainEts,Ms),
	true = ets:delete(MainEts),
	FinalResult.

%% 随机获取一个节点
get_random_node(NodeList) ->
    NodeListLen = erlang:length(NodeList),
    lists:nth(get_random(NodeListLen), NodeList).

get_random(Num) ->
    {Res, _} = random:uniform_s(Num, erlang:now()),
    Res.

%% 使用task模块创建异步进程处理文本行
spawn_worker_process(eof,_FD,_NodeList,_TaskOwnerNode,_MainEts,Res)	->
	Res;
spawn_worker_process({ok,Data},FD,NodeList,TasOwnerNode,MainEts,Res)	->
	Node = get_random_node(NodeList),
	TaskRef = task:async(Node,?MODULE,worker_execute,[Data,TasOwnerNode,MainEts]),
	spawn_worker_process(file:read_line(FD),FD,NodeList,TasOwnerNode,MainEts,[TaskRef|Res]).

%% 逻辑进程处理文本行数据并将其处理结果返回给主进程 
worker_execute(Data,TasOwnerNode,MainEts)	->
	%% <<"This is an example\n">>
	%% bianry split
	[_|WordList] = lists:reverse(binary:split(Data,?SPLIT_CHAR_LIST,[global])),
	TempEts = ets:new(tempets,[set]),
	ok = lists:foreach(fun(UK) ->
						case K = string:to_lower(erlang:binary_to_list(UK)) of
							[] ->
								ignored;
							_ 	->
								true = etsCountAdd(TempEts,K,{2,1},{K,1})
						end
						end,WordList),
	rpc:call(TasOwnerNode,?MODULE,insert_main_ets_table,[MainEts,ets:tab2list(TempEts)]),
	ets:delete(TempEts),
	{erlang:node(),erlang:self()}.

insert_main_ets_table(MainEts,WorkerResult)	->
	[etsCountAdd(MainEts,K,{2,V},{K,V}) || {K,V} <- WorkerResult],
	ok.

% update_counter(Tab, Key, UpdateOp) -> Result
% UpdateOp = {Pos, Incr} | {Pos, Incr, Threshold, SetValue}

etsCountAdd(EtsTab,Key,UpdateValue,InsertValue)	->
	%% 插入新的数据元组，异键表中存在相同键的数据是发生错误；通过update_counter
	try ets:insert_new(EtsTab,InsertValue) of
		false ->
		    ets:update_counter(EtsTab,Key,UpdateValue),
		    true;
		_ ->
		    true
	catch _:_ 	->
		false
	end.