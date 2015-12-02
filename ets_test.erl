
-module(ets_test).

-compile(export_all).

-record(s, {n=0, ts, rate=0, stats}).
-record(stats, {n=0, rej=0, ts}).


start() ->
    ets_new(sip_ack),
    ets_new(stats),
    ets_new(plc_fg),
    start_plc(),
    spawn_link(fun() -> init() end).

stop() ->
    ?MODULE ! stop,
    receive after 1000 -> ok end,
    stop_plc().

add_rate(R) ->
    ?MODULE ! {add_rate, R}.


init() ->
    io:format("starting~n", []),
    register(?MODULE, self()),
    loop(tick(#s{stats=new_stats()})).

loop(S) ->
    receive    
	stop ->
	    io:format("stopped~n", []);
	tick ->
	    loop(tick(S));
	{add_rate, AddRate} ->
	    loop(do_add_rate(S, AddRate))
    end.

tick(S) ->
    TS = ts(),
    erlang:send_after(100, self(), tick),
    N = kick_off_workers(S, TS),
    NewS = update_ts(TS, add_n(N, S)),
    update_stats(NewS).


do_add_rate(S=#s{rate=OldRate}, AddRate) ->
    S#s{rate=OldRate+AddRate}.

update_stats(S=#s{ts=TS, stats=Stats}) ->
    case is_small_tick(TS, Stats) of
	true -> S;
	_ -> S#s{stats=do_update_stats(TS, Stats)}
    end.

do_update_stats(TS, St=#stats{n=N2, rej=R2, ts=TS2}) ->
    Diff = t_diff_as_tenths_of_second(TS, TS2),
    ThisPeriodN = read(w_completed)-N2,
    ThisPeriodR = read(plc_reject)-R2,
    NumberThisPeriod = N-N2,
    io:format("tick (~4.1fs), tab sz: ~p, plc sz: ~p, ok: ~p, rej: ~p~n", 
              [Diff, tab_sz(sip_ack), tab_sz(plc_fg), 
               ThisPeriodN, ThisPeriodR]),
    St#stats{n=N, ts=TS}.
    
is_small_tick(TS, #stats{ts=TS2}) ->
    Diff = t_diff_as_tenths_of_second(TS, TS2),
    if Diff < 1.0 -> true;
       true -> false
    end.

new_stats() -> #stats{ts=ts()}.

update_ts(TS, S=#s{}) -> S#s{ts=TS}.

add_n(N, S=#s{n=OldN}) -> S#s{n=OldN+N}.

ts() -> now().

t_diff_as_tenths_of_second(TS1, TS2) ->
    round(t_diff(TS1, TS2) / 100000) / 10.

t_diff(Now, Then) -> %%when size(Then) == 3 ->    
    timer:now_diff(Now, Then).
%% t_diff(_, _) -> 
%%     0.

tab_sz(T) ->
    ets:info(T, size).

kick_off_workers(S, TS) ->
    Num = calc_number_of_workers_to_kick_off(S, TS),
    do_kick_off_workers(S, Num),
    Num.

do_kick_off_workers(#s{n=N}, Num) ->
    [start_worker(X) || X <- lists:seq(N, N+Num-1)].

calc_number_of_workers_to_kick_off(#s{rate=Rate}, _TS) ->
    round(Rate / 10).
%%     round(PercentOfOneSecond(S, TS)),
%%     SoFar = sofar_this_second(S, TS)
%%     1.


%%---------------
%% plc primitives
start_plc() ->
    spawn_link(fun() -> plc_init() end).

stop_plc() ->
    plc ! stop.

plc_fg() ->
    PlcPid = whereis(plc),
    Ref = erlang:monitor(process, PlcPid),
    PlcPid ! {fg, self()},
    receive
        ok -> erlang:demonitor(Ref), ok;
        no -> erlang:demonitor(Ref),
              {error, plc_reject};
        {'DOWN', _, process, PlcPid, _} ->
            inc(w_plc_killed),
            {error, plc_reject}
    end.

plc_init() ->
    register(plc, self()),
    plc_loop(1000).

plc_loop(T) ->
    receive
        stop ->
	    io:format("PLC stopped~n", []),
            send_no_to_all();
        {fg, Pid} ->
            Key = now(),
            enqueue(Key, Pid),
            plc_loop(0);
        {timeout, Key} ->
            dequeue(Key, no),
            plc_loop(T)
    after T ->
            case first(plc_fg) of
                undefined ->
                    plc_loop(1000);
                Key ->
                    check_load_and_maybe_dequeue(Key),
                    plc_loop(T)
            end
    end.

check_load_and_maybe_dequeue(Key) ->
    case erlang:statistics(run_queue) of
        N when N < 16 -> dequeue(Key, ok);
        _ -> ok
    end.


send_no_to_all() ->
    case first(plc_fg) of
        undefined -> ok;
        Key -> dequeue(Key, no),
               send_no_to_all()
    end.

enqueue(Key, Pid) ->
    Tref = erlang:send_after(440, self(), {timeout, Key}),
    insert(plc_fg, {Key, Pid, Tref}).

dequeue(Key, Msg) ->
    {_, Pid, Tref} = lookup(plc_fg, Key),
    delete(plc_fg, Key),
    erlang:cancel_timer(Tref),
    Pid ! Msg.
    
    
    

%%---------------
%% worker primitives

start_worker(N) ->
    spawn_link(fun() -> w_init(N) end).

w_init(N) ->
    %% io...
    inc(w_started),
    insert(sip_ack, {{N, hej}, undefined}),
    w_do_stuff(),
    receive after 250 -> ok end,
    match(N),
    inc(w_completed),
    delete(sip_ack, {N, hej}),
    ok.

w_do_stuff() ->
    case plc_fg() of
        ok ->
            L = lists:seq(1, 3500),
            L2 = [X+25 || X <- L],
            L3 = [X*X*25 || X <- L2],
            L4 = [X*25 || X <- L3],
            L4;
        _ ->
            inc(plc_reject),
            ok
    end.


ets_new(T) ->
    case ets:info(T) of
        undefined ->
            ets:new(T, [public, named_table]);
        _ ->
            ets:delete_all_objects(T)
    end.


%%---------------
%% Counter primitives


inc(K) -> inc(K, 1).
inc(K, Inc) -> inc(stats, K, Inc).
inc(T, K, Inc) -> update_counter(T, K, Inc).
read(K) -> 
    read(stats, K).
read(T, K) -> 
    case catch ets:update_counter(T, K, 0) of
        X when is_integer(X), X>=0 -> X;
        _		           -> 0
    end.

update_counter(T, K, Inc) ->
    case catch ets:update_counter(T, K, Inc) of
	X when is_integer(X) -> X;
	_ ->
	    case ets:insert_new(T, {K, Inc}) of
		true -> Inc;
		_ -> update_counter(T, K, Inc)
	    end
    end.


first(T) ->
    case ets:first(T) of
        '$end_of_table' -> undefined;
        Key -> Key
    end.

insert(T, X) ->
    ets:insert(T, X).
lookup(T, K) ->
    case ets:lookup(T, K) of
        [X] -> X;
        _ -> undefined
    end.
delete(T, K) ->
    ets:delete(T, K).

match(Key) ->
%%    ok.
    ets:match_object(sip_ack, {{Key, '_'}, '_'}).


%% c(ets_test).
%% ets_test:start().
%% ets_test:stop().
%% ets_test:add_rate(2000).
%% ets_test:add_rate(1000).
%% ets_test:add_rate(500).
%% ets:tab2list(stats).
