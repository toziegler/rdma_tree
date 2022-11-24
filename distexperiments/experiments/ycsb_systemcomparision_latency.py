import config
from distexprunner import *

# NUMBER_NODES = 4
NUMBER_NODES = 5

parameter_grid = ParameterGrid(
    dramGB=[15],
    numberNodes= [NUMBER_NODES],
    initialFillDegree = [180], # 90 percentage on 2 nodes and the we scale compute and bw keep data fixed 
    pp=[4],
    fp=[1],
    partitioned= ["noYCSB_partitioned"],
    RUNS=[1,2,3],
)

@reg_exp(servers=config.server_list[:NUMBER_NODES])
def compile(servers):
    servers.cd("/home/tziegler/farm-like/build")
    cmake_cmd = f'cmake -DSANI=OFF  -DCMAKE_BUILD_TYPE=Release ..'
    procs = [s.run_cmd(cmake_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    make_cmd = f'make -j'
    procs = [s.run_cmd(make_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    

PAGE_SIZE = 8192
YCSB_TUPLE_SIZE = 128 + 8
@reg_exp(servers=config.server_list[:NUMBER_NODES], params=parameter_grid, raise_on_rc=False)
def ycsbBenchmark(servers, dramGB, numberNodes, initialFillDegree ,pp,fp, partitioned,RUNS):
    servers.cd("/home/tziegler/farm-like/build/frontend")
    
    sizeBytes = dramGB * 1024 * 1024 * 1024
    numTuples = int(((int(sizeBytes * (initialFillDegree / 100)  / YCSB_TUPLE_SIZE)) / 2))

    cmds = []

    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb_latency -worker=20 -dramGB={dramGB} -nodes={numberNodes} -ownIp={servers[i].ibIp} -csvFile=ycsb_latency_syscompare_farm_run_{RUNS}.csv -YCSB_run_for_seconds=30 -YCSB_tuple_count={numTuples} -{partitioned} -tag={partitioned} -YCSB_all_workloads -YCSB_all_zipf -YCSB_record_latency -YCSB_warm_up'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
