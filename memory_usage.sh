python create_memory_usage.py  -n 10 --samples 1000 --gurobi_method -1;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_method 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_method 1;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_method 2;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_presolve 0 --gurobi_pre_sparsify 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_presolve 0 --gurobi_pre_sparsify 2;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_presolve 1 --gurobi_pre_sparsify 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_presolve 1 --gurobi_pre_sparsify 2;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_presolve 2 --gurobi_pre_sparsify 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_presolve 2 --gurobi_pre_sparsify 2;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_calc 0 --gurobi_bound 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_calc 0 --gurobi_bound 1;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_calc 1 --gurobi_bound 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_calc 1 --gurobi_bound 1;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_calc 2 --gurobi_bound 0;
pkill -f python*;
python create_memory_usage.py  -n 10 --samples 1000 --gurobi_calc 2 --gurobi_bound 1;
pkill -f python*;