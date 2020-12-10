# YACS
# yet another centralized scheduler- Big Data Project
# We need to first start master, to do that we do as follows
# The last argument is name of the algorithm (RANDOM , RR , LL)
python3 Master.py config.json RANDOM 
# Then We need to start Workers
# python3 Worker.py port-number worker-id
python3 Worker.py 4000 1
# After we start all the workers
# We run a file to generate requests with number of requests
python3 Copy\ of\ requests.py 10
# Now we run analyze files to generate required graphs
python3 analyze_part1.py
python3 analyze_part2.py RANDOM
