import argparse
import datetime
import zmq
import json

import boto3
import numpy as np
import os

client = boto3.client('lambda')

def main():
    parser = argparse.ArgumentParser(description='Makes a call to the TASC benchmark server.')
    parser.add_argument('-c', '--clients', nargs=1, type=int, metavar='Y',
                        help='The number of clients to invoke.',
                        dest='clients', required=True)
    parser.add_argument('-l', '--lambda', nargs=1, type=str, metavar='Y',
                        help='The name of AWS Lambda Function to be run.', 
                        dest='awslambda', required=True)
    parser.add_argument('-a', '--address', nargs=1, type=str, metavar='A',
                        help='ELB Address for the Load Balancer Values.', 
                        dest='address', required=True)
    parser.add_argument('-t', '--txn', nargs=1, type=int, metavar='Y',
                        help='The number of txns to be done.',
                        dest='txn', required=True)
    parser.add_argument('-r', '--reads', nargs=1, type=int, metavar='Y',
                        help='The number of reads to be done.',
                        dest='reads', required=True)
    parser.add_argument('-w', '--writes', nargs=1, type=int, metavar='Y',
                        help='The number of writes to be done.',
                        dest='writes', required=True)
    parser.add_argument('-rl', '--lookups', nargs=1, type=int, metavar='Y',
                        help='The number of routing lookups to be done.',
                        dest='lookups', required=True)
    parser.add_argument('-z', '--zipf', nargs='?', type=float, metavar='Y',
                        help='Zipfian coefficient',
                        dest='zipf', required=False, default=1.0)
    parser.add_argument('-p', '--pre', nargs='?', type=str, metavar='Y',
                        help='Prefix key',
                        dest='prefix', required=False, default='tasc')
    parser.add_argument('-n', '--numkeys', nargs='?', type=int, metavar='Y',
                        help='Keyspace to choose from',
                        dest='knum', required=False, default=1000)
    parser.add_argument('-ip', '--myip', nargs=1, type=str, metavar='A',
                        help='This servers public IP', 
                        dest='ip', required=True)
    args = parser.parse_args()

    function = args.awslambda[0]
    num_clients = args.clients[0]

    payload = {
    	'num_txns': args.txn[0],
    	'num_reads': args.reads[0],
    	'num_writes': args.writes[0],
    	'num_lookups': args.lookups[0],
    	'benchmark_ip': args.ip[0],
    	'elb': args.address[0],
    	'zipf': args.zipf,
    	'prefix': args.prefix,
    	'N': args.knum
    }

    message = json.dumps(payload)
    context = zmq.Context()
    lambda_socket = context.socket(zmq.PULL)
    lambda_socket.bind('tcp://*:6600')

    for _ in range(num_clients):
    	print('Invoked...')
    	response = client.invoke(
                FunctionName=function,
                InvocationType='Event',
                Payload=message
                )
    	if response["StatusCode"] > 299:
    		print('Error')

    throughputs = []
    lookups = []
    reads = []
    writes = []

    for _ in range(num_clients):
    	benchmark_data = lambda_socket.recv_string()
    	benchmark_data = benchmark_data.split(";")

    	throughput = float(benchmark_data[0])

    	# Check Read, Write, Lookup

    	if benchmark_data[1].split(",")[0] != "":
    		rt = [float(x) for x in benchmark_data[1].split(",")]
    		reads.extend(rt)

    	if benchmark_data[2].split(",")[0] != "":
    		wt = [float(x) for x in benchmark_data[2].split(",")]
    		writes.extend(wt)

    	if benchmark_data[3].split(",")[0] != "":
    		lt = [float(x) for x in benchmark_data[3].split(",")]
    		lookups.extend(lt)

    throughput = sum(throughputs)

    lookups = np.array(lookups)
    l_med = np.percentile(lookups, 50)
    l_99 = np.percentile(lookups, 99)

    reads = np.array(reads)
    r_med = np.percentile(reads, 50)
    r_99 = np.percentile(reads, 99)

    writes = np.array(writes)
    w_med = np.percentile(writes, 50)
    w_99 = np.percentile(writes, 99)

    print('Throughput: {} ops/sec\n'.format(throughput))

    print('Routing Lookups')
    print('Median/99th: {}, {}\n'.format(l_med, l_99))

    print('Reads')
    print('Median/99th: {}, {}\n'.format(r_med, r_99))

    print('Writes')
    print('Median/99th: {}, {}\n'.format(w_med, w_99))


if __name__ == '__main__':
	main()



